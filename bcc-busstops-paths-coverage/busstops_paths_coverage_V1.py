"""
BCC Open Data → PostGIS pipeline (V1)

What this script does
---------------------
A) Download data (ArcGIS FeatureServer → GeoJSON)
   - Bus stops (points)
   - Paths/cycleways (lines)

B) Load into PostGIS (schema: bcc_open)
   - raw_busstops (EPSG:4326) + GiST index
   - raw_paths    (EPSG:4326) + GiST index
   - Optional: sanity-check API count vs DB count

C) Derive & analyze (distance-safe in EPSG:7856)
   C1) Create projected copies:
       - busstops_7856 (Point, 7856)
       - paths_7856    (MultiLineString, 7856)
   C2) Bus stop coverage area:
       - busstops_buffer_400 (400m buffer)
       - busstops_400_cov (dissolved coverage polygon)
   C3) Served paths:
       - paths_served_400m (paths inside coverage)
   C4) KPI:
       - served_km, total_km, served_percent

Notes
-----
- `fetch_count()` is optional validation and may return None if the service returns an error JSON.
- Distances (buffer/length) are done in EPSG:7856 (meters).
"""

import geopandas as gpd
import requests
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from db_config_local import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT


# ----------------------------
# Config
# ----------------------------
SCHEMA = "bcc_open"
PAGE_SIZE = 1000
WHERE_ALL = "1=1"

BUSSTOPS_LAYER = (
    "https://portal.data.nsw.gov.au/arcgis/rest/services/Hosted/"
    "Blacktown_Council_Data_Public/FeatureServer/0"
)
PATHS_LAYER = (
    "https://portal.data.nsw.gov.au/arcgis/rest/services/Hosted/"
    "Blacktown_Council_Data_Public/FeatureServer/1"
)


# ----------------------------
# ArcGIS REST fetching helpers
# ----------------------------

def fetch_page(layer_url: str, offset: int, page_size: int, where: str = WHERE_ALL):
    """Fetch one page of features from an ArcGIS FeatureServer layer as GeoJSON."""
    url = f"{layer_url}/query"
    params = {
        "where": where,
        "outFields": "*",
        "returnGeometry": "true",
        "resultOffset": offset,
        "resultRecordCount": page_size,
        "f": "geojson",
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    # Strict validation: must look like GeoJSON FeatureCollection
    if "features" not in data or not isinstance(data["features"], list):
        raise ValueError(f"Unexpected response structure. Keys={list(data.keys())}")

    return data["features"]


def fetch_all(layer_url: str, page_size: int = PAGE_SIZE, where: str = WHERE_ALL):
    """Fetch all features (paged) and return a list of GeoJSON Features."""
    all_features = []
    offset = 0

    while True:
        features_per_page = fetch_page(layer_url, offset=offset, page_size=page_size, where=where)
        all_features.extend(features_per_page)

        # stop conditions
        if len(features_per_page) == 0:
            break
        if len(features_per_page) < page_size:
            break

        offset += page_size

    return all_features


def fetch_count(layer_url: str, where: str = WHERE_ALL) -> int | None:
    """Return feature count from the API, or None if the endpoint returns an error JSON."""
    url = f"{layer_url}/query"
    params = {
        "where": where,
        "returnCountOnly": "true",
        "f": "json",
    }

    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()

    # ArcGIS sometimes returns HTTP 200 but with {"error": {...}}
    if "error" in data:
        print("COUNT API ERROR:", data["error"])
        return None

    return int(data["count"])


# ----------------------------
# DB helpers
# ----------------------------
def exec_sql(engine, sql: str) -> None:
    """Execute SQL (DDL/DML). No result returned."""
    with engine.begin() as conn:
        conn.execute(text(sql))


def table_count(engine, schema: str, table: str) -> int:
    """Return COUNT(*) from a table."""
    sql = text(f"SELECT COUNT(*) FROM {schema}.{table};")
    with engine.begin() as conn:
        return int(conn.execute(sql).scalar())


def report_count(name: str, api_count: int | None, db_count: int) -> None:
    """Print a quick sanity-check line comparing API count vs DB count."""
    if api_count is None:
        print(f"{name}: API count unavailable → DB count = {db_count}")
    else:
        print(f"{name}: API = {api_count} | DB = {db_count} | Match = {api_count == db_count}")


# ----------------------------
# Main pipeline
# ----------------------------
def main():
    # ============================================================
    # A) Download data (ArcGIS FeatureServer → GeoJSON)
    # ============================================================

    # A1) Bus stops
    data_bus = fetch_all(BUSSTOPS_LAYER, PAGE_SIZE, WHERE_ALL)
    print(f"{len(data_bus)} bus stop features fetched!")

    # A2) Paths
    data_paths = fetch_all(PATHS_LAYER, PAGE_SIZE, WHERE_ALL)
    print(f"{len(data_paths)} path features fetched!")


    # ============================================================
    # B) Load into PostGIS (schema: bcc_open)
    # ============================================================

    # B0) Connect
    password = quote_plus(DB_PASSWORD)
    engine = create_engine(
        f"postgresql+psycopg2://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    print("engine object created")

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("DB connection OK")
    except Exception as e:
        print("DB connection FAILED:")
        print(e)
        return

    # B1) Ensure schema exists
    exec_sql(engine, f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};")

    # B2) Load raw_busstops (EPSG:4326) + index
    gdf_bus = gpd.GeoDataFrame.from_features(data_bus, crs="EPSG:4326").rename_geometry("geom")
    gdf_bus.to_postgis("raw_busstops", engine, schema=SCHEMA, if_exists="replace", index=False)

    exec_sql(
        engine,
        f"""
        CREATE INDEX IF NOT EXISTS raw_busstops_geom_gix
            ON {SCHEMA}.raw_busstops
            USING gist(geom);
        """,
    )

    # B3) Load raw_paths (EPSG:4326) + index
    gdf_paths = gpd.GeoDataFrame.from_features(data_paths, crs="EPSG:4326").rename_geometry("geom")
    gdf_paths.to_postgis("raw_paths", engine, schema=SCHEMA, if_exists="replace", index=False)

    exec_sql(
        engine,
        f"""
        CREATE INDEX IF NOT EXISTS raw_paths_geom_gix
            ON {SCHEMA}.raw_paths
            USING gist(geom);
        """,
    )

    # B4) Optional sanity check: API count vs DB count
    count_buses_api = fetch_count(BUSSTOPS_LAYER, WHERE_ALL)
    count_paths_api = fetch_count(PATHS_LAYER, WHERE_ALL)

    count_bus_db = table_count(engine, SCHEMA, "raw_busstops")
    count_path_db = table_count(engine, SCHEMA, "raw_paths")

    report_count("busstops", count_buses_api, count_bus_db)
    report_count("paths", count_paths_api, count_path_db)


    # ============================================================
    # C) Derive & analyze (distance-safe in EPSG:7856)
    # ============================================================

    # ----------------------------
    # C1) Create projected copies
    # ----------------------------
    exec_sql(
        engine,
        f"""
        DROP TABLE IF EXISTS {SCHEMA}.busstops_7856;

        CREATE TABLE {SCHEMA}.busstops_7856 AS
        SELECT * FROM {SCHEMA}.raw_busstops;

        ALTER TABLE {SCHEMA}.busstops_7856
          ALTER COLUMN geom TYPE geometry(Point, 7856)
          USING ST_Transform(geom, 7856);

        CREATE INDEX IF NOT EXISTS busstops_7856_geom_gix
          ON {SCHEMA}.busstops_7856
          USING gist (geom);
        """,
    )

    exec_sql(
        engine,
        f"""
        DROP TABLE IF EXISTS {SCHEMA}.paths_7856;

        CREATE TABLE {SCHEMA}.paths_7856 AS
        SELECT * FROM {SCHEMA}.raw_paths;

        ALTER TABLE {SCHEMA}.paths_7856
          ALTER COLUMN geom TYPE geometry(MultiLinestring, 7856)
          USING ST_Multi(ST_Transform(geom, 7856));

        CREATE INDEX IF NOT EXISTS paths_7856_geom_gix
          ON {SCHEMA}.paths_7856
          USING gist (geom);
        """,
    )

    # ----------------------------
    # C2) Bus stop coverage area
    # ----------------------------
    exec_sql(
        engine,
        f"""
        DROP TABLE IF EXISTS {SCHEMA}.busstops_buffer_400;

        CREATE TABLE {SCHEMA}.busstops_buffer_400 AS
        SELECT
          fid,
          suburb,
          ST_Buffer(geom, 400) AS geom
        FROM {SCHEMA}.busstops_7856;

        CREATE INDEX IF NOT EXISTS busstops_buffer_400_gix
          ON {SCHEMA}.busstops_buffer_400
          USING gist (geom);
        """,
    )

    exec_sql(
        engine,
        f"""
        DROP TABLE IF EXISTS {SCHEMA}.busstops_400_cov;

        CREATE TABLE {SCHEMA}.busstops_400_cov AS
        SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM {SCHEMA}.busstops_buffer_400;

        CREATE INDEX IF NOT EXISTS busstops_400_cov_gix
          ON {SCHEMA}.busstops_400_cov
          USING gist(geom);
        """,
    )

    # ----------------------------
    # C3) Served paths (inside coverage)
    # ----------------------------
    exec_sql(
        engine,
        f"""
        DROP TABLE IF EXISTS {SCHEMA}.paths_served_400m;

        CREATE TABLE {SCHEMA}.paths_served_400m AS
        SELECT
          p.fid,
          ST_Multi(
            ST_CollectionExtract(
              ST_Intersection(p.geom, c.geom),
              2
            )
          ) AS geom
        FROM {SCHEMA}.paths_7856 AS p
        JOIN {SCHEMA}.busstops_400_cov AS c
          ON ST_Intersects(p.geom, c.geom)
        WHERE NOT ST_IsEmpty(ST_Intersection(p.geom, c.geom));

        CREATE INDEX IF NOT EXISTS paths_served_400m_gix
          ON {SCHEMA}.paths_served_400m
          USING gist (geom);
        """
    )

    # ----------------------------
    # C4) KPI (served_km / total_km / served_percent)
    # ----------------------------
    with engine.begin() as conn:
        served_km = conn.execute(
            text(
                f"""
                SELECT ROUND((SUM(ST_Length(geom))/1000.0)::numeric, 3)
                FROM {SCHEMA}.paths_served_400m;
                """
            )
        ).scalar()

        total_km = conn.execute(
            text(
                f"""
                SELECT ROUND((SUM(ST_Length(geom))/1000.0)::numeric, 3)
                FROM {SCHEMA}.paths_7856;
                """
            )
        ).scalar()

    print("served_km:", served_km)
    print("total_km:", total_km)

    if served_km is None or total_km in (None, 0):
        served_percent = None
    else:
        served_percent = round(100 * float(served_km) / float(total_km), 2)

    print("served_percent:", served_percent)


if __name__ == "__main__":
    main()
