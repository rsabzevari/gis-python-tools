import geopandas as gpd
import requests
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from db_config_local import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT


def fetch_page(layer_url : str, offset : int, page_size: int, where : str = "1=1"):

    url = f"{layer_url}/query"
    params = {
        "where": where,
        "outFields" : "*",
        "returnGeometry": "true",
        "resultOffset" : offset,
        "resultRecordCount" : page_size,
        "f": "geojson"
    }
    
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()

    data = r.json()

    # Strict validation
    if "features" not in data or not isinstance(data["features"], list):
        raise ValueError(f"Unexpected response structure. Keys={list(data.keys())}")
    
    features = data["features"]
    exceeded = bool(data.get("exceededTransferLimit", False))

    return features, exceeded

def fetch_all(layer_url : str, page_size: int = 1000, where: str = "1=1"):
    
    all_features = []
    offset = 0

    while True:
        features_per_page, exceeded = fetch_page(layer_url, offset=offset, page_size=page_size, where=where)
        all_features.extend(features_per_page)

        if len(features_per_page) == 0:
            break
        if len(features_per_page) < page_size:
            break
        offset += page_size     

    return all_features

def fetch_count(layer_url, where: str = "1=1") -> int:
    url = f"{layer_url}/query"
    params = {
        "where" : where,
        "returnCountOnly" : "true",
        "f": "json"
    }

    r= requests.get(url, params=params,timeout=60 )
    r.raise_for_status()
    data = r.json()

    if "count" not in data:
        raise ValueError(f"No 'count' in response. Keys={list(data.keys())}")
    return int(data["count"])


data = fetch_all("https://portal.data.nsw.gov.au/arcgis/rest/services/Hosted/Blacktown_Council_Data_Public/FeatureServer/0",
          1000,
          "1=1"
          )
print(f"{len(data)} features fetched!")

gdf = gpd.GeoDataFrame.from_features(data, crs = "EPSG:4326")
print(gdf["suburb"].head(3))

print(gdf.columns.tolist())

count_api = fetch_count("https://portal.data.nsw.gov.au/arcgis/rest/services/Hosted/Blacktown_Council_Data_Public/FeatureServer/0", "1=1")
print("API count:", count_api)
print("Fetched:", len(data))
print("Match:", count_api == len(data))

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

#---------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------
# A) Add layer 1 ingestion (paths/cycleways)

with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS bcc_open;"))

gdf = gpd.GeoDataFrame.from_features(data, crs="EPSG:4326")
gdf = gdf.rename_geometry("geom")
                          
gdf.to_postgis(
       "raw_busstops",
        engine,
        schema= "bcc_open",
        if_exists='replace',
        index=False
    )
    
with engine.begin() as conn:
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS raw_busstops_geom_gix
            ON bcc_open.raw_busstops
            USING gist(geom)
"""))
    
data_cycle = fetch_all("https://portal.data.nsw.gov.au/arcgis/rest/services/Hosted/Blacktown_Council_Data_Public/FeatureServer/1",
          1000,
          "1=1"
          )
gdf_1 = gpd.GeoDataFrame.from_features(data_cycle, crs = "EPSG:4326")
gdf_1 = gdf_1.rename_geometry("geom")

gdf_1.to_postgis(
    "raw_paths",
    engine,
    schema = "bcc_open",
    if_exists= "replace",
    index= False
)

with engine.begin() as conn:
    conn.execute(text("""
        CREATE INDEX IF NOT EXISTS raw_paths_geom_gix
            ON bcc_open.raw_paths
            USING gist(geom)
"""))
    
    
count_buses_api = fetch_count("https://portal.data.nsw.gov.au/arcgis/rest/services/Hosted/Blacktown_Council_Data_Public/FeatureServer/0","1=1")  
count_path_api = fetch_count("https://portal.data.nsw.gov.au/arcgis/rest/services/Hosted/Blacktown_Council_Data_Public/FeatureServer/1","1=1")

def table_count(engine, schema: str, table: str) -> int:
    sql = text(f"SELECT COUNT (*) FROM {schema}.{table};")
    with engine.begin() as conn:
        return int(conn.execute(sql).scalar())
    
count_bus_db = table_count(engine, "bcc_open", "raw_busstops")
count_path_db = table_count(engine, "bcc_open", "raw_paths")

print("DB busstops:", count_bus_db)
print("DB paths:", count_path_db)

print("API busstops:", count_buses_api, " - DB busstops:", count_bus_db, "Match:", count_buses_api == count_bus_db)
print("API paths:", count_path_api, " - DB paths:", count_path_db, "Match:", count_path_api == count_path_db)


#---------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------
# B) Create derived tables in 7856

# I tested this before running the below, to make sure Point or Line or multiline or polygon
# SELECT GeometryType(geom), ST_SRID(geom), COUNT(*)
#FROM bcc_open.raw_busstops
#GROUP BY 1,2;

sql = """
    DROP TABLE IF EXISTS bcc_open.busstops_7856;

    CREATE TABLE bcc_open.busstops_7856 AS
    SELECT * FROM bcc_open.raw_busstops;

    ALTER TABLE bcc_open.busstops_7856
    ALTER COLUMN geom TYPE geometry(Point, 7856)
    USING ST_Transform(geom, 7856);

    CREATE INDEX IF NOT EXISTS busstops_7856_geom_gix
    ON bcc_open.busstops_7856
    USING gist (geom);
"""
with engine.begin() as conn:
    conn.execute(text(sql))

sql = """
    DROP TABLE IF EXISTS bcc_open.paths_7856;

    CREATE TABLE bcc_open.paths_7856 AS
    SELECT * FROM bcc_open.raw_paths;

    ALTER TABLE bcc_open.paths_7856
    ALTER COLUMN geom TYPE geometry(MultiLinestring, 7856)
    USING ST_Multi(ST_Transform(geom, 7856));

    CREATE INDEX IF NOT EXISTS paths_7856_geom_gix
    ON bcc_open.paths_7856
    USING gist (geom);
"""
with engine.begin() as conn:
    conn.execute(text(sql))


#---------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------
#---------------------------------------------------------------------------------------------------------------
# C) Spatial analysis

# C1)400m coverage area of bus stops

sql = """
    DROP TABLE IF EXISTS bcc_open.busstops_buffer_400;

    CREATE TABLE bcc_open.busstops_buffer_400 AS
    SELECT
    fid,
    suburb,
    ST_Buffer(geom, 400) AS geom
    FROM bcc_open.busstops_7856;

    CREATE INDEX IF NOT EXISTS busstops_buffer_400_gix
    ON bcc_open.busstops_buffer_400
    USING gist (geom);
"""

with engine.begin() as conn:
    conn.execute(text(sql))

# C2) Dissolve the buffered layer

sql = """
    DROP TABLE IF EXISTS bcc_open.busstops_400_cov;
    
    CREATE TABLE bcc_open.busstops_400_cov AS
        SELECT 
        ST_UnaryUnion(ST_Collect(geom)) AS geom
        FROM bcc_open.busstops_buffer_400;
    
    CREATE INDEX IF NOT EXISTS busstops_400_cov_gix
    ON bcc_open.busstops_400_cov
    USING gist(geom);
"""

with engine.begin() as conn:
    conn.execute(text(sql))

# C3) Paths within the coverage. compute the coverage of the path inside the coeverage

sql = """
    DROP TABLE IF EXISTS bcc_open.paths_served_400m;

    CREATE TABLE bcc_open.paths_served_400m AS
    SELECT
    p.fid,
    ST_Multi(
        ST_CollectionExtract(
        ST_Intersection(p.geom, c.geom),
        2
        )
    ) AS geom
    FROM bcc_open.paths_7856 AS p
    JOIN bcc_open.busstops_400_cov AS c
    ON ST_Intersects(p.geom, c.geom)
    WHERE NOT ST_IsEmpty(ST_Intersection(p.geom, c.geom));

        CREATE INDEX IF NOT EXISTS paths_served_400m_gix
    ON bcc_open.paths_served_400m
    USING gist (geom);
"""

with engine.begin() as conn:
    conn.execute(text(sql))

sql = """
    SELECT ROUND( (SUM(ST_Length(geom))/1000.0)::numeric, 3 ) AS served_km
    FROM bcc_open.paths_served_400m;
"""
with engine.begin() as conn:
    served_km = conn.execute(text(sql)).scalar()

print("served_km:", served_km)

sql = """
    SELECT ROUND(SUM(ST_Length(geom)/ 1000) ::numeric, 3) AS total_km
    FROM bcc_open.paths_7856;
"""
with engine.begin() as conn:
    total_km = conn.execute(text(sql)).scalar()

print("total_km:", total_km)

served_percent = round(100 * float(served_km) / float(total_km), 2) if total_km else None

print("serrved_percent: ", served_percent)
