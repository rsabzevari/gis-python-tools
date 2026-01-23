"""
BCC Traffic Counts → Station Reference (V1)

A) Fetch station reference records from Data.NSW (CKAN datastore_search)
   - filter: lga = Blacktown
   - handle paging via limit/offset

B) Upsert into PostGIS (schema: bcc_traffic)
   - table: station_reference
   - geometry: Point (EPSG:4326)
"""

from __future__ import annotations

import requests
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

from db_config_local import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT


# ----------------------------
# Config
# ----------------------------
ENDPOINT = "https://data.nsw.gov.au/data/api/action/datastore_search"
RESOURCE_ID = "f4092c24-87d8-44dc-b23d-83f2ff2a414f"
LGA_FILTER = "Blacktown"
PAGE_SIZE = 1000

SCHEMA = "bcc_traffic"
TABLE = "station_reference"


# ----------------------------
# CKAN helpers
# ----------------------------
def ckan_fetch_page(resource_id: str, limit: int, offset: int, lga: str) -> tuple[list[dict], int]:
    """Fetch one page from CKAN datastore_search. Returns (records, total)."""
    payload = {
        "resource_id": resource_id,
        "limit": limit,
        "offset": offset,
        "filters": {"lga": lga},
    }

    resp = requests.post(ENDPOINT, json=payload, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    if data.get("success") is not True:
        raise RuntimeError(f"CKAN returned success=false. Response keys={list(data.keys())}")

    result = data.get("result")
    if not isinstance(result, dict) or "records" not in result or "total" not in result:
        raise ValueError(f"Unexpected CKAN result structure. Keys={list(result.keys()) if isinstance(result, dict) else type(result)}")

    records = result["records"]
    total = int(result["total"])

    if not isinstance(records, list):
        raise ValueError("CKAN result['records'] is not a list.")

    return records, total


def ckan_fetch_all(resource_id: str, page_size: int, lga: str) -> list[dict]:
    """Fetch all records with paging."""
    all_records: list[dict] = []
    offset = 0
    total = None

    while True:
        records, total_now = ckan_fetch_page(resource_id, page_size, offset, lga)
        if total is None:
            total = total_now

        all_records.extend(records)

        if not records:
            break
        if len(all_records) >= total:
            break

        offset += page_size

    return all_records


# ----------------------------
# DB helpers
# ----------------------------
def make_engine():
    password = quote_plus(DB_PASSWORD)
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def table_count(engine, schema: str, table: str) -> int:
    with engine.begin() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table};")).scalar())


def upsert_station_reference(engine, schema: str, table: str, records: list[dict]) -> int:
    """
    Bulk upsert station reference records into PostGIS.
    Returns final row count.
    """
    sql = text(f"""
    INSERT INTO {schema}.{table}
      (station_key, station_id, lga, suburb, road_name, wgs84_latitude, wgs84_longitude, geom)
    VALUES
      (:station_key, :station_id, :lga, :suburb, :road_name, :lat, :lon,
       ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
      )
    ON CONFLICT (station_key) DO UPDATE
    SET
      station_id = EXCLUDED.station_id,
      lga = EXCLUDED.lga,
      suburb = EXCLUDED.suburb,
      road_name = EXCLUDED.road_name,
      wgs84_latitude = EXCLUDED.wgs84_latitude,
      wgs84_longitude = EXCLUDED.wgs84_longitude,
      geom = EXCLUDED.geom;
    """)

    with engine.begin() as conn:
        for rec in records:
            # Defensive parsing
            try:
                params = {
                    "station_key": str(rec["station_key"]),
                    "station_id": str(rec["station_id"]),
                    "lga": rec.get("lga"),
                    "suburb": rec.get("suburb"),
                    "road_name": rec.get("road_name"),
                    "lat": float(rec["wgs84_latitude"]),
                    "lon": float(rec["wgs84_longitude"]),
                }
            except Exception as e:
                raise ValueError(f"Bad record encountered. Keys={list(rec.keys())}") from e

            conn.execute(sql, params)

        n = conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table};")).scalar_one()

    return int(n)


# ----------------------------
# Main
# ----------------------------
def main():
    # A) Fetch all
    records = ckan_fetch_all(RESOURCE_ID, PAGE_SIZE, LGA_FILTER)
    print(f"Fetched records: {len(records)}")
    if records:
        print("Last station_key:", records[-1].get("station_key"))

    # B) DB connect
    engine = make_engine()
    print("DB engine created")

    # optional pre-count (helps sanity checking)
    try:
        before = table_count(engine, SCHEMA, TABLE)
        print("Rows before:", before)
    except Exception:
        print("Could not count existing rows (table may not exist yet).")

    # C) Upsert
    after = upsert_station_reference(engine, SCHEMA, TABLE, records)
    print("Rows after:", after)


if __name__ == "__main__":
    main()
