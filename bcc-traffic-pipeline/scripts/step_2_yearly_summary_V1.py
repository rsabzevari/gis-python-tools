"""
BCC Traffic Counts → Yearly Summary (V1)

What this script does
---------------------
A) Fetch yearly summary records from Data.NSW (CKAN datastore_search)
   - dataset: yearly summary (RESOURCE_ID)
   - per station_key (paged with limit/offset)

B) Upsert into PostGIS (schema: bcc_traffic)
   - table: yearly_summary
   - unique key: (station_key, year, period, count_type, traffic_direction_seq, cardinal_direction_seq)
   - updates: classification_type, traffic_count

Notes
-----
- Uses station keys from bcc_traffic.station_reference
- CKAN responses are validated (HTTP + success flag + structure)
"""

from __future__ import annotations

import time
import requests
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

from db_config_local import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT


# ----------------------------
# Config
# ----------------------------
ENDPOINT = "https://data.nsw.gov.au/data/api/action/datastore_search"
YEARLY_RESOURCE_ID = "f9e3216d-6f91-406e-935e-e3fd9423b9e3"

SCHEMA = "bcc_traffic"
STATION_TABLE = "station_reference"
YEARLY_TABLE = "yearly_summary"

PAGE_SIZE = 1000
REQUEST_TIMEOUT = 60

# Optional: slow down a tiny bit to be nice to the API
SLEEP_BETWEEN_STATIONS_SEC = 0.05

# Optional: quick smoke test (set to a station key string or None)
SMOKE_TEST_STATION_KEY = None  # e.g. "57299"


# ----------------------------
# DB helpers
# ----------------------------
def make_engine():
    password = quote_plus(DB_PASSWORD)
    return create_engine(
        f"postgresql+psycopg2://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )


def fetch_station_keys(engine) -> list[str]:
    sql = text(f"""
        SELECT station_key
        FROM {SCHEMA}.{STATION_TABLE}
        ORDER BY station_key;
    """)
    with engine.begin() as conn:
        return [str(x) for x in conn.execute(sql).scalars().all()]


def table_count(engine, schema: str, table: str) -> int:
    with engine.begin() as conn:
        return int(conn.execute(text(f"SELECT COUNT(*) FROM {schema}.{table};")).scalar())


# ----------------------------
# CKAN helpers
# ----------------------------
def ckan_post(payload: dict) -> dict:
    """POST to CKAN datastore_search with validation."""
    resp = requests.post(ENDPOINT, json=payload, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()

    if data.get("success") is not True:
        raise RuntimeError(f"CKAN success=false. Top-level keys={list(data.keys())}")

    result = data.get("result")
    if not isinstance(result, dict) or "records" not in result or "total" not in result:
        raise ValueError("Unexpected CKAN structure: missing result/records/total")

    return result


def fetch_yearly_for_station(station_key: str, page_size: int = PAGE_SIZE) -> list[dict]:
    """Fetch ALL yearly-summary rows for one station_key (paged)."""
    all_records: list[dict] = []
    offset = 0
    total = None

    while True:
        payload = {
            "resource_id": YEARLY_RESOURCE_ID,
            "limit": page_size,
            "offset": offset,
            "filters": {"station_key": station_key},
        }

        result = ckan_post(payload)

        if total is None:
            total = int(result["total"])

        records = result["records"]
        if not records:
            break

        all_records.extend(records)

        if len(all_records) >= total:
            break

        offset += page_size

    return all_records


# ----------------------------
# Upsert
# ----------------------------
UPSERT_SQL = text(f"""
INSERT INTO {SCHEMA}.{YEARLY_TABLE}
  (station_key, year, period, count_type, classification_type,
   traffic_direction_seq, cardinal_direction_seq, traffic_count)
VALUES
  (:station_key, :year, :period, :count_type, :classification_type,
   :traffic_direction_seq, :cardinal_direction_seq, :traffic_count)
ON CONFLICT (station_key, year, period, count_type, traffic_direction_seq, cardinal_direction_seq)
DO UPDATE SET
  classification_type = EXCLUDED.classification_type,
  traffic_count = EXCLUDED.traffic_count;
""")


def normalize_row(rec: dict) -> dict:
    """
    Convert CKAN record types safely and return SQL params.
    Raises a helpful error if required fields are missing.
    """
    try:
        return {
            "station_key": str(rec["station_key"]),
            "year": int(rec["year"]),
            "period": str(rec["period"]),
            "count_type": str(rec["count_type"]),
            "classification_type": rec.get("classification_type"),
            "traffic_direction_seq": int(rec["traffic_direction_seq"]),
            "cardinal_direction_seq": int(rec["cardinal_direction_seq"]),
            "traffic_count": int(rec["traffic_count"]),
        }
    except Exception as e:
        raise ValueError(f"Bad yearly summary record. Keys={list(rec.keys())}") from e


def upsert_yearly_rows(engine, rows: list[dict]) -> int:
    """Upsert a list of yearly rows. Returns number of executed rows."""
    if not rows:
        return 0

    with engine.begin() as conn:
        for rec in rows:
            conn.execute(UPSERT_SQL, normalize_row(rec))

    return len(rows)


# ----------------------------
# Main
# ----------------------------
def main():
    engine = make_engine()
    print("DB engine created")

    # station list
    if SMOKE_TEST_STATION_KEY:
        station_keys = [SMOKE_TEST_STATION_KEY]
        print(f"SMOKE TEST mode: only station_key={SMOKE_TEST_STATION_KEY}")
    else:
        station_keys = fetch_station_keys(engine)
        print(f"Stations found: {len(station_keys)}")
        print("First 3:", station_keys[:3])

    # optional pre-count
    try:
        before = table_count(engine, SCHEMA, YEARLY_TABLE)
        print("Rows before (yearly_summary):", before)
    except Exception:
        print("Could not count yearly_summary (table may not exist yet).")

    total_fetched = 0
    total_upserted = 0

    for i, station_key in enumerate(station_keys, start=1):
        rows = fetch_yearly_for_station(station_key, PAGE_SIZE)
        n_up = upsert_yearly_rows(engine, rows)

        total_fetched += len(rows)
        total_upserted += n_up

        print(f"[{i}/{len(station_keys)}] station_key={station_key} | fetched={len(rows)} | upserted={n_up}")

        if SLEEP_BETWEEN_STATIONS_SEC:
            time.sleep(SLEEP_BETWEEN_STATIONS_SEC)

    # final count
    try:
        after = table_count(engine, SCHEMA, YEARLY_TABLE)
        print("Rows after (yearly_summary):", after)
    except Exception:
        after = None

    print("Done.")
    print("total_fetched:", total_fetched)
    print("total_upserted:", total_upserted)
    if after is not None:
        print("db_total_rows:", after)


if __name__ == "__main__":
    main()
