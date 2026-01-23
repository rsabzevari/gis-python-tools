# BCC Traffic Counts → PostGIS Pipeline (V1)

This project builds a simple, reproducible ETL pipeline that pulls **Blacktown traffic count data** from **Data.NSW (CKAN API)** and loads it into **PostGIS** for analysis and downstream workflows.

It is split into two steps:

- **Step 1 (V1):** Download + upsert **station reference** (stations + locations)  
- **Step 2 (V1):** Download + upsert **yearly summary** (traffic counts per station/year/period)

---

## What you get in PostGIS

All outputs are written to the `bcc_traffic` schema.

### Step 1 outputs
- `bcc_traffic.station_reference`
  - Station metadata + geometry (`POINT`, EPSG:4326)
  - Used as the “station list” source for Step 2

### Step 2 outputs
- `bcc_traffic.yearly_summary`
  - Yearly summary traffic-count records per station
  - Upserted using a composite unique key (see below)

---

## Data sources

These scripts use the Data.NSW CKAN endpoint:

- CKAN endpoint: `https://data.nsw.gov.au/data/api/action/datastore_search`

Resource IDs:
- **Station reference**: `f4092c24-87d8-44dc-b23d-83f2ff2a414f`
- **Yearly summary**: `f9e3216d-6f91-406e-935e-e3fd9423b9e3`

The scripts query the API using `limit/offset` paging and filter by `lga = Blacktown`.

---

## Project structure

bcc-traffic-pipeline/
README.md
scripts/
step_1_station_reference_v1.py
step_2_yearly_summary_v1.py
db_config_local.py # local only (DO NOT COMMIT)


> If your filenames/folders are different, update this tree to match your repo.

---

## Requirements

- Python 3.10+
- Postgres + PostGIS
- Python packages:
  - `requests`
  - `SQLAlchemy`
  - `psycopg2-binary`

> Note: GeoPandas is not required for Step 1/2 V1. Geometry is created in SQL using
> `ST_SetSRID(ST_MakePoint(lon, lat), 4326)`.

---

## Setup

### 1) Create a virtual environment

```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

2) Install dependencies
pip install requests SQLAlchemy psycopg2-binary

3) Add local DB config (DO NOT COMMIT)

Create this file:

bcc-traffic-pipeline/scripts/db_config_local.py

DB_USER = "postgres"
DB_PASSWORD = "your_password"
DB_NAME = "your_database"
DB_HOST = "localhost"
DB_PORT = "5432"


Make sure your repo .gitignore includes:

db_config_local.py
**/db_config_local.py
.env

4) Enable PostGIS

Run once in your database:

CREATE EXTENSION IF NOT EXISTS postgis;

Data sources

CKAN endpoint:

https://data.nsw.gov.au/data/api/action/datastore_search

Resource IDs:

Station reference: f4092c24-87d8-44dc-b23d-83f2ff2a414f

Yearly summary: f9e3216d-6f91-406e-935e-e3fd9423b9e3

Output tables

Schema: bcc_traffic

Step 1 — bcc_traffic.station_reference

Stores station metadata + point geometry (EPSG:4326).

Upsert key:

ON CONFLICT (station_key) DO UPDATE ...

Step 2 — bcc_traffic.yearly_summary

Stores yearly summary traffic-count rows per station.

Upsert key (composite):

(station_key, year, period, count_type, traffic_direction_seq, cardinal_direction_seq)

Run the pipeline

Run Step 1 first (so Step 2 can read station keys from the DB).

Step 1 — Station reference (V1)
python scripts/step_1_station_reference_v1.py


Typical outputs:

fetched record count

rows before/after in bcc_traffic.station_reference

Step 2 — Yearly summary (V1)
python scripts/step_2_yearly_summary_v1.py


Typical outputs:

station count

per-station progress (fetched / upserted)

final DB row count in bcc_traffic.yearly_summary

Quick sanity-check SQL

Station count:

SELECT COUNT(*) FROM bcc_traffic.station_reference;


Yearly summary count:

SELECT COUNT(*) FROM bcc_traffic.yearly_summary;


Rows per station:

SELECT station_key, COUNT(*) AS n
FROM bcc_traffic.yearly_summary
GROUP BY station_key
ORDER BY n DESC;

Notes (V1)

The scripts use CKAN paging (limit/offset) to fetch all records.

Step 2 runs station-by-station and upserts row-by-row for clarity and reproducibility.

Next improvements (V2 ideas)

Batch upsert (executemany) for big speed gains

Retry/backoff for CKAN requests if the API rate-limits

Resume mode (skip stations already loaded)

Store run metadata (timestamp, totals) in a log table

Add CLI args (station_key, page_size, lga)
