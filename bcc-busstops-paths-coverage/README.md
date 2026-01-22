# BCC Open Data → PostGIS Pipeline (V1)

A reproducible pipeline that downloads **Blacktown Council open data** from an **ArcGIS FeatureServer**, loads it into **PostGIS**, then performs a **bus stop coverage** analysis and computes a simple KPI for path/cycleway service coverage.

---

## What it does

### A) Download (ArcGIS FeatureServer → GeoJSON)
- **Bus stops** (points)
- **Paths / cycleways** (lines)

### B) Load into PostGIS (`schema: bcc_open`)
Creates/refreshes:
- `bcc_open.raw_busstops` (EPSG:4326) + GiST index  
- `bcc_open.raw_paths` (EPSG:4326) + GiST index  
- Optional validation: **API feature count vs DB row count**

### C) Derive & analyze (distance-safe in EPSG:7856 / meters)
1. Projected copies:
   - `bcc_open.busstops_7856` (Point, 7856)
   - `bcc_open.paths_7856` (MultiLineString, 7856)
2. Bus stop coverage:
   - `bcc_open.busstops_buffer_400` (400m buffers)
   - `bcc_open.busstops_400_cov` (dissolved coverage polygon)
3. Served paths (clipped by coverage):
   - `bcc_open.paths_served_400m`
4. KPI:
   - `served_km`, `total_km`, `served_percent`

> **Note:** buffer/length calculations are done in **EPSG:7856** (meters).

---

## Data sources

ArcGIS REST FeatureServer (NSW portal):
- Bus stops layer: `Hosted/Blacktown_Council_Data_Public/FeatureServer/0`
- Paths layer: `Hosted/Blacktown_Council_Data_Public/FeatureServer/1`

These endpoints are configured in the script as `BUSSTOPS_LAYER` and `PATHS_LAYER`.

---

## Tech stack

- Python
- GeoPandas + Requests (API fetching & GeoJSON handling)
- SQLAlchemy + psycopg2 (Postgres connection)
- Postgres + PostGIS (storage + spatial analysis)

---



