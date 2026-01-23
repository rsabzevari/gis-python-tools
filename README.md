# GIS Python Tools

A collection of small, practical GIS mini-projects using **Python (GeoPandas)** and **PostGIS**.  
Each project lives in its own folder with its own README.

---

## Projects

### 1) `bcc-busstops-paths-coverage/`
Bus stop coverage and served-path analysis (GeoPandas + optional PostGIS).  
See the folder README for workflow, outputs, and how to run.

### 2) `bcc-traffic-pipeline/`
Traffic counts pipeline utilities and SQL scripts.  
- `scripts/` contains Python utilities (V1 recommended)
- `sql/` contains SQL steps and helpers  
See the folder README(s) for run steps.

### 3) `clip_cadastre_by_suburb/`
Clip cadastre parcels to a chosen suburb.
- **File-based** GeoPandas version (select inputs via dialogs, export to shp/gpkg/geojson)
- **PostGIS** version (load cadastre/suburbs from DB, clip, export/write back)  
➡️ Open `clip_cadastre_by_suburb/README.md`

### 4) `zone_review/`
Multi-zone lot slice extraction (cadastre × zoning intersection).
- SQL (PostGIS) and Python (GeoPandas) implementations  
➡️ Open `zone_review/README.md`

---

## Setup

### Requirements
Typical packages used across projects:
- `geopandas`
- `sqlalchemy`
- `psycopg2-binary`
- `requests`

Example install:
```bash
pip install geopandas sqlalchemy psycopg2-binary requests
