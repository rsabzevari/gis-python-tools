# Spatial QA/QC — V1 (GeoPandas)

A lightweight **spatial data QA/QC** script built with **GeoPandas + Shapely**.

It loads **one vector layer** (SHP / GPKG / GeoJSON / GDB*), runs a small set of geometry checks, and produces:

- **`errors.gpkg`** — a layer of only the features that failed checks (with `check`, `severity`, `message`)
- **`report_*.json`** — a JSON report with dataset summary, failure counts, and PASS/FAIL status

> \* FileGDB support depends on your environment/driver setup.

---

## What V1 checks

### Geometry quality (ERROR)
- **Missing geometry** (`NaN`)
- **Empty geometry** (e.g. `POINT EMPTY`)
- **Invalid geometry** (self-intersections, ring issues, etc.)

### Geometry duplicates (WARN)
- **Duplicate geometry** using `normalize() + WKB`  
  (helps catch duplicates where vertex order / ring orientation differs)

---

## Outputs

### 1) Errors layer: `errors_<input_name>.gpkg`
Contains only features that triggered checks, with extra columns:

- `__rowid__` — internal stable row identifier
- `check` — check name (e.g. `invalid_geometry`)
- `severity` — `ERROR` or `WARN`
- `message` — human-readable description
- `geometry` — original geometry of the failing feature

Open this layer in **QGIS** to inspect problems visually.

### 2) Report: `report_<input_name>.json`
Includes:

- runtime timestamp
- PASS/FAIL status
- summary stats (feature count, CRS, bbox, geometry types)
- failure counts by severity and by check

---

## How to run

From your environment (example):

```bash
python spatial_qaqc_v1.py
