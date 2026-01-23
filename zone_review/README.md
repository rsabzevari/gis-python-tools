# zone_review — Multi-zone lots (slice extraction)

This folder contains **two implementations** of the same analysis:

1) **SQL (PostGIS)** — creates a table of per-lot “slices” where a single cadastre lot intersects **more than one zoning class**.  
2) **Python (GeoPandas)** — does the same using file inputs (cadastre + zoning) and exports to a GIS file.

Output is a dataset/table named **`multi_zone_slices`** with:
- `cadid`
- `LAY_CLASS`
- `SYM_CODE`
- `cad_area`
- `slice_area`
- `coverage` (slice % of the lot)
- `geometry/geom`

---

## Folder structure

