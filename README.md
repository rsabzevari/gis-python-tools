# GIS Python Tools

Small collection of GIS utilities written in Python, mainly using GeoPandas.

## clip_cadastre_by_suburb_V1.py

This script clips a cadastre layer to a single suburb and exports the result.

### Features

- Opens file dialogs to select:
  - cadastre layer (e.g. NSW cadastre)
  - suburbs layer (with a `suburbname` field)
  - output folder
- Asks for:
  - suburb name (matched case-insensitively)
  - output file name
  - output format (`shp`, `gpkg`, or `geojson`)
- Validates:
  - output extension
  - suburb exists in the layer
  - CRS of cadastre and suburbs match
  - clip result is not empty

### Requirements

- Python 3.x  
- GeoPandas  
- Tkinter (comes with standard Python on Windows)