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



