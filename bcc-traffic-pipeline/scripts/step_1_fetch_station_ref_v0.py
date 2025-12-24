import geopandas as gpd
import requests
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from db_config_local import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT


ENDPOINT = "https://data.nsw.gov.au/data/api/action/datastore_search"    # datastore_search endpoint
RESOURCE_ID = "f4092c24-87d8-44dc-b23d-83f2ff2a414f"                     # station ref resource id

#==================================================
# TASK M
#==================================================


payload = {
    "resource_id": RESOURCE_ID,
    "limit": 5,
    "filters": {"lga": "Blacktown"}
}

# 1) raise error if HTTP failed
# 2) convert to json
# 3) check success
# 4) get total
# 5) get records
# 6) print total
# 7) print first 3 station_keys

resp = requests.post(ENDPOINT, json=payload, timeout=60)
resp.raise_for_status()

data = resp.json()
if data.get("success") is not True:
    raise RuntimeError(f"CKAN returned success = false: {data}")

result = data["result"]
records = result["records"]
total = result["total"]

print("total: ", total)

for record in records:
    print("station_key: ", record["station_key"])

offset = 0


limit = 5
offset = 0
all_records = []
total = None
print("_________________________")

#==========================================
# TASK N 
#==========================================

while True:
    payload = {
        "resource_id": RESOURCE_ID,
        "limit": limit,
        "offset" : offset,
        "filters": {"lga": "Blacktown"}
    }
    resp = requests.post(ENDPOINT, json = payload, timeout = 60)
    data = resp.json()
    result = data["result"]
    records = result["records"]
    if total is None:
        total = result["total"]

    all_records.extend(records)
    
    if not records:
        break
    if len(all_records) >= total:
        break
        
    offset = offset + limit

print(total)
print(len(all_records))
print("last station key: ",all_records[-1]["station_key"])


#=================================================
# Task O - Insert Into PostGIS
#=================================================
password = quote_plus(DB_PASSWORD)

engine = create_engine(
   f"postgresql+psycopg2://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
print("engine object created") 

with engine.begin() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM bcc_traffic.station_reference")).scalar()

print(count)

# Task O1
first = all_records[0]
print(first.keys())
print(first["station_key"], first["station_id"], first["wgs84_latitude"], first["wgs84_longitude"])

rec = all_records[0]


params = {
    "station_key": str(rec["station_key"]),
    "station_id": str(rec["station_id"]),
    "lga": rec.get("lga"),
    "road_name": rec.get("road_name"),
    "suburb": rec.get("suburb"),
    "lat": float(rec["wgs84_latitude"]),
    "lon": float(rec["wgs84_longitude"]),
}

sql = text("""
INSERT INTO bcc_traffic.station_reference
  (station_key, station_id, lga, suburb, road_name, wgs84_latitude, wgs84_longitude, geom)
VALUES
  (:station_key, :station_id, :lga, :suburb,
:road_name, :lat, :lon, ST_SetSRID(ST_MakePoint(:lon, :lat), 4326)
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
    conn.execute(sql, params)

with engine.begin() as conn:
    row = conn.execute(text("""
        SELECT station_key, station_id, ST_SRID(geom) AS srid, ST_AsText(geom) AS wkt
        FROM bcc_traffic.station_reference
        WHERE station_key = :k
    """), {"k": params["station_key"]}).mappings().first()

print(row)

#==================================================
# TASK P
#==================================================

sql = text("""
INSERT INTO bcc_traffic.station_reference
(station_key, station_id, lga, suburb,road_name, wgs84_latitude, wgs84_longitude, geom)
VALUES
(:station_key, :station_id, :lga, :suburb,:road_name, :lat, :lon,
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

    for rec in all_records:

       params = {
            "station_key": str(rec["station_key"]),
            "station_id": str(rec["station_id"]),
            "lga": rec.get("lga"),
            "suburb": rec.get("suburb"),
            "road_name": rec.get("road_name"),
            "lat": float(rec["wgs84_latitude"]),
            "lon": float(rec["wgs84_longitude"]),
       }
       conn.execute(sql, params)

    n = conn.execute(text("SELECT COUNT(*) FROM bcc_traffic.station_reference;")).scalar_one()
print("rows now:", n)
