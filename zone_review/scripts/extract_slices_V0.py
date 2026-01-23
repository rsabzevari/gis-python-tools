import geopandas as gpd
from pathlib import Path
import matplotlib.pyplot as plt

# ----- Loading Data -----
# Load full cadastre layer (lot polygons)
cadastre = gpd.read_file(r"\\bccfiler\shared\Shared\DEPT\PD\SPED\7. Data and Analytics\03_GIS\BCC Internal\Fixing zoning layers\05 June 2025\SS cadastre 5 June 25 (lot road unidentf waterf) - FG GDA2020.gpkg")
print(f"cadaste loaded with {len(cadastre)} features")

# Calculate total lot area for each cadastre feature (in current CRS units)
cadastre["cad_area"] = cadastre.geometry.area

# Load zoning layer (already in EPSG:7856 according to filename)
zoning = gpd.read_file(r"\\BCCFILER\shared\Shared\DEPT\PD\SPED\7. Data and Analytics\03_GIS\BCC Internal\Fixing zoning layers\05 June 2025\BCC Land Zoning (DPHI layer)-EPSG7856.gpkg")
print("zoning loaded!")

# Keep only the fields needed for the overlay analysis
zoning_shrinked = zoning[["LAY_CLASS", "SYM_CODE", "geometry"]]

# ---- Check CRS ----

# Quick check: confirm zoning fields
print(zoning_shrinked.columns)

# Basic CRS consistency check between cadastre and zoning
if cadastre.crs != zoning.crs:
    print("Cadastre and zoning CRS do not match!")
    exit

# Spatial overlay: intersect cadastre with zoning to get per-lot, per-zone slices
intersected = gpd.overlay(cadastre, zoning_shrinked, how='intersection')

# For each cadid, count how many distinct zoning classes (LAY_CLASS) it has
zone_counts = intersected.groupby('cadid')['LAY_CLASS'].nunique().reset_index(name = 'n_zones')
print(zone_counts.head())

# Keep only lots (cadid) that have more than one zoning class
multi_zone_lots = zone_counts[zone_counts["n_zones"] > 1]

# Extract the list of multi-zoned cadid values
multi_list = multi_zone_lots['cadid']

# Filter intersected slices down to only those belonging to multi-zoned lots
# .copy() to avoid SettingWithCopyWarning when adding new fields
slices = intersected[intersected['cadid'].isin(multi_list)].copy()

# Calculate area of each intersected slice
slices['slice_area'] = slices.geometry.area

# Calculate coverage of each slice as percentage of the whole lot area
slices['coverage'] = slices['slice_area']/ slices['cad_area'] * 100

# Final review GeoDataFrame: only keep key attributes and geometry
cad_slices = slices[['cadid', 'LAY_CLASS', 'SYM_CODE', 'cad_area','slice_area','coverage', 'geometry']]

out_path = r"C:\Users\sabzer\Downloads\Test\multi_zones_slices.gpkg"

cad_slices.to_file(out_path)

# ax = intersected.plot(figsize=(10, 10))
# ax.set_title("intersected")
# plt.show()


