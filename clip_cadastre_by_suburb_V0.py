import geopandas as gpd
from pathlib import Path


#----Reading Data
cadastre_path = r"J:\Strategic Planning\Data sharing\LISS_BaseMap\State_Cadastre_20251105.shp"

cadastre = gpd.read_file(cadastre_path)
print("Cadastre has been loaded")

suburbs_path = r"\\bccfiler\shared\Shared\DEPT\PD\SPED\7. Data and Analytics\03_GIS\BCC Internal\Suburbs\Blacktown suburbs - sent April 2025\Blacktown_suburb_202209.shp"

suburbs = gpd.read_file(suburbs_path)
print("Cliper has ben loaded")

#----Pick the suburb
print(suburbs.columns)
name_field = "suburbname"
print(suburbs[name_field]) #-- print(suburbs[name_field].unique()) in case there are many records

marsden_park = suburbs[suburbs[name_field] == 'MARSDEN PARK'].copy()
print('Marsden Park has been selected')

marsden_cadastre = gpd.clip(cadastre, marsden_park)
 
base = Path(r"C:\Users\sabzer\Downloads\Test")
file_name = "MarsdenParkCadastre"
ext = "gpkg"
out_path = base / f"{file_name}.{ext}"

marsden_cadastre.to_file(out_path)
print("Marsden Park Cadastre has been exported")