import geopandas as gpd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from db_config_local import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT

password = quote_plus(DB_PASSWORD) #make my password safe to put inside a URL string (# handles @ etc.)
engine = create_engine(
   f"postgresql+psycopg2://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
#print("engine object created") 

# try:
#     with engine.connect() as conn:
#         conn.execute(text("SELECT 1"))
#         print("DB connection OK")
# except Exception as e:
#     print("DB connection FAILED:")
#     print(e)

def clip_cadastre_by_suburb(suburb_name: str) -> gpd.GeoDataFrame:
   
    """
    Load suburbs + cadastre from PostGIS and return cadastre clipped to one suburb.

    Raises:
        ValueError: if the suburb does not exist in Blacktown_suburbs.
    """
   
        
    # 1. Load suburbs first (lighter)
    suburbs = gpd.read_postgis(
    'SELECT * FROM clip_cadastre.blacktown_suburbs',
    engine,
    geom_col="geom"
    )
    chosen = suburbs[suburbs["suburbname"] == suburb_name].copy()

    if chosen.empty:
        # No matching suburb in the table → don't load cadastre at all
        raise ValueError(f"Suburb '{suburb_name}' not found in Blacktown_suburbs")
    
    # 2. Only now load cadastre (heavier)
    cadastre = gpd.read_postgis(
        "SELECT * FROM clip_cadastre.cadastre",
        engine,
        geom_col="geom"
        )
    
    # 3. Clip
    result = gpd.overlay(cadastre, chosen, how= 'intersection')
    return result



if __name__ == "__main__":
    
    while True:
        suburb = input("Enter suburb name: ").strip().upper()

        try:
            gdf = clip_cadastre_by_suburb(suburb)
            break
        except ValueError as e:
            print(e)
            print("Please check the spelling and try again, or press Ctrl+C to exit.\n")

    print(f"{len(gdf)} parcels in {suburb}")
    gdf.to_file(fr"C:\Users\sabzer\Downloads\Test\{suburb}_cadastre.gpkg")
    gdf.to_postgis(
        f"{suburb}_cadastre",
        engine,
        schema= "clip_cadastre",
        if_exists='replace',
        index=False
    )
    
    



