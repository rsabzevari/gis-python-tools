import geopandas as gpd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from db_config_local import DB_USER, DB_PASSWORD, DB_NAME, DB_HOST, DB_PORT
import time
import antigravity

def main():
    password = quote_plus(DB_PASSWORD) #make my password safe to put inside a URL string (# handles @ etc.)
    engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )

    sql = """
        CREATE INDEX IF NOT EXISTS cadastre_geom_gix
        ON zone_review.state_cadastre
        USING gist (geom);

        CREATE INDEX IF NOT EXISTS zone_geom_gix
        ON zone_review.state_zone
        USING gist(geom);

    """
    with engine.begin() as conn:
        conn.execute(text(sql))

    multi_zone_sql =         """
        WITH intersected AS (
            -- Intersect lots and zones → one row per (lot, zone) slice
            SELECT 
                c.cadid,
                ST_Area(c.geom) AS cad_area, 
                z."LAY_CLASS",
                z."SYM_CODE",
                ST_Intersection(c.geom, z.geom) as geom
            FROM zone_review.state_cadastre as c
            JOIN zone_review.state_zone as z
                ON ST_Intersects(c.geom, z.geom)
        ),
        multi_zones AS (
            -- Keep only lots that have more than 1 distinct zoning class
            SELECT
                cadid,
                COUNT(DISTINCT "LAY_CLASS") as n_zones
            FROM intersected
            GROUP BY cadid
            HAVING COUNT(DISTINCT "LAY_CLASS") > 1
            ),
            slices AS (
                SELECT
                    i.cadid,
                    i.cad_area,
                    i."LAY_CLASS",
                    i."SYM_CODE",
                    i.geom
                FROM intersected as i
                JOIN multi_zones as m
                    ON (i.cadid = m.cadid)
        ),
        slice_area AS (
            -- Compute slice_area once so we can reuse it for coverage
            SELECT 
                cadid,
                cad_area,
                "LAY_CLASS",
                "SYM_CODE",
                ST_Area(geom) AS slice_area,
                geom
            FROM slices
            
        )
                
    SELECT
        cadid,
        cad_area,
        "LAY_CLASS",
        "SYM_CODE",
        slice_area,
        slice_area / cad_area * 100 AS coverage,
        geom
    FROM slice_area; 
        """
    

    multi_zone_slices = gpd.read_postgis(
        multi_zone_sql,
        engine,
        "geom"
    )
    multi_zone_slices.to_file(r"C:\Users\sabzer\Downloads\Test\test-postgis-v1")


if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(f"Runtime: {end - start:.2f} seconds")


    # gpd.read_postgis() onle nees a single select query (with ... AS is also considered as a single query)

    # We can either write in string and path to the function or read from a .sql file and path the gdf to the function. 
    #the only point is the .sql file should only contains one single query

    #For other types of queries we use engine.begin() and write multiple sql command to it.