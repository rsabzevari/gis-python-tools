import geopandas as gpd
from pathlib import Path
import time

def main():
    cadastre_path = Path(input("Please enter the cadastre path: ").strip())
    zone_path = Path(input("Please enter the zoning path: ").strip())
    out_folder = Path(input("Please enter the output folder: "))
    out_name = input("Please enter the output name: ")
    ext = input("Please enter the file extension from {shp, gpkg,geojson}: ")
    out_path = get_multi_zone_slices(cadastre_path, zone_path,out_folder, out_name, ext)
    print(f"The output has been exported to {out_path}")


def get_multi_zone_slices (
        cadastre_path,
        zone_path,
        out_folder,
        out_name,
        ext
) -> Path: 
    
    ext = ext.lower().strip().lstrip(".")
    allowed = {"shp", "gpkg", "geojson"}
    if ext not in allowed:
        raise ValueError(f"Unsupported extension '{ext}'. Use one of: {allowed}")

    cadastre = gpd.read_file(cadastre_path)
    
    zone = gpd.read_file(zone_path)

    # Reproject both layers to a common target CRS (EPSG:7856)
    target_epsg = 7856

    if cadastre.crs is None:
        raise ValueError("Cadastre layer has no CRS defined.")

    if zone.crs is None:
        raise ValueError("Zoning layer has no CRS defined.")

    if cadastre.crs.to_epsg() != target_epsg:
        cadastre = cadastre.to_crs(epsg=target_epsg)
        print("Cadastre reprojected to EPSG:7856")

    if zone.crs.to_epsg() != target_epsg:
        zone = zone.to_crs(epsg=target_epsg)
        print("Zoning reprojected to EPSG:7856")


    #Add cad_area column to cadastre:
    cadastre["cad_area"] = cadastre.geometry.area

    #Create intersection:
    intersected = gpd.overlay(cadastre,zone, how="intersection")

    # Filter cadids with more than one zone:

    #create zone_count table for each cadid showing the number of zones for each cadid
    zone_count = intersected.groupby("cadid")["LAY_CLASS"].nunique().reset_index(name = "n_zones")
    
    multi_zone = zone_count[zone_count["n_zones"] > 1].copy()

    # Create a list of above to be able to filter intersected gdf
    multi_zone_list = multi_zone["cadid"]

    # Get the rows in intersected which cadids are in the above list
    slices = intersected[intersected["cadid"].isin(multi_zone_list)].copy()

    # Calculate area and coverage for each slice
    slices["slice_area"] = slices.geometry.area
    slices["coverage"] = slices["slice_area"] / slices["cad_area"] * 100

    #Filter only the rows we need
    multi_zone_slices = slices[["cadid", "LAY_CLASS", "SYM_CODE", "cad_area", "slice_area", "coverage", "geometry"]]

    out_path = out_folder / f"{out_name}.{ext}"

    multi_zone_slices.to_file(out_path)

    return out_path

if __name__ == "__main__":
    start = time.time()
    main()
    end = time.time()
    print(f"Runtime: {end - start:.2f} seconds")