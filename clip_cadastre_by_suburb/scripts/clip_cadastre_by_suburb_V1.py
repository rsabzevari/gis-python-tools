import geopandas as gpd
from pathlib import Path
import os
import tkinter as tk
from tkinter import filedialog


def clip_cadastre_to_suburb(
        cadastre_path,
        suburbs_path,
        suburb_name,
        out_folder,
        out_name,
        ext
) -> Path:
    ext = ext.lower()
    allowed_ext = {"shp", "gpkg", "geojson"}
    if ext not in allowed_ext:
        raise ValueError(f"Unsupported extension '{ext}'. Use one of: {allowed_ext}")
    

    #Read Cadastre:
    cadastre = gpd.read_file(cadastre_path)

    #Read suburbs:
    suburbs = gpd.read_file(suburbs_path)

    #selected_suburb = suburbs[suburbs["suburbname"] == suburb_name].copy()
    selected_suburb = suburbs[suburbs["suburbname"].str.upper() == suburb_name.upper()].copy()
    if selected_suburb.empty:
        raise ValueError(f"No features found for suburb '{suburb_name}' in 'suburbname' field.")
    
    #Check if CRS matches
    if cadastre.crs != suburbs.crs:
        raise ValueError(f"CRS mismatch: cadastre={cadastre.crs}, suburbs={suburbs.crs}")

    #Clip cadastre to the suburb
    clipped_cadastre = gpd.clip(cadastre, selected_suburb)
    if clipped_cadastre.empty:
        raise ValueError(f"Clip result is empty for suburb '{suburb_name}'. Check inputs.")

    #Write output:
    out_path = out_folder / f"{out_name}.{ext}"
    clipped_cadastre.to_file(out_path)
    return out_path
    


if __name__ == "__main__":
    # Start a hidden Tk window so we can use dialogs
    root = tk.Tk()
    root.withdraw()  # hide the empty main window

    # Ask user to choose cadastre file
    cadastre_path = filedialog.askopenfilename(
        title="Select cadastre layer",
        filetypes=[("Vector data", "*.shp *.gpkg *.geojson *.gdb"), ("All files", "*.*")]
    )
    if not cadastre_path:
        print("No cadastre selected, exiting.")
        raise SystemExit

    # Ask user to choose suburbs file
    suburbs_path = filedialog.askopenfilename(
        title="Select suburbs layer",
        filetypes=[("Vector data", "*.shp *.gpkg *.geojson *.gdb"), ("All files", "*.*")]
    )
    if not suburbs_path:
        print("No suburbs layer selected, exiting.")
        raise SystemExit

    # Ask user to choose output folder
    out_folder = filedialog.askdirectory(
        title="Select output folder"
    )
    if not out_folder:
        print("No output folder selected, exiting.")
        raise SystemExit

    # Normalise paths for Windows (fix //SERVER/... -> \\SERVER\...)
    if os.name == "nt":
        cadastre_path = cadastre_path.replace('/', '\\')
        suburbs_path = suburbs_path.replace('/', '\\')

    # TURN them into Path objects here 👇
    cadastre_path = Path(cadastre_path)
    suburbs_path = Path(suburbs_path)
    out_folder = Path(out_folder)

    # Ask suburb name (simple text input in terminal for now)
    suburb_name = input("Enter suburb name (exactly as in 'suburbname' field): ")

    out_name = input("Enter output layer name (e.g. Marsden_Cadastre): ")

    ext = input("Enter output extension (e.g. shp or gpkg): ")

    out_path = clip_cadastre_to_suburb(
        cadastre_path,
        suburbs_path,
        suburb_name,
        out_folder,
        out_name,
        ext
    )

    print(f"Exported to: {out_path}")
    


