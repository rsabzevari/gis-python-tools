"""
Spatial QA/QC — V1

Outputs:
- errors layer (GeoPackage): features that failed checks (check / severity / message)
- report (JSON): dataset summary + failure counts + PASS/FAIL status

Checks included:
- Missing geometry (ERROR)
- Empty geometry (ERROR)
- Invalid geometry (ERROR)
- Duplicate geometry via normalize() + WKB (WARN)

Notes:
- __rowid__ is an internal stable row identifier used to link failures back to source features.
- Failures are stored as a plain DataFrame (lightweight), then merged back to geometry for export.
"""

import json
import os
from datetime import datetime
from pathlib import Path
import tkinter as tk
from tkinter import filedialog

import pandas as pd
import geopandas as gpd
import shapely


# -------------------------------------------------------------------
# Helpers: prepare + summary
# -------------------------------------------------------------------
def add_rowid(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Add a stable internal row id so checks can always reference original rows.
    """
    if "__rowid__" not in gdf.columns:
        gdf["__rowid__"] = gdf.index
    return gdf


def summarize(gdf: gpd.GeoDataFrame) -> dict:
    """
    Return a JSON-friendly summary dict (no masks, no geometry).
    """
    geom_types = gdf.geom_type.value_counts(dropna=False).to_dict()
    bbox = gdf.total_bounds.tolist() if len(gdf) != 0 else None

    qc = geometry_quality_check(gdf)
    dup = geometry_duplicate_check(gdf)

    return {
        "feature_count": len(gdf),
        "crs": str(gdf.crs),
        "bounding_box": bbox,
        "geometry_type_counts": geom_types,
        "missing_geometry_count": qc["missing"],
        "empty_geometry_count": qc["empty"],
        "invalid_geometry_count": qc["invalid"],
        "good_geometry_count": int(qc["good_mask"].sum()),
        "duplicate_geometry_count": dup["duplicated"],
        "columns": gdf.columns.to_list(),
    }


# -------------------------------------------------------------------
# 1) Geometry quality check (counts + masks)
# -------------------------------------------------------------------
def geometry_quality_check(gdf: gpd.GeoDataFrame) -> dict:
    """
    Core geometry QA.

    Masks:
    - missing_mask: geometry is NULL/NaN
    - empty_mask: geometry exists but is EMPTY (e.g., POINT EMPTY)
    - good_mask: not missing and not empty
    - invalid_mask: invalid geometries among good_mask
    - valid_mask: valid geometries among good_mask
    """
    missing_mask = gdf.geometry.isna()
    empty_mask = (~missing_mask) & gdf.geometry.is_empty

    good_mask = ~(missing_mask | empty_mask)

    invalid_mask = good_mask & (~gdf.is_valid)
    valid_mask = good_mask & gdf.is_valid

    return {
        "missing": int(missing_mask.sum()),
        "empty": int(empty_mask.sum()),
        "invalid": int(invalid_mask.sum()),
        "bad_total": int(missing_mask.sum() + empty_mask.sum() + invalid_mask.sum()),
        "missing_mask": missing_mask,
        "empty_mask": empty_mask,
        "good_mask": good_mask,
        "invalid_mask": invalid_mask,
        "valid_mask": valid_mask,
    }


# -------------------------------------------------------------------
# 2) Duplicate geometry check (normalize + WKB)
# -------------------------------------------------------------------
def geometry_duplicate_check(gdf: gpd.GeoDataFrame) -> dict:
    """
    Duplicate geometry check using normalize() + WKB.

    Useful when the same geometry is stored with different vertex order/orientation.

    Returns:
    - duplicated: number of rows involved in duplicates
    - duplicated_mask: full-length boolean mask aligned to gdf.index
    - n_checked: number of rows checked (valid geometries only)
    - method: label for reporting/debugging
    """
    qc = geometry_quality_check(gdf)
    valid_mask = qc["valid_mask"]
    n_checked = int(valid_mask.sum())

    if n_checked == 0:
        return {
            "duplicated": 0,
            "duplicated_mask": pd.Series(False, index=gdf.index),
            "n_checked": 0,
            "method": "normalize_wkb",
        }

    valid_geoms = gdf.loc[valid_mask, "geometry"]

    # Ensure normalize() exists in this environment
    sample_geom = valid_geoms.iloc[0]
    if not hasattr(sample_geom, "normalize"):
        raise RuntimeError("Geometry.normalize() not available. Upgrade Shapely/GeoPandas.")

    geom_key = valid_geoms.apply(lambda geom: geom.normalize().wkb)
    dup_valid = geom_key.duplicated(keep=False)

    duplicated_mask = pd.Series(False, index=gdf.index)
    duplicated_mask.loc[dup_valid[dup_valid].index] = True

    return {
        "duplicated": int(duplicated_mask.sum()),
        "duplicated_mask": duplicated_mask,
        "n_checked": n_checked,
        "method": "normalize_wkb",
    }


# -------------------------------------------------------------------
# 3) Failures tables (DataFrames)
# -------------------------------------------------------------------
def geometry_quality_failures(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Convert geometry quality masks into a failures table:
    columns: __rowid__, check, severity, message
    """
    qc = geometry_quality_check(gdf)

    missing_fail = pd.DataFrame({
        "__rowid__": gdf.loc[qc["missing_mask"], "__rowid__"],
        "check": "missing_geometry",
        "severity": "ERROR",
        "message": "Geometry is missing (NaN)",
    })

    empty_fail = pd.DataFrame({
        "__rowid__": gdf.loc[qc["empty_mask"], "__rowid__"],
        "check": "empty_geometry",
        "severity": "ERROR",
        "message": "Geometry is empty",
    })

    invalid_fail = pd.DataFrame({
        "__rowid__": gdf.loc[qc["invalid_mask"], "__rowid__"],
        "check": "invalid_geometry",
        "severity": "ERROR",
        "message": "Geometry is invalid",
    })

    return pd.concat([missing_fail, empty_fail, invalid_fail], ignore_index=True)


def geometry_duplicate_failures(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Convert duplicated_mask into a failures table (usually WARN).
    """
    dup = geometry_duplicate_check(gdf)

    duplicated_fail = pd.DataFrame({
        "__rowid__": gdf.loc[dup["duplicated_mask"], "__rowid__"],
        "check": "duplicate_geometry",
        "severity": "WARN",
        "message": "Geometry is duplicated (normalize+wkb)",
    })

    return duplicated_fail.reset_index(drop=True)


def geometry_failure_all(gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Combine geometry-quality failures and duplicate-geometry warnings.
    """
    return pd.concat(
        [geometry_quality_failures(gdf), geometry_duplicate_failures(gdf)],
        ignore_index=True,
    )


# -------------------------------------------------------------------
# 4) Errors layer builder + writers
# -------------------------------------------------------------------
def error_export(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Build an errors GeoDataFrame:
    - one row per failure
    - includes geometry (joined back from the original layer using __rowid__)
    """
    failures = geometry_failure_all(gdf)

    base = gdf[["__rowid__", "geometry"]]
    errors = failures.merge(base, on="__rowid__", how="left")

    return gpd.GeoDataFrame(errors, geometry="geometry", crs=gdf.crs)


def write_errors_file(
    gdf: gpd.GeoDataFrame,
    out_path: str,
    layer_name: str = "errors",
    driver: str = "GPKG",
) -> None:
    """
    Build errors GeoDataFrame and write it to disk.
    """
    errors_gdf = error_export(gdf)
    errors_gdf.to_file(out_path, layer=layer_name, driver=driver)


# -------------------------------------------------------------------
# 5) Report builder + writer
# -------------------------------------------------------------------
def report_builder(gdf: gpd.GeoDataFrame, layer_name: str = "layer") -> dict:
    """
    Build a JSON-friendly report dict from:
    - summarize() (dataset info)
    - geometry_failure_all() (failures table)
    """
    summary = summarize(gdf)
    failures = geometry_failure_all(gdf)

    by_severity = failures["severity"].value_counts(dropna=False).to_dict()
    by_check = failures["check"].value_counts(dropna=False).to_dict()

    status = "FAIL" if (failures["severity"] == "ERROR").sum() > 0 else "PASS"

    return {
        "runtime": datetime.now().isoformat(timespec="seconds"),
        "layer_name": layer_name,
        "status": status,
        "summary": summary,
        "failures": {
            "total_failures": len(failures),
            "by_severity": by_severity,
            "by_check": by_check,
        },
    }


def write_report_json(report: dict, out_path: str) -> None:
    """
    Save a report dict to a JSON file on disk.
    """
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)


# -------------------------------------------------------------------
# Manual run (V1) — file dialogs (same style as clip cadastre)
# -------------------------------------------------------------------
def main() -> None:
    print("Shapely version:", shapely.__version__)

    # Start a hidden Tk window so we can use dialogs
    root = tk.Tk()
    root.withdraw()

    # Ask user to choose input file
    input_path = filedialog.askopenfilename(
        title="Select input layer",
        filetypes=[("Vector data", "*.shp *.gpkg *.geojson *.gdb"), ("All files", "*.*")]
    )
    if not input_path:
        print("No input selected, exiting.")
        raise SystemExit

    # Ask user to choose output folder
    out_folder = filedialog.askdirectory(title="Select output folder")
    if not out_folder:
        print("No output folder selected, exiting.")
        raise SystemExit

    # Normalise paths for Windows (fix //SERVER/... -> \\SERVER\...)
    if os.name == "nt":
        input_path = input_path.replace("/", "\\")
        out_folder = out_folder.replace("/", "\\")

    # Convert to Path objects
    input_path = Path(input_path)
    out_folder = Path(out_folder)

    # Use input filename (without extension) as layer_name in the report + output filenames
    layer_name = input_path.stem

    # Load one layer (single-layer files; V2 can add GPKG layer picker)
    gdf = gpd.read_file(input_path)
    gdf = add_rowid(gdf)

    print("\nInput layer preview:")
    print(gdf.head())

    # Output paths (named using the input filename stem)
    errors_path = out_folder / f"errors_{layer_name}.gpkg"
    report_path = out_folder / f"report_{layer_name}.json"

    # Write outputs
    write_errors_file(gdf, str(errors_path), layer_name="errors", driver="GPKG")
    report = report_builder(gdf, layer_name=layer_name)
    write_report_json(report, str(report_path))

    print("\nDone. Outputs created:")
    print("-", errors_path)
    print("-", report_path)


if __name__ == "__main__":
    main()
