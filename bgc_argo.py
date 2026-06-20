"""
BGC-ARGO FLOAT DATA PROCESSING PIPELINE


Author: Md. Zuhaib Kabir

DESCRIPTION:
This script downloads, filters, processes, and combines
Biogeochemical Argo (BGC-Argo) float profile data into a
single cleaned and vertically gridded NetCDF file.

The pipeline uses BGC-Argo synthetic profile index files and
merged Sprof NetCDF files to extract biogeochemical variables
within the selected study region and time period.

Main Steps:
1. Mount Google Drive in Google Colab
2. Download the BGC-Argo synthetic profile index file
3. Filter BGC-Argo profiles by study region and time
4. Convert profile file paths to merged Sprof file paths
5. Download regional BGC-Argo Sprof NetCDF files in batches
6. Store downloaded Sprof files as ZIP archives
7. Automatically detect available BGC variables
8. Remove auxiliary pressure-displacement variables such as *_dPRES
9. Apply quality-control filtering to BGC and pressure variables
10. Select adjusted variables where available, otherwise use raw variables
11. Interpolate each profile onto a fixed pressure grid
12. Merge all valid profiles into a single cleaned NetCDF dataset
13. Save summary tables and diagnostic information for validation

Processed BGC Variables:
- BBP700
- CDOM
- CHLA
- CHLA_FLUORESCENCE
- DOWNWELLING_PAR
- DOWN_IRRADIANCE380
- DOWN_IRRADIANCE412
- DOWN_IRRADIANCE443
- DOWN_IRRADIANCE490
- DOWN_IRRADIANCE555
- DOXY
- NITRATE
- PH_IN_SITU_TOTAL

Output:
- Filtered BGC-Argo profile index CSV
- Regional Sprof file list CSV
- Zipped raw BGC-Argo Sprof NetCDF files
- Clean BGC variable list CSV
- Cleaned and vertically gridded BGC-Argo NetCDF dataset
- Final variable summary CSV
- Processing and download log files
"""



# 0. MOUNT GOOGLE DRIVE
from google.colab import drive
drive.mount("/content/drive")



# 1. IMPORT LIBRARIES AND USER SETTINGS
import os
import glob
import math
import shutil
import zipfile
import requests
import warnings
import subprocess
import sys
from datetime import datetime, timezone
import numpy as np
import pandas as pd
from tqdm import tqdm
from requests.adapters import HTTPAdapter, Retry

# Install netCDF4 if needed
try:
    from netCDF4 import Dataset, num2date
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "netCDF4"])
    from netCDF4 import Dataset, num2date



# Study area

LAT_MIN, LAT_MAX = x, y
LON_MIN, LON_MAX = x, y

# All available time
DATE_START_REQ = "1997-01-01"
DATE_END_REQ = datetime.now(timezone.utc).strftime("%Y-%m-%d")



# Google Drive folders
BASE_OUT = "/content/drive/MyDrive/YourPath"

BGC_BASE = os.path.join(BASE_OUT, "BGC_Argo_All_Variables")
INDEX_DIR = os.path.join(BGC_BASE, "bgc_index")
ZIP_DIR = os.path.join(BGC_BASE, "bgc_sprof_zip_batches")
OUT_DIR = os.path.join(BGC_BASE, "processed_output")

TMP_DOWNLOAD = "/content/bgc_argo_tmp_download"
TMP_EXTRACT = "/content/bgc_argo_extract_tmp"
TMP_AUDIT = "/content/bgc_argo_audit_tmp"

for d in [BGC_BASE, INDEX_DIR, ZIP_DIR, OUT_DIR, TMP_DOWNLOAD, TMP_EXTRACT, TMP_AUDIT]:
    os.makedirs(d, exist_ok=True)



# Output files
FILTERED_INDEX_CSV = os.path.join(
    OUT_DIR,
    "filtered_bgc_argo_index_all_variables_5_23N_80_100E.csv"
)

FILTERED_SPROF_CSV = os.path.join(
    OUT_DIR,
    "filtered_bgc_argo_sprof_files_all_variables_5_23N_80_100E.csv"
)

DISCOVERED_VARIABLES_CSV = os.path.join(
    OUT_DIR,
    "discovered_bgc_variables_5_23N_80_100E.csv"
)

CLEAN_DISCOVERED_VARIABLES_CSV = os.path.join(
    OUT_DIR,
    "discovered_bgc_variables_clean_5_23N_80_100E.csv"
)

OUT_NC = os.path.join(
    OUT_DIR,
    "BGC_Argo_All_Variables_BoB_5_23N_80_100E_cleaned_gridded.nc"
)

FAILED_DOWNLOAD_LOG = os.path.join(ZIP_DIR, "failed_bgc_sprof_downloads.log")
FAILED_PROCESS_LOG = os.path.join(OUT_DIR, "bgc_all_variable_process_failed.log")
PROGRESS_LOG = os.path.join(OUT_DIR, "bgc_all_variable_processed_zips.txt")

# Helper
def clear_directory(path):
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)

print("BGC-ARGO ALL-VARIABLE SETTINGS")
print(f"Latitude range : {LAT_MIN} to {LAT_MAX}")
print(f"Longitude range: {LON_MIN} to {LON_MAX}")
print(f"Time range     : {DATE_START_REQ} to {DATE_END_REQ}")
print(f"Base folder    : {BGC_BASE}")
print(f"Index folder   : {INDEX_DIR}")
print(f"ZIP folder     : {ZIP_DIR}")
print(f"Output folder  : {OUT_DIR}")




# 2. DOWNLOAD BGC-ARGO SYNTHETIC PROFILE INDEX
INDEX_URLS = [
    "https://data-argo.ifremer.fr/argo_synthetic-profile_index.txt",
    "https://usgodae.org/pub/outgoing/argo/argo_synthetic-profile_index.txt"
]

INDEX_LOCAL = os.path.join(INDEX_DIR, "argo_synthetic-profile_index.txt")


def download_file_with_retry(urls, out_path, min_size_bytes=1_000_000):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if os.path.exists(out_path) and os.path.getsize(out_path) >= min_size_bytes:
        print("Index already exists:")
        print(out_path)
        print("Size:", round(os.path.getsize(out_path) / (1024 * 1024), 2), "MB")
        return out_path

    session = requests.Session()
    retries = Retry(
        total=8,
        backoff_factor=1.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))

    last_error = None

    for url in urls:
        try:
            print("Trying download from:")
            print(url)

            tmp_part = out_path + ".part"

            with session.get(url, stream=True, timeout=240) as r:
                r.raise_for_status()

                with open(tmp_part, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            os.replace(tmp_part, out_path)

            if os.path.getsize(out_path) < min_size_bytes:
                raise RuntimeError("Downloaded file is too small or incomplete.")

            print("Downloaded successfully:")
            print(out_path)
            print("Size:", round(os.path.getsize(out_path) / (1024 * 1024), 2), "MB")
            return out_path

        except Exception as e:
            last_error = e
            print("Download failed from this URL.")
            print(repr(e))

            if os.path.exists(out_path + ".part"):
                os.remove(out_path + ".part")

    raise RuntimeError(f"All index download URLs failed. Last error: {last_error}")

INDEX_LOCAL = download_file_with_retry(INDEX_URLS, INDEX_LOCAL)
if not os.path.exists(INDEX_LOCAL):
    raise FileNotFoundError(f"Index file missing: {INDEX_LOCAL}")

print("Final index file path:")
print(INDEX_LOCAL)




# 3. READ AND CLEAN BGC-ARGO INDEX
expected_cols = [
    "file",
    "date",
    "latitude",
    "longitude",
    "ocean",
    "profiler_type",
    "institution",
    "parameters",
    "parameter_data_mode",
    "date_update"
]

try:
    bgc = pd.read_csv(INDEX_LOCAL, comment="#", header=0, low_memory=False)
    bgc.columns = [str(c).strip().lower() for c in bgc.columns]

    if "file" not in bgc.columns or "parameters" not in bgc.columns:
        raise ValueError("Header not detected correctly.")

except Exception:
    bgc = pd.read_csv(
        INDEX_LOCAL,
        comment="#",
        header=None,
        names=expected_cols,
        usecols=list(range(10)),
        low_memory=False
    )

bgc = bgc[[
    "file",
    "date",
    "latitude",
    "longitude",
    "parameters",
    "parameter_data_mode",
    "date_update"
]].copy()

# Argo date format = YYYYMMDDHHMMSS
date_str = (
    bgc["date"]
    .astype(str)
    .str.strip()
    .str.replace(r"\.0$", "", regex=True)
    .str.zfill(14)
    .str[:14]
)

bgc["date"] = pd.to_datetime(
    date_str,
    format="%Y%m%d%H%M%S",
    errors="coerce"
)

bgc["latitude"] = pd.to_numeric(bgc["latitude"], errors="coerce")
bgc["longitude"] = pd.to_numeric(bgc["longitude"], errors="coerce")
bgc["file"] = bgc["file"].astype(str).str.strip()
bgc["parameters"] = bgc["parameters"].astype(str).str.strip()
bgc["parameter_data_mode"] = bgc["parameter_data_mode"].astype(str).str.strip()

bgc = bgc.dropna(subset=["file", "date", "latitude", "longitude"])

# Keep NetCDF synthetic profile rows only
bgc = bgc[bgc["file"].str.endswith(".nc", na=False)].reset_index(drop=True)


print("BGC INDEX READ SUCCESSFULLY")
print("Total BGC index rows:", len(bgc))
print("Earliest date:", bgc["date"].min())
print("Latest date  :", bgc["date"].max())
print()
print(bgc.head())





# 4. FILTER BGC-ARGO INDEX BY REGION AND TIME ONLY
# No target-variable filtering. This keeps all BGC profiles.
t0_req = pd.to_datetime(DATE_START_REQ)
t1_req = pd.to_datetime(DATE_END_REQ) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

region_mask = (
    (bgc["latitude"] >= LAT_MIN) & (bgc["latitude"] <= LAT_MAX) &
    (bgc["longitude"] >= LON_MIN) & (bgc["longitude"] <= LON_MAX)
)

bgc_region = bgc.loc[region_mask].copy()

print("BGC profiles inside selected region:", len(bgc_region))

if len(bgc_region) == 0:
    raise ValueError("No BGC-Argo profiles found inside selected lat/lon box.")

earliest_region = bgc_region["date"].min()
latest_region = bgc_region["date"].max()

t0 = max(t0_req, earliest_region)
t1 = min(t1_req, latest_region)

print("Earliest available in region:", earliest_region)
print("Latest available in region  :", latest_region)
print("Using final time window     :", t0, "to", t1)

time_mask = (bgc_region["date"] >= t0) & (bgc_region["date"] <= t1)

sub = (
    bgc_region.loc[
        time_mask,
        ["file", "date", "latitude", "longitude", "parameters", "parameter_data_mode"]
    ]
    .sort_values("date")
    .reset_index(drop=True)
)

print("\nFiltered BGC profile index rows:", len(sub))
print(sub.head())

print("\nCHECK RANGES:")
print("Latitude min/max :", sub["latitude"].min(), sub["latitude"].max())
print("Longitude min/max:", sub["longitude"].min(), sub["longitude"].max())
print("Date min/max     :", sub["date"].min(), sub["date"].max())

all_parameter_tokens = sorted(
    set(
        " ".join(sub["parameters"].astype(str).tolist())
        .replace(",", " ")
        .split()
    )
)

print("\nAll parameters listed in selected regional BGC index:")
for p in all_parameter_tokens:
    print("-", p)

sub.to_csv(FILTERED_INDEX_CSV, index=False)

print("\nSaved filtered BGC index:")
print(FILTERED_INDEX_CSV)




# 5. CREATE UNIQUE SPROF FILE LIST
def profile_to_sprof_path(profile_path):
    """
    Convert synthetic profile path:
    dac/wmo/profiles/SDwmo_cycle.nc

    to merged Sprof path:
    dac/wmo/wmo_Sprof.nc
    """
    parts = str(profile_path).split("/")

    if len(parts) < 3:
        return None

    dac = parts[0]
    wmo = parts[1]

    return f"{dac}/{wmo}/{wmo}_Sprof.nc"


sub["sprof_file"] = sub["file"].apply(profile_to_sprof_path)
sub = sub.dropna(subset=["sprof_file"])

sprof_df = (
    sub[["sprof_file"]]
    .drop_duplicates()
    .sort_values("sprof_file")
    .reset_index(drop=True)
)

sprof_df.to_csv(FILTERED_SPROF_CSV, index=False)

print("Unique Sprof files to download:", len(sprof_df))
print(sprof_df.head())
print("Saved Sprof list:", FILTERED_SPROF_CSV)

if len(sprof_df) == 0:
    raise ValueError("No Sprof files created from index. Check file path format in index.")




# 6. DOWNLOAD ALL REGIONAL BGC SPROF FILES IN ZIP BATCHES
BASE_URLS = [
    "https://data-argo.ifremer.fr/dac/",
    "https://usgodae.org/pub/outgoing/argo/dac/"
]

sprof_paths = sprof_df["sprof_file"].tolist()

print("Total Sprof files to download:", len(sprof_paths))

BATCH = 250
num_batches = math.ceil(len(sprof_paths) / BATCH)

print("Total batches:", num_batches)
print("Batch size   :", BATCH)

session = requests.Session()
retries = Retry(
    total=8,
    backoff_factor=0.8,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
session.mount("https://", HTTPAdapter(max_retries=retries))
session.mount("http://", HTTPAdapter(max_retries=retries))


def download_sprof_to_tmp(rel_path):
    out_path = os.path.join(TMP_DOWNLOAD, rel_path)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return out_path

    last_error = None

    for base_url in BASE_URLS:
        url = base_url + rel_path
        tmp_part = out_path + ".part"

        try:
            with session.get(url, stream=True, timeout=240) as r:
                r.raise_for_status()

                with open(tmp_part, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            os.replace(tmp_part, out_path)

            if os.path.getsize(out_path) <= 0:
                raise RuntimeError("Downloaded file is empty.")

            return out_path

        except Exception as e:
            last_error = e

            if os.path.exists(tmp_part):
                os.remove(tmp_part)

    raise RuntimeError(f"Failed to download {rel_path}. Last error: {last_error}")


for bi in range(num_batches):
    start = bi * BATCH
    end = min((bi + 1) * BATCH, len(sprof_paths))
    batch_paths = sprof_paths[start:end]

    zip_name = os.path.join(
        ZIP_DIR,
        f"bgc_sprof_allvars_{start + 1:06d}_{end:06d}.zip"
    )

    if os.path.exists(zip_name) and os.path.getsize(zip_name) > 0:
        print("Skipping existing ZIP:", os.path.basename(zip_name))
        continue

    clear_directory(TMP_DOWNLOAD)

    failed = 0

    for rel_path in tqdm(batch_paths, desc=f"Downloading BGC batch {bi + 1}/{num_batches}"):
        try:
            download_sprof_to_tmp(rel_path)

        except Exception as e:
            failed += 1

            with open(FAILED_DOWNLOAD_LOG, "a") as log:
                log.write(f"{rel_path}\t{repr(e)}\n")

    nc_count = 0

    with zipfile.ZipFile(zip_name, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for root, dirs, files in os.walk(TMP_DOWNLOAD):
            for fname in files:
                if fname.lower().endswith(".nc"):
                    fpath = os.path.join(root, fname)
                    arcname = os.path.relpath(fpath, TMP_DOWNLOAD).replace(os.sep, "/")
                    z.write(fpath, arcname=arcname)
                    nc_count += 1

    print(
        f"Saved: {os.path.basename(zip_name)} | "
        f"Sprof files={nc_count} | failed={failed}"
    )

print("\nDONE downloading all regional BGC Sprof files.")
print("ZIP folder:", ZIP_DIR)
print("Failed download log:", FAILED_DOWNLOAD_LOG)




# 7. ZIP INVENTORY CHECK
zip_files = sorted(glob.glob(os.path.join(ZIP_DIR, "*.zip")))

print("Total BGC ZIP files found:", len(zip_files))

if len(zip_files) == 0:
    raise ValueError("No BGC ZIP files found. Run download step first.")

rows = []

for zpath in zip_files:
    zsize_mb = os.path.getsize(zpath) / (1024 * 1024)

    with zipfile.ZipFile(zpath, "r") as z:
        names = [n for n in z.namelist() if n.lower().endswith(".nc")]

        rows.append({
            "zip_name": os.path.basename(zpath),
            "zip_size_mb": round(zsize_mb, 3),
            "sprof_count": len(names),
            "first_sprof": names[0] if names else ""
        })

inv = pd.DataFrame(rows)

inv_path = os.path.join(OUT_DIR, "bgc_sprof_zip_inventory_all_variables.csv")
inv.to_csv(inv_path, index=False)

print("Saved inventory:", inv_path)
print(inv.head(10))
print("Total Sprof files inside ZIPs:", inv["sprof_count"].sum())




# 8. DEFINE PRESSURE GRID
P_MIN, P_MAX, P_STEP = 0.0, 2000.0, 10.0
PGRID = np.arange(P_MIN, P_MAX + P_STEP, P_STEP).astype("f4")
NLEV = len(PGRID)

print("Pressure grid levels:", NLEV)
print("Pressure range:", PGRID[0], "to", PGRID[-1], "dbar")




# 9. DISCOVER AND CLEAN ALL BGC VARIABLES FROM SPROF FILES
zip_files = sorted(glob.glob(os.path.join(ZIP_DIR, "*.zip")))

EXCLUDE_BASE_VARIABLES = {
    "PRES",
    "TEMP",
    "PSAL",
    "JULD",
    "LATITUDE",
    "LONGITUDE",
    "CYCLE_NUMBER",
    "DIRECTION",
    "POSITION_QC",
    "CONFIG_MISSION_NUMBER",
    "VERTICAL_SAMPLING_SCHEME",
    "PLATFORM_NUMBER",
    "PROJECT_NAME",
    "PI_NAME",
    "DATA_CENTRE",
    "DC_REFERENCE",
    "DATA_STATE_INDICATOR",
    "DATA_MODE",
    "PARAMETER",
    "SCIENTIFIC_CALIB_EQUATION",
    "SCIENTIFIC_CALIB_COEFFICIENT",
    "SCIENTIFIC_CALIB_COMMENT",
    "SCIENTIFIC_CALIB_DATE",
    "HISTORY_INSTITUTION",
    "HISTORY_STEP",
    "HISTORY_SOFTWARE",
    "HISTORY_SOFTWARE_RELEASE",
    "HISTORY_REFERENCE",
    "HISTORY_DATE",
    "HISTORY_ACTION",
    "HISTORY_PARAMETER",
    "HISTORY_START_PRES",
    "HISTORY_STOP_PRES",
    "HISTORY_PREVIOUS_VALUE",
    "HISTORY_QCTEST"
}

SUFFIXES_TO_REMOVE = [
    "_ADJUSTED_ERROR",
    "_ADJUSTED_QC",
    "_ADJUSTED",
    "_DATA_MODE",
    "_QC",
    "_ERROR"
]


def base_variable_name(varname):
    base = str(varname)

    for suffix in SUFFIXES_TO_REMOVE:
        if base.endswith(suffix):
            base = base[: -len(suffix)]

    return base


def is_profile_level_variable(ds, varname):
    if varname not in ds.variables:
        return False

    dims = ds.variables[varname].dimensions

    return ("N_PROF" in dims) and ("N_LEVELS" in dims)


def is_candidate_bgc_variable(ds, varname):
    if not is_profile_level_variable(ds, varname):
        return False

    base = base_variable_name(varname)

    # Remove physical/metadata variables
    if base in EXCLUDE_BASE_VARIABLES:
        return False

    # Remove pressure displacement variables
    if base.endswith("_dPRES"):
        return False

    # Remove QC/error/data-mode variables
    if varname.endswith("_QC"):
        return False

    if varname.endswith("_ADJUSTED_QC"):
        return False

    if varname.endswith("_ADJUSTED_ERROR"):
        return False

    if varname.endswith("_ERROR"):
        return False

    if varname.endswith("_DATA_MODE"):
        return False

    return True


discovered = {}
files_scanned = 0

for zpath in tqdm(zip_files, desc="Scanning Sprof files for BGC variables"):
    clear_directory(TMP_AUDIT)

    with zipfile.ZipFile(zpath, "r") as z:
        nc_names = [n for n in z.namelist() if n.lower().endswith(".nc")]
        z.extractall(TMP_AUDIT, members=nc_names)

    nc_files = sorted(glob.glob(os.path.join(TMP_AUDIT, "**", "*.nc"), recursive=True))

    for fpath in nc_files:
        files_scanned += 1

        try:
            ds = Dataset(fpath, "r")

            for varname in ds.variables.keys():
                if is_candidate_bgc_variable(ds, varname):
                    base = base_variable_name(varname)

                    if base not in discovered:
                        discovered[base] = {
                            "base_variable": base,
                            "example_variable": varname,
                            "has_raw": False,
                            "has_adjusted": False,
                            "has_qc": False,
                            "has_adjusted_qc": False,
                            "units": "",
                            "long_name": "",
                            "example_file": os.path.basename(fpath)
                        }

                    if varname == base:
                        discovered[base]["has_raw"] = True

                    if varname == f"{base}_ADJUSTED":
                        discovered[base]["has_adjusted"] = True

                    if f"{base}_QC" in ds.variables:
                        discovered[base]["has_qc"] = True

                    if f"{base}_ADJUSTED_QC" in ds.variables:
                        discovered[base]["has_adjusted_qc"] = True

                    units = getattr(ds.variables[varname], "units", "")
                    long_name = getattr(ds.variables[varname], "long_name", "")

                    if units and not discovered[base]["units"]:
                        discovered[base]["units"] = units

                    if long_name and not discovered[base]["long_name"]:
                        discovered[base]["long_name"] = long_name

            ds.close()

        except Exception as e:
            with open(FAILED_PROCESS_LOG, "a") as f:
                f.write(f"Variable discovery failed: {fpath} | {repr(e)}\n")

discovered_df = pd.DataFrame(list(discovered.values()))

if len(discovered_df) == 0:
    raise ValueError("No BGC profile-level variables detected in downloaded Sprof files.")

discovered_df = discovered_df.sort_values("base_variable").reset_index(drop=True)
discovered_df.to_csv(DISCOVERED_VARIABLES_CSV, index=False)

# Extra safety cleaning
discovered_df = discovered_df[
    ~discovered_df["base_variable"].astype(str).str.endswith("_dPRES", na=False)
].copy()

discovered_df = discovered_df[
    ~discovered_df["base_variable"].isin(["TEMP_dPRES", "PSAL_dPRES", "PRES_dPRES"])
].copy()

discovered_df = discovered_df.sort_values("base_variable").reset_index(drop=True)

BGC_VARIABLES = discovered_df["base_variable"].astype(str).tolist()

discovered_df.to_csv(CLEAN_DISCOVERED_VARIABLES_CSV, index=False)


print("CLEAN BGC VARIABLE DISCOVERY COMPLETE")

print("Sprof files scanned:", files_scanned)
print("Number of clean BGC variables detected:", len(BGC_VARIABLES))
print()
for v in BGC_VARIABLES:
    print("-", v)
print()
print("Saved clean variable list:")
print(CLEAN_DISCOVERED_VARIABLES_CSV)




# 10. CREATE FINAL ALL-VARIABLE BGC NETCDF FILE
RESET_PROCESSING = True

if RESET_PROCESSING:
    for f in [OUT_NC, FAILED_PROCESS_LOG, PROGRESS_LOG]:
        if os.path.exists(f):
            os.remove(f)

nc = Dataset(OUT_NC, "w", format="NETCDF4")

nc.createDimension("N_PROF", None)
nc.createDimension("N_LEVELS", NLEV)

nc.title = "Cleaned and Gridded All-Variable BGC-Argo Profiles"
nc.study_area = "Bay of Bengal / Northern Indian Ocean"
nc.latitude_range = f"{LAT_MIN} to {LAT_MAX}"
nc.longitude_range = f"{LON_MIN} to {LON_MAX}"
nc.source = "BGC-Argo GDAC Sprof NetCDF files"
nc.processing = (
    "Automatic BGC variable discovery, _dPRES removal, adjusted/raw variable selection, "
    "relaxed BGC QC filtering using flags 1, 2, 3, 8, and vertical interpolation"
)
nc.bgc_qc_flags_used = "1, 2, 3, 8"
nc.pressure_qc_flags_used = "1, 2"
nc.history = f"Created on {datetime.now(timezone.utc).isoformat()}"

v = nc.createVariable("PRES_GRID", "f4", ("N_LEVELS",))
v[:] = PGRID
v.units = "dbar"
v.long_name = "Fixed pressure grid"

v = nc.createVariable("JULD", "f8", ("N_PROF",))
v.units = "days since 1950-01-01 00:00:00 UTC"
v.long_name = "Julian day"

v = nc.createVariable("LATITUDE", "f4", ("N_PROF",))
v.units = "degree_north"

v = nc.createVariable("LONGITUDE", "f4", ("N_PROF",))
v.units = "degree_east"

v = nc.createVariable("CYCLE_NUMBER", "i4", ("N_PROF",))
v.long_name = "Argo float cycle number"

nc.createVariable("PLATFORM_NUMBER", str, ("N_PROF",))
nc.createVariable("SOURCE_FILE", str, ("N_PROF",))

fill = np.float32(np.nan)

for _, row in discovered_df.iterrows():
    varname = str(row["base_variable"])

    v = nc.createVariable(
        varname,
        "f4",
        ("N_PROF", "N_LEVELS"),
        zlib=True,
        complevel=4,
        fill_value=fill
    )

    v.units = str(row.get("units", ""))
    v.long_name = str(row.get("long_name", varname))
    v.source_variable_example = str(row.get("example_variable", ""))
    v.has_raw = str(row.get("has_raw", ""))
    v.has_adjusted = str(row.get("has_adjusted", ""))
    v.has_qc = str(row.get("has_qc", ""))
    v.has_adjusted_qc = str(row.get("has_adjusted_qc", ""))

nc.close()

print("✅ Final all-variable BGC NetCDF created:")
print(OUT_NC)




# 11. PROCESSING FUNCTIONS
# Corrected relaxed BGC QC:
# This keeps CDOM, because diagnostic showed CDOM was removed by stricter QC.
GOOD_QC_BGC = {"1", "2", "3", "8"}

# Pressure should remain stricter
GOOD_QC_PRES = {"1", "2"}

USE_RAW_IF_NO_ADJUSTED = True


def as_normal_array(x):
    if np.ma.isMaskedArray(x):
        if x.dtype.kind in ["f", "i", "u"]:
            return x.filled(np.nan)
        else:
            return x.filled("")
    return np.array(x)


def get_var(ds, name, i=0):
    if name not in ds.variables:
        return None

    var = ds.variables[name]
    data = var[:]
    data = as_normal_array(data)

    if "N_PROF" in var.dimensions:
        axis = var.dimensions.index("N_PROF")
        data = np.take(data, i, axis=axis)

    return np.squeeze(data)


def finite_count(x):
    if x is None:
        return 0

    try:
        arr = np.array(x, dtype=float)
        return int(np.isfinite(arr).sum())
    except Exception:
        return 0


def choose_data_and_qc(ds, base_name, i=0):
    """
    Prefer adjusted variable.
    If adjusted is absent or empty, use raw variable.
    """
    adj_name = f"{base_name}_ADJUSTED"
    raw_name = base_name

    adj_qc_name = f"{base_name}_ADJUSTED_QC"
    raw_qc_name = f"{base_name}_QC"

    if adj_name in ds.variables:
        data = get_var(ds, adj_name, i)

        if finite_count(data) >= 2:
            qc = get_var(ds, adj_qc_name, i) if adj_qc_name in ds.variables else None
            return data, qc, adj_name

    if USE_RAW_IF_NO_ADJUSTED and raw_name in ds.variables:
        data = get_var(ds, raw_name, i)

        if finite_count(data) >= 2:
            qc = get_var(ds, raw_qc_name, i) if raw_qc_name in ds.variables else None
            return data, qc, raw_name

    return None, None, None


def qc_mask(values, qc, good_qc):
    if values is None:
        return None

    v = np.array(values, dtype=float).copy()

    if qc is None:
        return v

    try:
        q = np.array(qc)

        if q.dtype.kind == "S":
            q = np.char.decode(q)

        q = q.astype(str)
        q = np.char.strip(q)
        q = np.array([str(x)[-1] if str(x) != "" else "" for x in q.ravel()])

        if len(q) != len(v):
            return v

        good = np.isin(q, list(good_qc))
        v[~good] = np.nan

    except Exception:
        return v

    return v


def interp_to_grid(pres, var):
    pres = np.array(pres, dtype=float)
    var = np.array(var, dtype=float)

    mask = np.isfinite(pres) & np.isfinite(var)

    if mask.sum() < 2:
        return np.full(NLEV, np.nan, dtype="f4")

    pres = pres[mask]
    var = var[mask]

    idx = np.argsort(pres)
    pres = pres[idx]
    var = var[idx]

    _, unique_idx = np.unique(pres, return_index=True)
    pres = pres[unique_idx]
    var = var[unique_idx]

    if len(pres) < 2:
        return np.full(NLEV, np.nan, dtype="f4")

    out = np.interp(PGRID, pres, var, left=np.nan, right=np.nan)

    return out.astype("f4")


def decode_nc_string(x):
    if x is None:
        return ""

    arr = np.array(x)

    try:
        if arr.dtype.kind == "S":
            return b"".join(arr.ravel()).decode(errors="ignore").strip()

        if arr.dtype.kind == "U":
            return "".join(arr.ravel()).strip()

        if arr.size == 1:
            return str(arr.item()).strip()

        return "".join(arr.astype(str).ravel()).strip()

    except Exception:
        return str(x).strip()


def scalar_float(x, default=np.nan):
    if x is None:
        return default

    try:
        arr = np.array(x).squeeze()

        if arr.size == 0:
            return default

        val = float(arr.flat[0])

        if not np.isfinite(val):
            return default

        return val

    except Exception:
        return default


def scalar_int(x, default=-1):
    val = scalar_float(x, np.nan)

    if not np.isfinite(val):
        return default

    return int(val)


def juld_to_datetime(ds, juld_value):
    if not np.isfinite(juld_value):
        return None

    try:
        var = ds.variables["JULD"]
        units = getattr(var, "units", "days since 1950-01-01 00:00:00 UTC")
        calendar = getattr(var, "calendar", "gregorian")

        dt = num2date(
            juld_value,
            units=units,
            calendar=calendar,
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=True
        )

        dt = pd.Timestamp(dt)

        if dt.tzinfo is not None:
            dt = dt.tz_convert(None)

        return dt

    except Exception:
        try:
            return pd.Timestamp("1950-01-01") + pd.to_timedelta(juld_value, unit="D")
        except Exception:
            return None


def profile_inside_region_time(lat, lon, dt):
    if not np.isfinite(lat) or not np.isfinite(lon):
        return False

    if not (LAT_MIN <= lat <= LAT_MAX):
        return False

    if not (LON_MIN <= lon <= LON_MAX):
        return False

    if dt is not None:
        if not (t0 <= dt <= t1):
            return False

    return True


print("✅ Processing functions ready.")




# 12. PROCESS ALL CLEAN BGC VARIABLES
zip_files = sorted(glob.glob(os.path.join(ZIP_DIR, "*.zip")))

if len(zip_files) == 0:
    raise ValueError("No BGC ZIP files found. Run the download step first.")

done = set()

if os.path.exists(PROGRESS_LOG):
    with open(PROGRESS_LOG, "r") as f:
        done = set(x.strip() for x in f if x.strip())

out = Dataset(OUT_NC, "a")

total_profiles_read = 0
valid_profiles_saved = 0
skipped_outside_region = 0
skipped_no_pressure = 0
skipped_empty_after_qc = 0

valid_counts_by_variable = {v: 0 for v in BGC_VARIABLES}

for zpath in tqdm(zip_files, desc="Processing clean BGC variables"):
    zip_base = os.path.basename(zpath)

    if zip_base in done:
        continue

    clear_directory(TMP_EXTRACT)

    try:
        with zipfile.ZipFile(zpath, "r") as z:
            z.extractall(TMP_EXTRACT)

    except Exception as e:
        with open(FAILED_PROCESS_LOG, "a") as f:
            f.write(f"{zip_base} | ZIP extraction error | {repr(e)}\n")
        continue

    nc_files = sorted(
        glob.glob(os.path.join(TMP_EXTRACT, "**", "*.nc"), recursive=True)
    )

    for fpath in nc_files:
        source_rel = os.path.relpath(fpath, TMP_EXTRACT).replace(os.sep, "/")

        try:
            ds = Dataset(fpath, "r")

            nprof = ds.dimensions["N_PROF"].size if "N_PROF" in ds.dimensions else 1

            for i in range(nprof):
                total_profiles_read += 1

                lat = scalar_float(get_var(ds, "LATITUDE", i))
                lon = scalar_float(get_var(ds, "LONGITUDE", i))
                juld = scalar_float(get_var(ds, "JULD", i))
                dt = juld_to_datetime(ds, juld)

                if not profile_inside_region_time(lat, lon, dt):
                    skipped_outside_region += 1
                    continue

                pres, pres_qc, pres_source = choose_data_and_qc(ds, "PRES", i)

                if pres is None:
                    skipped_no_pressure += 1
                    continue

                pres = qc_mask(pres, pres_qc, GOOD_QC_PRES)

                gridded_data = {}
                has_any_valid_bgc = False

                for varname in BGC_VARIABLES:
                    data, qc, source_name = choose_data_and_qc(ds, varname, i)

                    if data is None:
                        gridded_data[varname] = np.full(NLEV, np.nan, dtype="f4")
                        continue

                    # Corrected relaxed BGC QC
                    data = qc_mask(data, qc, GOOD_QC_BGC)

                    data_g = interp_to_grid(pres, data)

                    gridded_data[varname] = data_g

                    if np.isfinite(data_g).sum() > 0:
                        has_any_valid_bgc = True
                        valid_counts_by_variable[varname] += 1

                if not has_any_valid_bgc:
                    skipped_empty_after_qc += 1
                    continue

                idx = out.dimensions["N_PROF"].size

                out["JULD"][idx] = juld
                out["LATITUDE"][idx] = lat
                out["LONGITUDE"][idx] = lon
                out["CYCLE_NUMBER"][idx] = scalar_int(get_var(ds, "CYCLE_NUMBER", i))

                platform = decode_nc_string(get_var(ds, "PLATFORM_NUMBER", i))
                out["PLATFORM_NUMBER"][idx] = platform
                out["SOURCE_FILE"][idx] = source_rel

                for varname in BGC_VARIABLES:
                    out[varname][idx, :] = gridded_data[varname]

                valid_profiles_saved += 1

            ds.close()

        except Exception as e:
            with open(FAILED_PROCESS_LOG, "a") as f:
                f.write(f"{source_rel} | {repr(e)}\n")

    out.sync()

    with open(PROGRESS_LOG, "a") as f:
        f.write(zip_base + "\n")

out.close()


print("BGC PROCESSING DONE SUCCESSFULLY")
print("Total profiles read        :", total_profiles_read)
print("Valid profiles saved       :", valid_profiles_saved)
print("Skipped outside region/time:", skipped_outside_region)
print("Skipped no pressure        :", skipped_no_pressure)
print("Skipped empty after QC     :", skipped_empty_after_qc)
print("Final BGC NetCDF file      :", OUT_NC)
print("Failed process log         :", FAILED_PROCESS_LOG)
print()

print("Valid profile count by BGC variable:")
for varname, count in valid_counts_by_variable.items():
    print(f"{varname:30s}: {count}")




# 13. FINAL ALL-VARIABLE BGC NETCDF CHECK
ds = Dataset(OUT_NC, "r")

nprof = ds.dimensions["N_PROF"].size
nlev = ds.dimensions["N_LEVELS"].size


print("FINAL ALL-VARIABLE BGC-ARGO NETCDF CHECK")

print("Output file:", OUT_NC)
print("N_PROF     :", nprof)
print("N_LEVELS   :", nlev)
print("Pressure   :", ds["PRES_GRID"][0], "to", ds["PRES_GRID"][-1], "dbar")
print()

lat_arr = ds["LATITUDE"][:]
lon_arr = ds["LONGITUDE"][:]

if np.ma.isMaskedArray(lat_arr):
    lat_arr = lat_arr.filled(np.nan)

if np.ma.isMaskedArray(lon_arr):
    lon_arr = lon_arr.filled(np.nan)

print("Latitude range :", np.nanmin(lat_arr), "to", np.nanmax(lat_arr))
print("Longitude range:", np.nanmin(lon_arr), "to", np.nanmax(lon_arr))
print()

summary_rows = []

for varname in BGC_VARIABLES:
    if varname in ds.variables:
        arr = ds[varname][:]

        if np.ma.isMaskedArray(arr):
            arr = arr.filled(np.nan)

        valid_values = int(np.isfinite(arr).sum())
        profiles_with_data = int(np.sum(np.isfinite(arr).sum(axis=1) > 0))

        if valid_values > 0:
            vmin = float(np.nanmin(arr))
            vmax = float(np.nanmax(arr))
        else:
            vmin = np.nan
            vmax = np.nan

        summary_rows.append({
            "variable": varname,
            "shape": str(arr.shape),
            "valid_values": valid_values,
            "profiles_with_data": profiles_with_data,
            "min": vmin,
            "max": vmax
        })

summary_df = pd.DataFrame(summary_rows)

summary_csv = os.path.join(
    OUT_DIR,
    "final_bgc_clean_variable_summary_5_23N_80_100E.csv"
)

summary_df.to_csv(summary_csv, index=False)

print(summary_df)
print()
print("Saved final variable summary:")
print(summary_csv)

ds.close()
print("All-variable BGC-Argo processing complete.")
