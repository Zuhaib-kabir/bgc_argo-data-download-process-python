# BGC-Argo Data Download and Processing Pipeline

This repository contains a complete Python-based workflow for downloading, filtering, processing, quality-controlling, vertically interpolating, and combining Biogeochemical Argo (BGC-Argo) float profile data into a single cleaned NetCDF dataset.

The workflow is designed for regional oceanographic and biogeochemical analysis over the Bay of Bengal and northern Indian Ocean.

---

## Study Region

The processing domain used in this workflow is:

* **Latitude:** x to y
* **Longitude:** x to y
* **Region:** Edit it
* **Time period:** All available BGC-Argo profiles within the selected region
* on other nc and csv file name i use BoB location edit it on your own location 

---

## Overview

Biogeochemical Argo, commonly known as BGC-Argo, extends the global Argo profiling float program by adding biogeochemical sensors to observe important ocean properties such as dissolved oxygen, nitrate, pH, chlorophyll-a, particle backscatter, colored dissolved organic matter, and downwelling irradiance.

This repository uses the BGC-Argo synthetic profile index and merged Sprof NetCDF files to extract regional BGC-Argo observations, apply quality control, interpolate profile data to a standard pressure grid, and save all valid observations into a single cleaned NetCDF file.

---

## Main Features

* Downloads the BGC-Argo synthetic profile index
* Filters BGC-Argo profiles by latitude, longitude, and time
* Converts synthetic profile paths into merged Sprof file paths
* Downloads regional BGC-Argo Sprof NetCDF files
* Stores downloaded Sprof files as ZIP archives
* Automatically detects available BGC variables
* Removes auxiliary pressure-displacement variables such as `*_dPRES`
* Uses adjusted variables where available
* Falls back to raw variables when adjusted variables are unavailable
* Applies quality-control filtering
* Interpolates profiles to a fixed pressure grid
* Combines all valid profiles into one cleaned NetCDF file
* Generates diagnostic and summary CSV files
* Supports visualization of Argo profile locations and mean vertical profiles

---

## Repository Structure

```text
bgc_argo-data-download-process-python/
│
├── bgc_argo.py
├── README.md
├── LICENSE
├── .gitignore
│
└── outputs/
    ├── filtered_bgc_argo_index_all_variables_5_23N_80_100E.csv
    ├── filtered_bgc_argo_sprof_files_all_variables_5_23N_80_100E.csv
    ├── discovered_bgc_variables_5_23N_80_100E.csv
    ├── discovered_bgc_variables_clean_5_23N_80_100E.csv
    ├── final_bgc_clean_variable_summary_5_23N_80_100E.csv
    └── BGC_Argo_All_Variables_BoB_5_23N_80_100E_cleaned_gridded.nc
```

---

## Processed BGC-Argo Variables

The pipeline automatically detects available BGC variables from downloaded Sprof files. In the current regional processing, the following variables are included:

| Variable             | Description                                        |
| -------------------- | -------------------------------------------------- |
| `BBP700`             | Particle backscattering at 700 nm                  |
| `CDOM`               | Colored dissolved organic matter                   |
| `CHLA`               | Chlorophyll-a                                      |
| `CHLA_FLUORESCENCE`  | Chlorophyll fluorescence                           |
| `DOWNWELLING_PAR`    | Downwelling photosynthetically available radiation |
| `DOWN_IRRADIANCE380` | Downwelling irradiance at 380 nm                   |
| `DOWN_IRRADIANCE412` | Downwelling irradiance at 412 nm                   |
| `DOWN_IRRADIANCE443` | Downwelling irradiance at 443 nm                   |
| `DOWN_IRRADIANCE490` | Downwelling irradiance at 490 nm                   |
| `DOWN_IRRADIANCE555` | Downwelling irradiance at 555 nm                   |
| `DOXY`               | Dissolved oxygen                                   |
| `NITRATE`            | Nitrate                                            |
| `PH_IN_SITU_TOTAL`   | In-situ total scale pH                             |

---

## Processing Workflow

The complete workflow follows these steps:

1. Mount Google Drive in Google Colab
2. Import required Python libraries
3. Define study region, time range, and output directories
4. Download the BGC-Argo synthetic profile index
5. Read and clean the index file
6. Filter BGC-Argo profiles by region and time
7. Convert profile paths into merged Sprof file paths
8. Download regional Sprof NetCDF files
9. Save downloaded Sprof files as ZIP archives
10. Check ZIP inventory
11. Define a common pressure grid
12. Automatically detect available BGC variables
13. Remove auxiliary variables such as `*_dPRES`
14. Create the final cleaned NetCDF file structure
15. Apply quality-control filtering
16. Interpolate each variable to the fixed pressure grid
17. Merge all valid profiles into one NetCDF file
18. Generate summary and diagnostic files
19. Create visualization outputs

---

## Quality-Control Settings

The pipeline applies separate QC criteria for pressure and BGC variables.

### Pressure QC

```python
GOOD_QC_PRES = {"1", "2"}
```

### BGC Variable QC

```python
GOOD_QC_BGC = {"1", "2", "3", "8"}
```

A relaxed BGC QC setting is used because diagnostic testing showed that some variables, especially `CDOM`, may be present in the Sprof files but removed under stricter QC criteria.

---

## Vertical Interpolation

All valid profiles are interpolated onto a standard pressure grid:

```python
P_MIN = 0.0
P_MAX = 2000.0
P_STEP = 10.0
```

The final dataset contains vertical levels from **0 to 2000 dbar** at **10 dbar intervals**.

---

## Main Output Dataset

The main final output is:

```text
BGC_Argo_All_Variables_BoB_5_23N_80_100E_cleaned_gridded.nc
```

This NetCDF file contains:

```text
PRES_GRID
JULD
LATITUDE
LONGITUDE
CYCLE_NUMBER
PLATFORM_NUMBER
SOURCE_FILE
BBP700
CDOM
CHLA
CHLA_FLUORESCENCE
DOWNWELLING_PAR
DOWN_IRRADIANCE380
DOWN_IRRADIANCE412
DOWN_IRRADIANCE443
DOWN_IRRADIANCE490
DOWN_IRRADIANCE555
DOXY
NITRATE
PH_IN_SITU_TOTAL
```

---

## Final NetCDF Structure

```text
Dimensions:
    N_PROF    = number of valid BGC-Argo profiles
    N_LEVELS  = fixed pressure levels

Variables:
    PRES_GRID(N_LEVELS)
    JULD(N_PROF)
    LATITUDE(N_PROF)
    LONGITUDE(N_PROF)
    CYCLE_NUMBER(N_PROF)
    PLATFORM_NUMBER(N_PROF)
    SOURCE_FILE(N_PROF)

    BBP700(N_PROF, N_LEVELS)
    CDOM(N_PROF, N_LEVELS)
    CHLA(N_PROF, N_LEVELS)
    CHLA_FLUORESCENCE(N_PROF, N_LEVELS)
    DOWNWELLING_PAR(N_PROF, N_LEVELS)
    DOWN_IRRADIANCE380(N_PROF, N_LEVELS)
    DOWN_IRRADIANCE412(N_PROF, N_LEVELS)
    DOWN_IRRADIANCE443(N_PROF, N_LEVELS)
    DOWN_IRRADIANCE490(N_PROF, N_LEVELS)
    DOWN_IRRADIANCE555(N_PROF, N_LEVELS)
    DOXY(N_PROF, N_LEVELS)
    NITRATE(N_PROF, N_LEVELS)
    PH_IN_SITU_TOTAL(N_PROF, N_LEVELS)
```

---

## Important CSV Outputs

```text
filtered_bgc_argo_index_all_variables_5_23N_80_100E.csv
filtered_bgc_argo_sprof_files_all_variables_5_23N_80_100E.csv
discovered_bgc_variables_5_23N_80_100E.csv
discovered_bgc_variables_clean_5_23N_80_100E.csv
final_bgc_clean_variable_summary_5_23N_80_100E.csv
```

---

## Diagnostic Outputs

```text
all_bgc_variables_diagnostic_summary.csv
all_bgc_variables_diagnostic_profile_details.csv
bgc_all_variable_process_failed.log
failed_bgc_sprof_downloads.log
```

---

## Visualization Outputs

Additional plotting scripts can generate:

* BGC-Argo float/profile location map
* All-time mean vertical profile plots
* Upper-ocean BGC variable profiles
* Surface profile-location dot maps
* Variable-wise profile summary figures

Example figure outputs:

```text
Fig_BGC_Argo_float_profile_locations_BoB.png
Fig_BGC_Argo_all_time_mean_vertical_profiles_0_2000m.png
Fig_BGC_Argo_all_time_mean_vertical_profiles_0_300m.png
```

---

## Python Requirements

This workflow is designed for Google Colab but can also run in a local Python environment.

Required packages:

```text
numpy
pandas
requests
tqdm
netCDF4
matplotlib
cartopy
```

Optional packages:

```text
xarray
scipy
```

---

## How to Run

1. Open the Python script or notebook in Google Colab.
2. Mount Google Drive.
3. Set the study region and output directory.
4. Run the script from top to bottom.
5. The final cleaned BGC-Argo NetCDF file will be saved in the output folder.

Default output folder:

```text
/content/drive/MyDrive/Abrar_Argo/BGC_Argo_All_Variables/processed_output
```

---

## Data Source

This workflow uses BGC-Argo data from the Argo Global Data Assembly Centers.

Main data sources used in the script:

```text
BGC-Argo official website:
https://biogeochemical-argo.org/

BGC-Argo data access:
https://biogeochemical-argo.org/data-access.php

Argo data FAQ:
https://argo.ucsd.edu/data/data-faq/

Coriolis/Ifremer GDAC:
https://data-argo.ifremer.fr/

Argo DAC directory:
https://data-argo.ifremer.fr/dac/

BGC-Argo synthetic profile index:
https://data-argo.ifremer.fr/argo_synthetic-profile_index.txt

USGODAE GDAC mirror:
https://usgodae.org/pub/outgoing/argo/

GO-BGC data information:
https://www.go-bgc.org/getting-started-with-go-bgc-data
```

---

## Suggested Citation / Acknowledgement

When using Argo or BGC-Argo data, users should acknowledge the Argo Program and the Global Data Assembly Centers.

Suggested acknowledgement:

```text
These data were collected and made freely available by the International Argo Program and the national programs that contribute to it. The Argo Program is part of the Global Ocean Observing System.
```

For BGC-Argo data, also acknowledge the Biogeochemical-Argo program and the Argo Global Data Assembly Centers.

---

## Notes

* This pipeline processes merged BGC-Argo Sprof files.
* Sprof files are useful because they contain synthetic profiles with physical and biogeochemical variables on profile-level dimensions.
* Variables ending with `_dPRES` are removed because they are auxiliary pressure-displacement variables, not primary BGC science variables.
* The final dataset is suitable for regional oceanographic analysis, vertical profile analysis, BGC variability assessment, and scientific visualization.

---

## Author

**Md. Zuhaib Kabir**

---

## Repository Purpose

This repository was developed to provide a reproducible Python workflow for regional BGC-Argo data download, processing, quality control, vertical interpolation, merging, and visualization for oceanographic research.
