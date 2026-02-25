"""
config.py — Central configuration for the IDEAM radar pipeline.

Quick-start: the two sections you'll edit most often are clearly marked below.
All settings can also be overridden at runtime via CLI flags — see pipeline.py.
"""

from datetime import datetime
from pathlib import Path


# ═════════════════════════════════════════════════════════════════════════════
#  ① WHAT TO PROCESS  (edit this regularly)
# ═════════════════════════════════════════════════════════════════════════════

# Sites to process when no --sites flag is given.
# Use the exact folder names that appear in the S3 bucket (case-sensitive).
RADAR_SITES: list[str] = [
    "Bogota",           # .nc.gz  — decompressed to .nc on download
    "Corozal",          # .RAW*
    "San_Andres",       # .RAW*
    "Tablazo",          # .RAW*
    "santa_elena",      # .nc
    "Barrancabermeja",  # .RAW*   — only present on 2025-10-16
]

# Default date range when no --start / --end flags are given.
# To run a single full day use the same date for both, e.g.:
#   START_DATE = datetime(2023, 9, 22)
#   END_DATE   = datetime(2023, 9, 22)
START_DATE = datetime(2023,  9, 22)
END_DATE   = datetime(2023,  9, 22)


# ═════════════════════════════════════════════════════════════════════════════
#  ② SITE FORMAT MAP  (update when you add a new site)
# ═════════════════════════════════════════════════════════════════════════════

# Maps each site name to its file format:
#   "iris"      → Sigmet/IRIS binary files  (.RAWxxxx)
#   "netcdf"    → CfRadial2 NetCDF files    (.nc)
#   "netcdf_gz" → gzip-compressed NetCDF    (.nc.gz) — auto-decompressed on download
#
# Sites not listed here default to "iris".
RADAR_FORMAT: dict[str, str] = {
    "Bogota":           "netcdf_gz",
    "Corozal":          "iris",
    "San_Andres":       "iris",
    "Tablazo":          "iris",
    "santa_elena":      "netcdf",
    "Barrancabermeja":  "iris",
}

# Some sites use a non-standard filename convention that doesn't embed the
# site code + date in the filename prefix.  For those sites we must list at
# the folder level (l2_data/YYYY/MM/DD/site/) rather than using a filename
# prefix query.  Add any site whose filenames don't start with
# {SIT}{YY}{MM}{DD} here.
FOLDER_LEVEL_SITES: set[str] = {
    "Bogota",
    "santa_elena",
}


# ═════════════════════════════════════════════════════════════════════════════
#  S3 source  (rarely changes)
# ═════════════════════════════════════════════════════════════════════════════

S3_BUCKET_NAME = "s3-radaresideam"
S3_PREFIX      = "l2_data"


# ═════════════════════════════════════════════════════════════════════════════
#  Local storage paths
# ═════════════════════════════════════════════════════════════════════════════

RAW_DATA_ROOT  = Path("data/raw")
HDF5_DATA_ROOT = Path("data/odim_hdf5")


# ═════════════════════════════════════════════════════════════════════════════
#  Conversion settings
# ═════════════════════════════════════════════════════════════════════════════

COMPRESSION_LEVEL           = 6
DELETE_RAW_AFTER_CONVERSION = False
ODIM_SOURCE_ORG             = "IDEAM"


# ═════════════════════════════════════════════════════════════════════════════
#  Parallelism & logging
# ═════════════════════════════════════════════════════════════════════════════

PARALLEL_WORKERS = 4
LOG_FILE         = Path("logs/pipeline.log")
