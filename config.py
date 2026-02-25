"""
config.py — Central configuration for the IDEAM radar pipeline.
Edit this file to set your date range, sites, and storage paths.
"""

from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────
#  AWS S3 — IDEAM bucket (public, no credentials needed)
# ─────────────────────────────────────────────
S3_BUCKET_NAME = "s3-radaresideam"
S3_BUCKET_URI  = f"s3://{S3_BUCKET_NAME}"
S3_PREFIX      = "l2_data"          # top-level prefix inside the bucket

# ─────────────────────────────────────────────
#  All known IDEAM radar sites
#  Format: full name as it appears in S3 paths
# ─────────────────────────────────────────────
RADAR_SITES = [
    "Guaviare",
    "Barrancabermeja",
    "Bogota",
    "Cali",
    "Medellin",
    "Manizales",
    "Corozal",
    "Mariquita",
    "Mocoa",
    "PuertoCarreño",
    "Riohacha",
    "SantaMarta",
    "Valledupar",
    "Villavicencio",
]

# ─────────────────────────────────────────────
#  Date / time range to download
#  Set START_DATE and END_DATE; the pipeline
#  will iterate every hour between them.
# ─────────────────────────────────────────────
START_DATE = datetime(2022, 10, 6, 0)   # inclusive
END_DATE   = datetime(2022, 10, 6, 23)  # inclusive

# ─────────────────────────────────────────────
#  Local storage roots
#  RAW files  → RAW_DATA_ROOT/{site}/{YYYY}/{MM}/{DD}/
#  HDF5 files → HDF5_DATA_ROOT/{site}/{YYYY}/{MM}/{DD}/
# ─────────────────────────────────────────────
RAW_DATA_ROOT  = Path("data/raw")
HDF5_DATA_ROOT = Path("data/odim_hdf5")

# ─────────────────────────────────────────────
#  Conversion options
# ─────────────────────────────────────────────
DELETE_RAW_AFTER_CONVERSION = False   # True → save disk space, keep only HDF5
COMPRESSION_LEVEL           = 6      # gzip level 1–9 for HDF5 datasets
PARALLEL_WORKERS            = 4      # concurrent download + conversion threads

# ─────────────────────────────────────────────
#  Logging
# ─────────────────────────────────────────────
LOG_FILE = Path("logs/pipeline.log")

# ─────────────────────────────────────────────
#  ODIM metadata overrides (optional)
#  Leave as None to auto-read from RAW headers
# ─────────────────────────────────────────────
ODIM_SOURCE_ORG = "IDEAM"       # fills /what/source ORG field
