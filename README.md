# IDEAM Radar Pipeline

End-to-end pipeline that downloads `.RAW` (Sigmet/IRIS) volumetric radar files
from the IDEAM S3 bucket and converts them to **HDF5 ODIM v2.2** format, ready
for processing with Py-ART, wradlib, BALTRAD, or any ODIM-compatible tool.

---

## Project structure

```
ideam_radar_pipeline/
├── config.py          ← All settings: sites, date range, paths, options
├── downloader.py      ← S3 listing + downloading logic
├── converter.py       ← RAW → HDF5 ODIM conversion logic
├── pipeline.py        ← Orchestrator (CLI entry point)
├── requirements.txt
└── README.md

data/
├── raw/               ← Downloaded .RAW files
│   └── {site}/{YYYY}/{MM}/{DD}/
└── odim_hdf5/         ← Converted HDF5 ODIM files
    └── {site}/{YYYY}/{MM}/{DD}/
logs/
└── pipeline.log
```

---

## Installation

```bash
# 1. Create and activate a virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt
```

> **Note:** `xradar` requires Python ≥ 3.9. The IRIS/Sigmet backend used here
> is the same one used in the original IDEAM notebook (`engine="iris"`).

---

## Quick start

### 1. Edit `config.py`

```python
# Sites to download (full names as they appear in S3)
RADAR_SITES = [
    "Guaviare",
    "Barrancabermeja",
    "Bogota",
    # ... add/remove as needed
]

# Date range (hourly resolution)
START_DATE = datetime(2022, 10, 6, 0)
END_DATE   = datetime(2022, 10, 6, 23)
```

### 2. Run the pipeline

```bash
# Download + convert all configured sites for the configured date range
python pipeline.py

# Only download RAW files (no conversion)
python pipeline.py --download-only

# Only convert already-downloaded RAW files
python pipeline.py --convert-only

# Override sites at runtime
python pipeline.py --sites Guaviare Bogota

# Override date range at runtime
python pipeline.py --start 2022-10-06T00 --end 2022-10-06T12

# Auto-discover which sites have data for the start date
python pipeline.py --discover

# Increase parallelism (default: 4 concurrent sites)
python pipeline.py --workers 8

# Verbose / debug logging
python pipeline.py --verbose
```

---

## S3 bucket details

| Property | Value |
|---|---|
| Bucket | `s3-radaresideam` |
| Access | Public (anonymous) — no AWS credentials needed |
| Prefix | `l2_data/{YYYY}/{MM}/{DD}/{SiteName}/` |
| File naming | `{SIT}{YY}{MM}{DD}[{HH}[{MM}]].RAW*` |

The `create_query()` logic from the original IDEAM notebook is preserved
exactly in `downloader.build_s3_prefix()`.

---

## Output: HDF5 ODIM v2.2 structure

Each output `.h5` file follows the EUMETNET OPERA ODIM_H5 v2.2 specification:

```
/what              object=PVOL, version=H5rad 2.2, date, time, source
/where             lon, lat, height
/how               software, system, wavelength, beamwidth
/dataset1/         sweep 1 (lowest elevation)
  /what            product=SCAN, startdate, starttime, enddate, endtime
  /where           elangle, nbins, nrays, rstart(km), rscale(m), a1gate
  /how             scan_index, scan_count
  /data1/          moment (e.g. DBZH)
    data           uint16 array [nrays × nbins], gzip compressed
    /what          quantity, gain, offset, nodata, undetect
  /data2/          next moment ...
/dataset2/         sweep 2 ...
```

### Supported moments

| Sigmet / xradar variable | ODIM quantity |
|---|---|
| DBZH / DBZ | DBZH |
| VRADH / VEL | VRADH |
| WRADH / WIDTH | WRADH |
| ZDR | ZDR |
| RHOHV | RHOHV |
| PHIDP | PHIDP |
| KDP | KDP |
| SNRH | SNRH |

---

## Reading the output

### With Py-ART
```python
import pyart
radar = pyart.io.read_odim_h5("data/odim_hdf5/Guaviare/2022/10/06/GUA221006.h5")
radar.info()
display = pyart.graph.RadarDisplay(radar)
display.plot("DBZH", sweep=0, vmin=-10, vmax=60)
```

### With wradlib
```python
import wradlib as wrl
vol = wrl.io.open_odim_dataset("data/odim_hdf5/Guaviare/2022/10/06/GUA221006.h5")
```

### With xradar
```python
import xradar as xd
dtree = xd.io.open_odim_datatree("data/odim_hdf5/Guaviare/2022/10/06/GUA221006.h5")
```

---

## Known IDEAM-specific considerations

1. **Site names in S3** — Use the exact names as they appear in the bucket
   (capital first letter, e.g. `"Guaviare"` not `"guaviare"`). Run
   `python pipeline.py --discover` to see what's available for a given date.

2. **Partial hours** — Some hours have multiple `.RAW` files (volume scans).
   All files matching the hourly prefix are downloaded.

3. **Southern hemisphere radars** — Latitude is negative; this is handled
   automatically by the xradar/iris reader.

4. **Dual-PRF velocity** — Staggered-PRT velocity fields are read as-is;
   dealiasing must be applied separately (e.g. `pyart.correct.dealias_region_based`).
