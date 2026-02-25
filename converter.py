"""
converter.py — Converts IDEAM Sigmet/IRIS .RAW files to HDF5 ODIM format.

Strategy:
  1. Read the .RAW file sweep-by-sweep using xradar (which wraps wradlib's
     iris reader) — this is consistent with the original IDEAM notebook code.
  2. Write each sweep as a /datasetN group in an ODIM-compliant HDF5 file
     using h5py, following the ODIM_H5 v2.2 specification.
  3. Pack float data into uint16 with per-moment gain/offset to minimise
     file size while preserving full dynamic range.

ODIM quantity mapping (Sigmet → ODIM):
  DB_DBZ / DB_DBZ2  → DBZH
  DB_VEL / DB_VEL2  → VRADH
  DB_WIDTH          → WRADH
  DB_ZDR            → ZDR
  DB_RHOHV          → RHOHV
  DB_PHIDP          → PHIDP
  DB_KDP            → KDP
  DB_SNR            → SNRH
"""

import logging
from pathlib import Path

import h5py
import numpy as np
import xarray as xr
import xradar as xd

from config import (
    HDF5_DATA_ROOT,
    COMPRESSION_LEVEL,
    DELETE_RAW_AFTER_CONVERSION,
    ODIM_SOURCE_ORG,
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  Moment name mapping: xradar / xarray variable names → ODIM quantity strings
# ─────────────────────────────────────────────────────────────────────────────

MOMENT_TO_ODIM = {
    # Reflectivity
    "DBZH":    "DBZH",
    "DBZV":    "DBZV",
    "DBZ":     "DBZH",
    # Radial velocity
    "VRADH":   "VRADH",
    "VRADV":   "VRADV",
    "VEL":     "VRADH",
    # Spectrum width
    "WRADH":   "WRADH",
    "WRADV":   "WRADV",
    "WIDTH":   "WRADH",
    # Polarimetric
    "ZDR":     "ZDR",
    "RHOHV":   "RHOHV",
    "PHIDP":   "PHIDP",
    "KDP":     "KDP",
    # Signal-to-noise
    "SNRH":    "SNRH",
    "SNRV":    "SNRV",
    # Clutter filter power removed
    "CCORH":   "CCORH",
}

# Variables that xradar adds for geometry — not radar moments, skip them
NON_MOMENT_VARS = {
    "x", "y", "z", "azimuth", "elevation", "range",
    "time", "sweep_mode", "sweep_number",
    "prt_mode", "follow_mode", "sweep_fixed_angle",
    "longitude", "latitude", "altitude",
    "crs_wkt", "east_west", "north_south", "height",
}


# ─────────────────────────────────────────────────────────────────────────────
#  Utility helpers
# ─────────────────────────────────────────────────────────────────────────────

def _bytes(s: str) -> bytes:
    """Encode a string as null-terminated bytes for HDF5 scalar attributes."""
    return np.bytes_(s)


def _pack_moment(data: np.ndarray, nodata_val: float = 0.0, undetect_val: float = 1.0):
    """
    Packs a float32/64 array into uint16 using a linear gain/offset transform.

        physical = gain * raw + offset
        raw = round((physical - offset) / gain)

    Returns (packed_uint16, gain, offset, nodata, undetect).
    """
    finite_mask = np.isfinite(data)
    valid = data[finite_mask]

    if valid.size > 0:
        dmin, dmax = float(valid.min()), float(valid.max())
        span = dmax - dmin
        if span == 0.0:
            gain   = 0.01          # degenerate but safe
            offset = dmin - gain
        else:
            # Use 2..65535 for valid data (0=nodata, 1=undetect reserved)
            gain   = span / 65533.0
            offset = dmin - 2.0 * gain
    else:
        gain, offset = 0.5, -32.0

    packed = np.full(data.shape, int(nodata_val), dtype=np.uint16)
    # Mark undetect for zero-or-below signal (below sensitivity threshold)
    packed[~finite_mask] = int(undetect_val)
    # Pack valid values
    packed[finite_mask] = np.clip(
        np.round((valid - offset) / gain), 2, 65535
    ).astype(np.uint16)

    return packed, gain, offset, nodata_val, undetect_val


def _odim_quantity(var_name: str) -> str:
    """Maps an xarray variable name to its ODIM quantity string."""
    return MOMENT_TO_ODIM.get(var_name.upper(), var_name.upper())


# ─────────────────────────────────────────────────────────────────────────────
#  Local HDF5 path builder
# ─────────────────────────────────────────────────────────────────────────────

def local_hdf5_path(raw_path: Path, radar_site: str) -> Path:
    """
    Given a local RAW file path, returns the corresponding HDF5 output path.

    RAW:  data/raw/Guaviare/2022/10/06/GUA221006.RAW1A3B
    HDF5: data/odim_hdf5/Guaviare/2022/10/06/GUA221006.h5
    """
    # Derive YYYY/MM/DD from the raw path structure
    rel   = raw_path.relative_to(raw_path.parents[3])   # site/YYYY/MM/DD/file
    parts = rel.parts                                     # (site, YYYY, MM, DD, file)
    stem  = raw_path.stem                                 # filename without extension
    out_dir = HDF5_DATA_ROOT / radar_site / parts[1] / parts[2] / parts[3]
    return out_dir / (stem + ".h5")


# ─────────────────────────────────────────────────────────────────────────────
#  Core conversion function
# ─────────────────────────────────────────────────────────────────────────────

def convert_raw_to_odim(raw_path: Path, radar_site: str, output_path: Path = None) -> Path | None:
    """
    Converts a single IDEAM .RAW file to HDF5 ODIM format.

    Parameters
    ----------
    raw_path    : Path to the local .RAW file
    radar_site  : Site name (used for directory structure and ODIM source tag)
    output_path : Optional explicit output path; auto-derived if None

    Returns the output Path on success, None on failure.
    """
    if output_path is None:
        output_path = local_hdf5_path(raw_path, radar_site)

    if output_path.exists():
        logger.debug(f"HDF5 already exists, skipping: {output_path.name}")
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # ── Step 1: discover available sweeps via xradar ──────────────────────────
    try:
        sweep_groups = xd.io.backends.iris.IrisBackendEntrypoint()\
            .get_variables(str(raw_path)).get("sweep_group_name", None)
    except Exception:
        sweep_groups = None

    # Fallback: try sweep_0 through sweep_19
    if sweep_groups is None or len(sweep_groups) == 0:
        sweep_groups = [f"sweep_{i}" for i in range(20)]

    # ── Step 2: open each sweep and collect valid datasets ────────────────────
    sweeps: list[xr.Dataset] = []
    for sweep_name in sweep_groups:
        try:
            ds = xr.open_dataset(
                str(raw_path),
                engine="iris",
                group=sweep_name,
                mask_and_scale=True,
            )
            # Attach georeferenced x/y/z coords
            ds = xd.georeference.get_x_y_z(ds)
            sweeps.append(ds)
            logger.debug(f"  Loaded sweep {sweep_name}: elev={ds.sweep_fixed_angle.values:.2f}°")
        except Exception as exc:
            logger.debug(f"  Sweep {sweep_name} not available: {exc}")
            break   # stop at first missing sweep

    if not sweeps:
        logger.error(f"No valid sweeps found in {raw_path.name}; skipping.")
        return None

    # ── Step 3: extract site metadata from the first sweep ───────────────────
    ds0   = sweeps[0]
    lat   = float(ds0.latitude.values)
    lon   = float(ds0.longitude.values)
    alt   = float(ds0.altitude.values)

    # Date/time from the first ray's time coordinate
    t0      = ds0.time.values[0]
    t0_dt   = np.datetime64(t0, "s").astype("datetime64[s]").item()  # Python datetime
    date_str = t0_dt.strftime("%Y%m%d")
    time_str = t0_dt.strftime("%H%M%S")

    # ── Step 4: write ODIM HDF5 ───────────────────────────────────────────────
    try:
        with h5py.File(str(output_path), "w") as f:

            # ── /what ──────────────────────────────────────────────────────
            what = f.create_group("what")
            what.attrs["object"]  = _bytes("PVOL")
            what.attrs["version"] = _bytes("H5rad 2.2")
            what.attrs["date"]    = _bytes(date_str)
            what.attrs["time"]    = _bytes(time_str)
            what.attrs["source"]  = _bytes(
                f"NOD:{radar_site[:3].upper()},ORG:{ODIM_SOURCE_ORG},PLC:{radar_site}"
            )

            # ── /where ────────────────────────────────────────────────────
            where = f.create_group("where")
            where.attrs["lon"]    = lon
            where.attrs["lat"]    = lat
            where.attrs["height"] = alt

            # ── /how ──────────────────────────────────────────────────────
            how = f.create_group("how")
            how.attrs["software"] = _bytes("xradar+h5py")
            how.attrs["system"]   = _bytes("Vaisala-Sigmet-IRIS")

            # ── /datasetN (one per sweep) ─────────────────────────────────
            for sweep_idx, ds in enumerate(sweeps, start=1):
                dset_name = f"dataset{sweep_idx}"
                dset      = f.create_group(dset_name)

                elev     = float(ds.sweep_fixed_angle.values)
                nrays    = ds.dims.get("azimuth", ds.dims.get("time", 1))
                nbins    = ds.dims["range"]
                rstart_m = float(ds.range.values[0])
                rscale_m = float(np.diff(ds.range.values).mean()) if nbins > 1 else 250.0

                # Sweep time bounds
                t_start  = np.datetime64(ds.time.values[0],  "s").astype("datetime64[s]").item()
                t_end    = np.datetime64(ds.time.values[-1], "s").astype("datetime64[s]").item()

                # /datasetN/what
                ds_what = dset.create_group("what")
                ds_what.attrs["product"]   = _bytes("SCAN")
                ds_what.attrs["startdate"] = _bytes(t_start.strftime("%Y%m%d"))
                ds_what.attrs["starttime"] = _bytes(t_start.strftime("%H%M%S"))
                ds_what.attrs["enddate"]   = _bytes(t_end.strftime("%Y%m%d"))
                ds_what.attrs["endtime"]   = _bytes(t_end.strftime("%H%M%S"))

                # /datasetN/where
                ds_where = dset.create_group("where")
                ds_where.attrs["elangle"] = elev
                ds_where.attrs["nbins"]   = nbins
                ds_where.attrs["nrays"]   = nrays
                ds_where.attrs["rstart"]  = rstart_m / 1000.0   # ODIM expects km
                ds_where.attrs["rscale"]  = rscale_m            # ODIM expects m
                ds_where.attrs["a1gate"]  = 0

                # /datasetN/how
                ds_how = dset.create_group("how")
                ds_how.attrs["scan_index"] = sweep_idx
                ds_how.attrs["scan_count"] = len(sweeps)
                ds_how.attrs["elangle"]    = elev

                # /datasetN/dataN (one per moment)
                moment_counter = 1
                for var_name in ds.data_vars:
                    if var_name.upper() in NON_MOMENT_VARS:
                        continue
                    if var_name not in ds:
                        continue

                    da = ds[var_name]
                    if da.ndim != 2:
                        continue  # skip 0-D or 1-D auxiliary variables

                    data_array = da.values.astype(np.float32)
                    odim_qty   = _odim_quantity(var_name)

                    packed, gain, offset, nodata, undetect = _pack_moment(data_array)

                    data_group = dset.create_group(f"data{moment_counter}")
                    data_group.create_dataset(
                        "data",
                        data=packed,
                        compression="gzip",
                        compression_opts=COMPRESSION_LEVEL,
                        chunks=True,
                    )

                    dg_what = data_group.create_group("what")
                    dg_what.attrs["quantity"]  = _bytes(odim_qty)
                    dg_what.attrs["gain"]      = gain
                    dg_what.attrs["offset"]    = offset
                    dg_what.attrs["nodata"]    = nodata
                    dg_what.attrs["undetect"]  = undetect

                    logger.debug(f"    [{dset_name}/data{moment_counter}] {odim_qty}")
                    moment_counter += 1

        logger.info(f"  ✓ Converted → {output_path.name}  ({len(sweeps)} sweeps)")

        # ── Step 5: optionally remove the source RAW ──────────────────────────
        if DELETE_RAW_AFTER_CONVERSION:
            raw_path.unlink()
            logger.info(f"  ✗ Deleted source RAW: {raw_path.name}")

        return output_path

    except Exception as exc:
        logger.error(f"  ✗ Conversion failed for {raw_path.name}: {exc}", exc_info=True)
        # Remove incomplete HDF5 if it was partially written
        if output_path.exists():
            output_path.unlink()
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Batch converter (called by pipeline.py)
# ─────────────────────────────────────────────────────────────────────────────

def convert_files(raw_paths: list[Path], radar_site: str) -> list[Path]:
    """
    Converts a list of RAW files for a given site.
    Returns a list of successfully produced HDF5 paths.
    """
    converted = []
    total = len(raw_paths)
    for idx, raw_path in enumerate(raw_paths, start=1):
        logger.info(f"[{radar_site}] Converting {idx}/{total}: {raw_path.name}")
        result = convert_raw_to_odim(raw_path, radar_site)
        if result:
            converted.append(result)

    logger.info(
        f"[{radar_site}] Conversion complete: "
        f"{len(converted)}/{total} files succeeded."
    )
    return converted
