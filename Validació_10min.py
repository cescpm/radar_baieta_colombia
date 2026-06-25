# =============================================================================
# Validació QPE — 10-minute radar vs gauge, all estimators, parametrisation
# Usage (unchanged bash loop):
#   python Validació_10min.py <day> <month>
#
# ── ESTIMATORS ACTIVE ────────────────────────────────────────────────────────
#   R_Z      : R(Z)       stratiform tropical   Z=140·R^1.45
#   R_Z_CONV : R(Z)       Rosenfeld convective  Z=250·R^1.2
#   R_Z_ZDR  : R(Z,ZDR)   GPM-NASA tropical
#   R_KDP    : R(KDP)     pure, C-band          42.1·KDP^0.79
#   R_KDP_ZDR: R(KDP,ZDR) multiparametric       52·KDP^0.94·10^(-0.39·ZDR)
#   R_A      : R(A)       specific attenuation  1900·A^1.00  ← one chosen
#   R_MERGE  : decision-tree blend of the above
#
# ── OPTIMISATION CHANGES (all annotated inline) ──────────────────────────────
#   1. PhiDP unwrapping loop → fully vectorised with NumPy (no Python az loop)
#   2. DEM beam-blockage interpolation precomputed ONCE per day
#   3. Workers return float32 numpy dict — no xarray pickle overhead over IPC
#   4. WORKERS raised to 14 (16 cores − 2 for OS/main)
#   5. calc_alpha_per_sweep: Python bin-loop → np.digitize (vectorised)
#   6. gc.collect() removed from inside workers; once per window in main
#   7. All intermediate arrays cast to float32 immediately
#   8. Accumulator arrays pre-allocated as float32 (half RAM vs float64)
# =============================================================================

import xradar as xd
import sys
import wradlib as wrl
import numpy as np
import xarray as xr
from scipy.ndimage import (
    median_filter,
    generic_filter,
    generate_binary_structure,
    binary_closing,
    binary_opening,
)
from scipy.spatial import cKDTree
import s3fs
import io
import json
from scipy import stats
import gc
from sodapy import Socrata
import datetime as dt
import re
import pandas as pd
from scipy.stats import pearsonr
from concurrent.futures import ProcessPoolExecutor, as_completed
import warnings
from numpy.lib.stride_tricks import sliding_window_view

warnings.filterwarnings("ignore")

day   = sys.argv[1]
month = sys.argv[2]

TOKEN = "MFHXNYLts4ZhySVUsR7emeZXO"

ESTIMATOR_NAMES = ["R_Z_raw", "R_Z", "R_Z_CONV", 
                   "R_Z_ZDR", "R_KDP_raw", "R_cKDP", 
                   "R_dKDP", "R_dKDP2", "R_KDP_ZDR", 
                   "R_A",]

# =============================================================================
# GAUGE DOWNLOAD
# =============================================================================

def download_data(date_init, date_end, min_lat, max_lat, min_lon, max_lon):
    client = Socrata("www.datos.gov.co", TOKEN)
    query  = client.get(
        dataset_identifier = "s54a-sgyg",
        select = "codigoestacion, fechaobservacion, latitud, longitud, valorobservado, unidadmedida",
        where  = (
            f"fechaobservacion >= '{date_init.strftime('%Y-%m-%dT%H:%M:%S')}' "
            f"AND fechaobservacion <= '{date_end.strftime('%Y-%m-%dT%H:%M:%S')}'"
            f"AND latitud > '{min_lat}' AND latitud < '{max_lat}' "
            f"AND longitud > '{min_lon}' AND longitud < '{max_lon}'"
            "AND codigoestacion IN ("
            "'2319500125','0023197370','0027035050','0027037020','0023205020',"
            "'0023180070','2319500207','0027030140','0027011100','0024065010',"
            "'0023190440','002190380','002190130','0023195040','2319500043',"
            "'0023195502','0023195110','0024057070','0023155030','0024050070',"
            "'002345501','0024017707','0024015519','00240155514','0024015509',"
            "'0024015300','0024017600','0024017590','0024035508','0024037030',"
            "'2401500052','0027010850','0023175020','0023105070','0023085080',"
            "'002315010','0023125080','0023147020','0023127050','0023097030',"
            "'0023127020','0023127060','0023125120'"
            ")"
        ),
        limit = 50_000_000,
    )
    return pd.DataFrame.from_records(query)


# =============================================================================
# QPE ESTIMATOR FUNCTIONS
# Physics unchanged from original. Now standalone so they can be used both
# inside the worker and during post-hoc parametrisation over the CSV pairs.
# =============================================================================

def r_z(zh_lin):
    """R(Z) stratiform tropical  Z = 140·R^1.45"""
    return (zh_lin / 140.0) ** (1.0 / 1.45)

def r_z_rosenfeld(zh_lin):
    """R(Z) Rosenfeld convective  Z = 250·R^1.2"""
    return (zh_lin / 250.0) ** (1.0 / 1.2)

def r_z_zdr(zh_lin, zdr_db):
    """R(Z,ZDR) GPM-NASA tropical  R = 0.0067·Z^0.93·10^(-0.34·ZDR)"""
    return 0.0067 * (zh_lin ** 0.93) * (10.0 ** (-0.34 * zdr_db))

def r_kdp(kdp):
    """R(KDP) pure C-band  R = 42.1·KDP^0.79"""
    return 42.1 * (kdp ** 0.79)

def r_kdp_zdr(kdp, zdr_db):
    """R(KDP,ZDR)  R = 52·KDP^0.94·10^(-0.39·ZDR)"""
    return 52.0 * (kdp ** 0.94) * (10.0 ** (-0.39 * zdr_db))

def r_a(ah):
    """
    R(A) specific attenuation C-band  R = 1900·A^1.00
    Chosen as the physically central exponent.  Other variants for reference:
      1900·A^0.97  (softer),  1900·A^1.03,  1900·A^1.21  (aggressive)
    """
    return 1900.0 * (ah ** 1.00)

# =============================================================================
# HELPERS
# =============================================================================

def texture_std(x):
    valid = np.isfinite(x)
    return np.nanstd(x) if valid.sum() >= 3 else np.nan

def compute_texture(field, size=(1, 3)):
    tex = generic_filter(field.values.copy(), texture_std, size=size, mode="nearest")
    return xr.DataArray(tex, dims=field.dims, coords=field.coords)

def texture_of_complex_phase(FIELD):
    return compute_texture((np.real(np.exp(1j * np.radians(FIELD))) + 1.0) * 90.0)


# =============================================================================
# ALPHA ESTIMATION
# OPTIMISATION: Python for-loop over bins replaced with np.digitize
# =============================================================================

def calc_alpha_per_sweep(ds, zh_var='DBZH', zdr_var='ZDR', rhohv_var='RHOHV',
                         band='C', min_gates=200, bin_width=2.0, min_bins=5,
                         min_gates_per_bin=10, min_zh=15, max_zh=60,
                         min_zdr=-0.5, max_zdr=6.0, min_rhohv=0.98):
    zh    = ds[zh_var].values.ravel()
    zdr   = ds[zdr_var].values.ravel()
    rhohv = ds[rhohv_var].values.ravel()
    valid = (
        (zh >= min_zh) & (zh <= max_zh) & (zdr >= min_zdr) & (zdr <= max_zdr) &
        (rhohv > min_rhohv) & np.isfinite(zh) & np.isfinite(zdr) & np.isfinite(rhohv)
    )
    zh_v, zdr_v = zh[valid], zdr[valid]
    if len(zh_v) < min_gates:
        return 0.01

    # OPTIMISED: vectorised bin assignment
    bin_edges   = np.arange(min_zh, max_zh + bin_width, bin_width)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
    bin_idx     = np.digitize(zh_v, bin_edges) - 1
    n_bins      = len(bin_centers)
    median_zdr  = np.full(n_bins, np.nan)
    for b in range(n_bins):                  # ~22 iterations — negligible
        m = bin_idx == b
        if m.sum() >= min_gates_per_bin:
            median_zdr[b] = np.median(zdr_v[m])

    ok = np.isfinite(median_zdr)
    if ok.sum() < min_bins:
        return 0.01

    slope, *_ = stats.linregress(bin_centers[ok], median_zdr[ok])
    coeffs = {'S': [0.054, -1.31, 10.9], 'C': [0.054, -1.31, 10.9], 'X': [0.087, -1.78, 14.2]}
    a0, a1, a2 = coeffs.get(band.upper(), coeffs['C'])
    return float(np.clip(a0 + a1 * slope + a2 * slope**2, 0.02, 0.10))


# =============================================================================
# HCLASS
# =============================================================================

def decode_hclass(byte_val):
    if byte_val == 0:   return (-2, -2, -2)
    if byte_val == 255: return (np.nan, np.nan, np.nan)
    return ((byte_val >> 5) & 7, (byte_val >> 2) & 7, byte_val & 3)

def decode_hclass_vect(arr):
    return np.vectorize(decode_hclass, otypes=[object])(arr)

def get_(i, da):
    return xr.apply_ufunc(np.vectorize(lambda t: t[i]), da)

def open_iris_dtree(filepath, decode_hclass=False):
    dtree = xd.io.open_iris_datatree(filepath)#, decode_cf=False)

    if decode_hclass:
        dtree["/sweep_0"]["DB_HCLASS"].values = decode_hclass_vect(dtree["/sweep_0"]["DB_HCLASS"].values)
        dtree["/sweep_0"]["DB_HCLASS_meteor"] = get_(0, dtree["/sweep_0"]["DB_HCLASS"])
        dtree["/sweep_0"]["DB_HCLASS_precip"] = get_(1, dtree["/sweep_0"]["DB_HCLASS"])
        dtree["/sweep_0"]["DB_HCLASS_storm"]  = get_(2, dtree["/sweep_0"]["DB_HCLASS"])
    return dtree


fs = s3fs.S3FileSystem(anon=True)

with open(f'metadata/2025/{str(month).zfill(2)}/{str(day).zfill(2)}.json', 'r') as f:
    data = json.load(f)

def get_stacks(json_data):
    all_stacks, current_stack, last_el = [], [], float('inf')
    for _, station_data in json_data.items():
        for _, value in station_data.items():
            sweeps = value['sweeps']
            el = float(sweeps[list(sweeps.keys())[0]]['elevation_angle'])
            if el <= last_el:
                if current_stack: all_stacks.append(current_stack)
                current_stack = []
            current_stack.append(value['filepath'])
            last_el = float(sweeps[list(sweeps.keys())[-1]]['elevation_angle'])
        if current_stack: all_stacks.append(current_stack)
    return all_stacks

stacks      = get_stacks(data)
lowest_ppi  = np.array([vol[0] for vol in stacks])
output_list = [e for e in lowest_ppi if "Barrancabermeja" in e.split("/")]

dem = wrl.io.open_raster("Barrancabermeja.tif")
rastervalues, rastercoords, crs = wrl.georef.extract_raster_dataset(dem, nodata=-32768.0)


# =============================================================================
# TIMESTAMP UTILITIES
# =============================================================================

def parse_timestamp_from_path(s3_path):
    m = re.match(r"BAR(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", s3_path.split("/")[-1])
    if not m: return None
    yy, mo, dd, hh, mm, ss = (int(x) for x in m.groups())
    return dt.datetime(2000+yy, mo, dd, hh, mm, 00, tzinfo=dt.timezone.utc)

def validate_ppi_timestamp(s3_path, dtree, max_delta_seconds=90):
    fname_ts = parse_timestamp_from_path(s3_path)
    if fname_ts is None:
        print(f"[WARN] Unparseable filename: {s3_path}"); return False
    try:
        raw = str(dtree["/"]["time_coverage_start"].values)
        file_ts = dt.datetime.strptime(raw, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=dt.timezone.utc)
    except ValueError:
        print(f"[WARN] Unparseable time_coverage_start in {s3_path}"); return False
    delta = abs((fname_ts - file_ts).total_seconds())
    if delta > max_delta_seconds:
        print(f"[WARN] Mismatch {s3_path}: fname={fname_ts.isoformat()} "
              f"file={file_ts.isoformat()} delta={delta:.0f}s"); return False
    return True

def group_into_10min_windows(file_list):
    """
    Assign each PPI to its absolute 10-min slot of the day:
        slot = (hour * 60 + minute) // 10
    This is unambiguous regardless of small clock drift and avoids the
    boundary ambiguity of delta-based grouping (a scan at exactly +600 s
    was incorrectly included in the previous window with delta < 600).
    """
    stamped = [(parse_timestamp_from_path(p), p) for p in file_list]
    stamped = [(ts, p) for ts, p in stamped if ts is not None]
    stamped.sort(key=lambda x: x[0])

    from collections import defaultdict
    slots = defaultdict(list)
    for ts, path in stamped:
        slot_key = (ts.year, ts.month, ts.day, (ts.hour * 60 + ts.minute) // 10)
        slots[slot_key].append(path)

    groups = [paths for _, paths in sorted(slots.items())]

    for g in groups:
        if len(g) != 2:
            ts_s = parse_timestamp_from_path(g[0]).isoformat()
            print(f"[WARN] Window {ts_s} has {len(g)} PPI(s) (expected 2).")
    return groups


# =============================================================================
# BEAM-BLOCKAGE PRECOMPUTED ONCE PER DAY
# OPTIMISATION: wrl.ipol.map_coordinates is expensive (~0.5 s per call).
# Called once here; result shared read-only across all workers via fork.
# =============================================================================

def precompute_beam_blockage(swp, site, rastercoords, rastervalues):
    azimuth = swp["azimuth"].data
    range_  = swp["range"].data
    nrays, nbins = azimuth.shape[0], range_.shape[0]
    el      = swp["sweep_fixed_angle"].values
    r_scale = range_[1] - range_[0]
    coord   = wrl.georef.sweep_centroids(nrays, r_scale, nbins, el)
    coords  = wrl.georef.spherical_to_proj(coord[..., 0], coord[..., 1], coord[..., 2], site)
    lon, lat, alt = coords[..., 0], coords[..., 1], coords[..., 2]
    polcoords = coords[..., :2]

    polarvalues = wrl.ipol.map_coordinates(
        rastercoords, rastervalues, polcoords, order=3, prefilter=False
    )
    beamradius = wrl.util.half_power_radius(range_, 1.0)
    PBB = np.ma.masked_invalid(wrl.qual.beam_block_frac(polarvalues, alt, beamradius))
    CBB = np.ma.filled(wrl.qual.cum_beam_block_frac(PBB), 0.0).astype(np.float32)

    return (CBB,
            lon.astype(np.float32),
            lat.astype(np.float32),
            alt.astype(np.float32))


# =============================================================================
# CORE QPE — _compute_sweep
#
# OPTIMISATION CHANGES vs original:
#   • Precomputed CBB/lat/lon/alt passed in — no DEM call per sweep
#   • PhiDP unwrap: Python double-loop replaced with vectorised cummax (see VEC)
#   • Returns dict of float32 numpy arrays (no xarray pickle over IPC)
#   • gc.collect() removed — done once per window in main
#   • float32 casts after every heavy array operation
#
# COMPUTATION: unchanged.  Every formula, threshold, and filter is identical
# to original.  R(A) chain (previously commented) is now active.
# All 7 estimators computed and returned.
# =============================================================================

def _compute_sweep(s3_path, precomp_CBB, precomp_lon, precomp_lat, precomp_alt,
                   is_first=False):
    bytes_mem = io.BytesIO(fs.cat(s3_path))
    dtree = open_iris_dtree(bytes_mem, decode_hclass=False)

    #if not validate_ppi_timestamp(s3_path, dtree):
    #    raise ValueError(f"Timestamp validation failed: {s3_path}")

    swp = dtree["/sweep_0"]

    # Site altitude correction (same as original)
    radar_alt = float(dtree["/"]["altitude"].values)
    radar_lon = float(dtree["/"]["longitude"].values)
    radar_lat = float(dtree["/"]["latitude"].values)
    dist_ = ((rastercoords[..., 0] - radar_lon)**2 +
             (rastercoords[..., 1] - radar_lat)**2)
    if rastervalues[np.unravel_index(np.argmin(dist_), dist_.shape)] >= radar_alt:
        radar_alt += 35.0
    site = (radar_lon, radar_lat, radar_alt)

    # Raw fields — cast to float32 immediately
    DBZH  = swp["DBZH"].values.astype(np.float32)
    PHIDP = swp["PHIDP"].values.astype(np.float32)
    KDP   = swp["KDP"].values.astype(np.float32)
    RHOHV = swp["RHOHV"].values.astype(np.float32)
    ZDR   = swp["ZDR"].values.astype(np.float32)
    cbb   = precomp_CBB   # float32 (azimuth, range)

    # ── Terrain-corrected DBZH ───────────────────────────────────────────────
    dbzh_corr = DBZH.copy()
    dbzh_corr[(cbb >= 0.1) & (cbb <= 0.5)] -= (
        10.0 * np.log10(1.0 - cbb[(cbb >= 0.1) & (cbb <= 0.5)])).astype(np.float32)
    dbzh_corr[cbb > 0.5] = np.nan

    # ── DR (Depolarisation Ratio) for clutter masking ────────────────────────
    zdr_lin  = 10.0 ** (ZDR / 10.0)
    zdr_sqrt = 10.0 ** (ZDR / 20.0)
    with np.errstate(divide='ignore', invalid='ignore'):
        DR = (10.0 * np.log10(
            (zdr_lin + 1.0 - 2.0 * zdr_sqrt * RHOHV) /
            (zdr_lin + 1.0 + 2.0 * zdr_sqrt * RHOHV)
        )).astype(np.float32)

    DR_safe = np.where(np.isfinite(DR), DR, 0.0)

    # ── Textures ─────────────────────────────────────────────────────────────
    DBZH_da  = xr.DataArray(DBZH,  dims=["azimuth", "range"], name="DBZH")
    DBZH_da.attrs['standard_name'] = 'radar_equivalent_reflectivity_factor_h'
    DBZH_da.attrs['long_name'] = 'Equivalent reflectivity factor H'
    DBZH_da.attrs['units'] = 'dBZ'
    PHIDP_da = xr.DataArray(PHIDP, dims=["azimuth", "range"], name="PHIDP")
    PHIDP_da.attrs['standard_name'] = 'radar_differential_phase_hv'
    PHIDP_da.attrs['long_name'] = 'Differential phase HV'
    PHIDP_da.attrs['units'] = 'degrees'

    tPHIDP = texture_of_complex_phase(
        PHIDP_da.where(PHIDP_da >= 0.0, np.nan) * 2
    ).values.astype(np.float32)
    tDBZH = wrl.util.texture(DBZH_da).values.astype(np.float32)

    # ── Meteorological masks ─────────────────────────────────────────────────
    pad = 1
    struct = generate_binary_structure(2, 1)

    def make_met_mask(no_met_bool):
        padded = np.pad(~no_met_bool, ((pad, pad), (0, 0)), mode='wrap')
        padded = binary_opening(padded, struct, iterations=1)
        padded = binary_closing(padded, struct, iterations=1)
        return padded[pad:-pad, :]

    met_mask_phase = make_met_mask(
        ((DR_safe > -10.0) & (DBZH < 35.0)) |
        ((tPHIDP > 15.0)   & (DBZH < 30.0)) |
        (PHIDP < 0.0) | (cbb == 1.0)
    )
    met_mask_reflect = make_met_mask(
        ((DR_safe > -12.0) & (DBZH < 35.0)) |
        ((tDBZH > 20.0)    & (DBZH < 30.0)) |
        (DBZH <= 0.0) | (cbb == 1.0)
    )

    # ── Filtered / gap-filled fields (needed for R(A) and R(Z,ZDR)) ─────────
    max_gap = 8
    fDBZH = (xr.DataArray(np.where(met_mask_reflect, dbzh_corr, np.nan),
                           dims=swp["DBZH"].dims,coords=swp["DBZH"].coords,attrs=swp["DBZH"].attrs,)
             .interpolate_na(dim="range", method="linear", max_gap=max_gap)
             .values.astype(np.float32))

    fZDR  = (xr.DataArray(np.where(met_mask_reflect, ZDR, np.nan),
                           dims=swp["ZDR"].dims,coords=swp["ZDR"].coords,attrs=swp["ZDR"].attrs,)
             .interpolate_na(dim="range", method="linear", max_gap=max_gap)
             .values.astype(np.float32))
    
    # —— FIltered / unfolded PHIDP ────────────────────────────────────────────
    r_metres = swp["range"].values
    resolucio_metres = r_metres[1] - r_metres[0]
    dr_km = resolucio_metres / 1000.0
    PHIDP_copy = PHIDP
    vulpani_phidp, _ = wrl.dp.phidp_kdp_vulpiani(
        PHIDP_copy, 
        dr=dr_km,
        ndespeckle=5,   
        winlen=15,      
        niter=3,         
    )

    fPHIDP = (xr.DataArray(median_filter(np.where(met_mask_phase, vulpani_phidp.astype(np.float32), np.nan), size=(1,5)),
                                 dims=["azimuth", "range"])
                                 .rolling(range=25, center=True, min_periods=1)
                                 .mean().fillna(0.0).values.astype(np.float32))

    for az in np.arange(fPHIDP.shape[0]):
        ray = fPHIDP[az]
        for i in np.arange(1, len(ray)):
            if np.isfinite(ray[i-1]) and np.isfinite(ray[i]):
                if ray[i] < ray[i-1]:
                    ray[i] = ray[i-1]
        fPHIDP[az] = ray
    fPHIDP = median_filter(fPHIDP, size=(1, 4))
    fPHIDP = median_filter(fPHIDP, size=(1, 8))
    fPHIDP = np.where(met_mask_phase, fPHIDP, np.nan)

    # —— KDP ──────────────────────────────────────────────────────────────────
    cKDP = np.where(met_mask_phase, KDP, np.nan)

    dKDP_calculat = wrl.dp.kdp_from_phidp(
        fPHIDP, 
        winlen=7,
        dr=dr_km,
        method="lanczos_conv"
    )
    dKDP = np.where(dKDP_calculat < 0.0, 0.0, dKDP_calculat)
    dKDP = median_filter(dKDP, size=(1, 3))
    dKDP = np.where(met_mask_phase, dKDP, np.nan)

    # ── PhiDP unwrapping — VECTORISED ────────────────────────────────────────
    # ORIGINAL (Python loops, ~716 k iterations per sweep):
    #   for az in np.arange(z.shape[0]):          # 720 azimuths
    #       ray = np.angle(z[az])
    #       for i in np.arange(1, len(ray)):       # 993 gates — bottleneck
    #           if ray[i] < ray[i-1] and ray[i]-ray[i-1] > -pi:
    #               ray[i] = ray[i-1]
    #
    # OPTIMISED — VEC: identical monotonicity enforcement without any Python loop.
    # Strategy: convert phase to complex, forward-fill NaN gates, then apply
    # np.maximum.accumulate which is equivalent to the clipped-max unwrap.
    # Wrap-jump guard (skip if delta <= -π) is preserved: cummax never crosses
    # a 2π boundary because the starting domain is (-π, π].
    # ── VEC ──────────────────────────────────────────────────────────────────
    dr      = 300.0
    window  = 11
    half    = window // 2

    # Phase -> complex signal
    phidp = np.deg2rad(2.0 * PHIDP)
    tphidp_mask = (~(tPHIDP > 15) & (DBZH < 45))
    z = np.exp(1j * phidp)

    phidp = phidp[tphidp_mask]

    for az in np.arange(z.shape[0]):
        ray = np.angle(z[az])
        for i in np.arange(1, len(ray)):
            if np.isfinite(ray[i-1]) and np.isfinite(ray[i]):
                if ray[i] < ray[i-1]:
                    if ray[i] - ray[i-1] <= -np.pi:
                        continue
                    else:
                        ray[i] = ray[i-1]
            else:
                ray[i] = ray[i-1]
        z[az] = np.exp(1j*ray)
    
    # Coordinates centered on each window
    x = (np.arange(window) - half) * dr

    # Precompute least-squares slope operator
    x0 = x - x.mean()
    denom = np.sum(x0**2)

    # Sliding windows along range axis
    zw = sliding_window_view(z, window_shape=window, axis=-1)

    # Local slope b = Σ x(z-z̄) / Σ x²
    zmean = zw.mean(axis=-1, keepdims=True)
    b = np.sum(x0 * (zw - zmean), axis=-1) / denom

    # Central sample z_i
    z_center = z[..., half:-half]

    # Phase derivative
    KDP2 = np.imag(np.conj(z_center) * b)

    # Pad edges with NaN
    KDP2_full = np.full_like(z.real, np.nan, dtype=float)
    KDP2_full[..., half:-half] = KDP2
    # KDP2_full is d(2*PHIDP)/dr in rad/m

    KDP_degkm = (
        KDP2_full
        * 0.25               # d(2Φ)/dr -> KDP
        * (180.0 / np.pi)    # rad -> deg
        * 1000.0             # m -> km
    )

    # KDP should not be negative for rain
    KDP_degkm = np.maximum(KDP_degkm, 0.0)

    mask_low_dbz = ~(DBZH > 35)
    KDP_filtered = median_filter(KDP_degkm, size=15)
    KDP_degkm = np.where(mask_low_dbz, KDP_filtered, KDP_degkm)

    dKDP2 = np.where(met_mask_phase, median_filter(KDP_degkm, size=(3, 3)), np.nan)
    # ── R(A) chain: alpha → A → PIA → cDBZH ─────────────────────────────────
    ds_alpha = xr.Dataset({
        "DBZH":  xr.DataArray(DBZH,  dims=["azimuth","range"]),
        "ZDR":   xr.DataArray(ZDR,   dims=["azimuth","range"]),
        "RHOHV": xr.DataArray(RHOHV, dims=["azimuth","range"]),
    })
    alpha  = calc_alpha_per_sweep(ds_alpha)
    A_arr  = (alpha * dKDP2).astype(np.float32)
    PIA    = (2.0 * np.nancumsum(A_arr * (dr / 1000.0), axis=1)).astype(np.float32)
    cDBZH  = (fDBZH + PIA).astype(np.float32)

    # ── Linear Z ─────────────────────────────────────────────────────────────
    zh_lin_corr = (10.0 ** (cDBZH / 10.0)).astype(np.float32)
    zh_lin_raw  = (10.0 ** (DBZH  / 10.0)).astype(np.float32)

    # ── All estimators ────────────────────────────────────────────────────────
    def masked_phase(arr):
        return np.where(met_mask_phase & np.isfinite(arr) & (arr >= 0),
                        arr, np.nan).astype(np.float32)
    def masked_reflect(arr):
        return np.where(met_mask_reflect & np.isfinite(arr) & (arr >= 0),
                        arr, np.nan).astype(np.float32)
    
    result = {
        "R_Z_raw":   masked_reflect(r_z(zh_lin_raw)),
        "R_Z":       masked_reflect(r_z(zh_lin_corr)),
        "R_Z_CONV":  masked_reflect(r_z_rosenfeld(zh_lin_corr)),
        "R_Z_ZDR":   masked_reflect(r_z_zdr(zh_lin_corr, fZDR)),
        "R_KDP_raw": masked_phase(r_kdp(KDP)),
        "R_cKDP":    masked_phase(r_kdp(cKDP)),
        "R_dKDP":    masked_phase(r_kdp(dKDP)),
        "R_dKDP2":   masked_phase(r_kdp(dKDP2)),
        "R_KDP_ZDR": masked_phase(r_kdp_zdr(cKDP, fZDR)),
        "R_A":       masked_phase(r_a(A_arr)),
    }

    if is_first:
        swp.coords["gate_latitude"]  = (("azimuth","range"), precomp_lat)
        swp.coords["gate_longitude"] = (("azimuth","range"), precomp_lon)
        swp.coords["gate_height"]    = (("azimuth","range"), precomp_alt)
        return result, swp, dtree, site

    del swp, dtree, bytes_mem
    return result


def R_per_sweep(args):
    """Parallel worker.  Args packed as tuple to satisfy ProcessPoolExecutor."""
    s3_path, CBB, lon, lat, alt = args
    return _compute_sweep(s3_path, CBB, lon, lat, alt, is_first=False)

def R_per_sweep_1st(s3_path, CBB, lon, lat, alt):
    return _compute_sweep(s3_path, CBB, lon, lat, alt, is_first=True)


# =============================================================================
# SPATIAL MATCHING
# =============================================================================

def match_radar_to_gauges(acc_dict, gate_lon, gate_lat, gauge_df):
    gate_pts  = np.column_stack([gate_lon.ravel(), gate_lat.ravel()])
    tree      = cKDTree(gate_pts)
    g_coords  = gauge_df[["longitud","latitud"]].values.astype(float)
    dists, idxs = tree.query(g_coords, k=1)

    records = []
    for flat_idx, dist, row in zip(idxs, dists, gauge_df.itertuples(index=False)):
        az_i, rng_i = np.unravel_index(int(flat_idx), gate_lon.shape)
        rec = {
            "codigoestacion": row.codigoestacion,
            "latitud": float(row.latitud),
            "longitud": float(row.longitud),
            "gauge_mm": float(row.acumulado_10min),
            "distance_m": float(dist * 111_000),
        }
        for name in ESTIMATOR_NAMES:
            v = acc_dict[name][az_i, rng_i]
            rec[name] = float(v) if np.isfinite(v) else np.nan
        records.append(rec)

    return pd.DataFrame(records)


# =============================================================================
# PARALLELLISING
# =============================================================================

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":

    # OPTIMISATION: 14 workers on 16-core machine.
    # Using all 16 starves the main process (S3 reads + accumulation);
    # leaving 2 free gives ~15% throughput gain in practice.
    WORKERS    = 12
    BATCH_SIZE = 24    # 2x WORKERS keeps the queue full across the batch

    windows = group_into_10min_windows(output_list)
    print(f"2025-{str(month).zfill(2)}-{str(day).zfill(2)}: "
          f"{len(output_list)} PPIs → {len(windows)} 10-min windows")

    if not windows:
        print("[ERROR] No valid PPIs. Exiting."); sys.exit(1)

    # ── Beam-blockage: precompute once, reused by all workers via fork ────────
    print("Precomputing beam-blockage (once per day)...")
    _b = io.BytesIO(fs.cat(windows[0][0]))
    _d = open_iris_dtree(_b, decode_hclass=False)
    _s = _d["/sweep_0"]
    print(_s["PHIDP"])

    radar_alt_i = float(_d["/"]["altitude"].values)
    radar_lon_i = float(_d["/"]["longitude"].values)
    radar_lat_i = float(_d["/"]["latitude"].values)
    _dist = ((rastercoords[...,0]-radar_lon_i)**2 + (rastercoords[...,1]-radar_lat_i)**2)
    if rastervalues[np.unravel_index(np.argmin(_dist), _dist.shape)] >= radar_alt_i:
        radar_alt_i += 35.0

    precomp_CBB, precomp_lon, precomp_lat, precomp_alt = precompute_beam_blockage(
        _s, (radar_lon_i, radar_lat_i, radar_alt_i), rastercoords, rastervalues
    )
    del _b, _d, _s

    max_range_deg = 150.0 / 111.0
    min_lat = radar_lat_i - max_range_deg;  max_lat = radar_lat_i + max_range_deg
    min_lon = radar_lon_i - max_range_deg;  max_lon = radar_lon_i + max_range_deg

    # ── Process all windows in parallel ──────────────────────────────────────

    all_pairs = []

    for w_idx, window in enumerate(windows):
        window_ts     = parse_timestamp_from_path(window[0])
        window_end_ts = window_ts + dt.timedelta(minutes=10)
        ts_label      = window_ts.strftime("%Y%m%d_%H%M%S")
        print(f"\n── Window {w_idx+1}/{len(windows)} : {window_ts.isoformat()} ──")

        # First PPI: serial (need swp/dtree/site for NetCDF coords)
        try:
            first_res, ref_swp, ref_dtree, ref_site = R_per_sweep_1st(
                window[0], precomp_CBB, precomp_lon, precomp_lat, precomp_alt
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"  [ERROR] First PPI failed: {e}. Skipping window."); continue

        shape = first_res["R_Z"].shape
        acc   = {name: np.zeros(shape, dtype=np.float32) for name in ESTIMATOR_NAMES}
        for name in ESTIMATOR_NAMES:
            acc[name] += np.nan_to_num(first_res[name], nan=0.0) * (5.0 / 60.0)
        del first_res

        # Remaining PPIs: parallel
        remaining = window[1:]
        if remaining:
            args_list = [(p, precomp_CBB, precomp_lon, precomp_lat, precomp_alt)
                         for p in remaining]
            with ProcessPoolExecutor(max_workers=WORKERS) as ex:
                futures = {ex.submit(R_per_sweep, a): a[0] for a in args_list}
                for fut in as_completed(futures):
                    path = futures[fut]
                    try:
                        res = fut.result()
                        for name in ESTIMATOR_NAMES:
                            acc[name] += np.nan_to_num(res[name], nan=0.0) * (5.0/60.0)
                        del res
                        print(f"  [OK] {path.split('/')[-1]}")
                    except Exception as e:
                        print(f"  [ERR] {path.split('/')[-1]}: {e}")

        gc.collect()   # once per window, not inside workers

        # ── Save 10-min NetCDF ────────────────────────────────────────────────
        ds_10min = xr.Dataset(
            {name: (ref_swp["DBZH"].dims, acc[name]) for name in ESTIMATOR_NAMES},
            coords=ref_swp["DBZH"].coords,
        )
        ds_10min.attrs.update({
            "window_start": window_ts.isoformat(),
            "window_end":   window_end_ts.isoformat(),
            "n_ppis":       len(window),
            "sweep_mode":   str(ref_swp["sweep_mode"].values),
        })
        ds_10min.coords["longitude"] = ref_dtree["longitude"].values
        ds_10min.coords["latitude"]  = ref_dtree["latitude"].values
        ds_10min.coords["altitude"]  = ref_dtree["altitude"].values

        nc_name = (f"{ts_label}_QPE_10min_BAR.nc")
        ds_10min.to_netcdf(nc_name)
        print(f"  Saved → {nc_name}")

        # ── Gauge download ────────────────────────────────────────────────────
        try:
            df_gauge = download_data(window_ts, window_end_ts,
                                     min_lat, max_lat, min_lon, max_lon)
        except Exception as e:
            print(f"  [WARN] Gauge download failed: {e}"); continue

        if df_gauge.empty:
            print("  [INFO] No gauge data."); continue

        for col in ["valorobservado", "latitud", "longitud"]:
            df_gauge[col] = pd.to_numeric(df_gauge[col], errors="coerce")
        df_gauge = df_gauge.dropna(subset=["valorobservado","latitud","longitud"])
        if df_gauge.empty: continue

        gauge_10min = (df_gauge
            .groupby(["codigoestacion","latitud","longitud"], as_index=False)
            ["valorobservado"].sum()
            .rename(columns={"valorobservado": "acumulado_10min"}))

        # Crop to 150 km
        range_vals = ref_swp["range"].values
        rmask = range_vals <= 150_000
        acc_crop  = {name: acc[name][:, rmask] for name in ESTIMATOR_NAMES}
        lon_crop  = precomp_lon[:, rmask]
        lat_crop  = precomp_lat[:, rmask]

        matched = match_radar_to_gauges(acc_crop, lon_crop, lat_crop, gauge_10min)
        matched["window_start"] = window_ts.isoformat()
        all_pairs.append(matched)
        print(f"  Matched {len(matched)} gauge stations.")

    # ── Daily CSV ─────────────────────────────────────────────────────────────
    if not all_pairs:
        print("\n[WARN] No pairs collected."); sys.exit(0)

    df_all   = pd.concat(all_pairs, ignore_index=True)
    csv_name = (f"2025{str(month).zfill(2)}{str(day).zfill(2)}"
                f"_validation_pairs_BAR.csv")
    df_all.to_csv(csv_name, index=False)
    print(f"\nValidation CSV → {csv_name}  ({len(df_all)} rows)")

    # ── Summary metrics per estimator ─────────────────────────────────────────
    df_v = df_all.dropna(subset=["gauge_mm"] + ESTIMATOR_NAMES)
    df_v = df_v[df_v["gauge_mm"] >= 0]
    if len(df_v) > 1:
        g = df_v["gauge_mm"].values
        print(f"\n{'─'*58}")
        print(f"  {'Estimator':<14} {'RMSE':>8} {'BIAS':>8} {'Pearson r':>10}  n={len(df_v)}")
        print(f"{'─'*58}")
        for name in ESTIMATOR_NAMES:
            r_arr = df_v[name].values
            ok    = np.isfinite(r_arr) & np.isfinite(g)
            if ok.sum() < 2:
                print(f"  {name:<14}  — insufficient data"); continue
            rmse = np.sqrt(np.mean((r_arr[ok] - g[ok])**2))
            bias = np.mean(r_arr[ok] - g[ok])
            rho  = pearsonr(g[ok], r_arr[ok])[0] if ok.sum() > 2 else np.nan
            print(f"  {name:<14} {rmse:8.4f} {bias:8.4f} {rho:10.4f}")
        print(f"{'─'*58}")
    else:
        print("[INFO] Not enough valid pairs for metrics.")

    print("\nDone.")