# =============================================================================
# Validació QPE — 10-minute radar vs gauge, all estimators, parametrisation
# Usage (unchanged bash loop):
#   python Validació_10min.py <day> <month>
# =============================================================================

import xradar as xd
import sys
import wradlib as wrl
import numpy as np
import xarray as xr
import os
import shutil
import time
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

# ── STRIPPED DOWN AND EXPANDED TO SEPARATE SWEEPS FOR REGRESSION ──
ESTIMATOR_NAMES = [
    "ZH_LIN_1_raw", "ZH_LIN_2_raw",
    "ZH_LIN_1",     "ZH_LIN_2",
    "KDP_1_raw",    "KDP_2_raw",
    "cKDP_1",       "cKDP_2",
    "dKDP_1",       "dKDP_2",
    "dKDP2_1",      "dKDP2_2",
    "A_1",          "A_2",
    "ZDR_1",        "ZDR_2"
]

# Define worker process globals explicitly
GLOBAL_CBB = None
GLOBAL_LON = None
GLOBAL_LAT = None
GLOBAL_ALT = None
GLOBAL_DF_GAUGES = pd.DataFrame()

# =============================================================================
# GAUGE DOWNLOAD (With Robust Retry Backoff for 503 Server Errors)
# =============================================================================

def download_data(date_init, date_end, min_lat, max_lat, min_lon, max_lon):
    client = Socrata("www.datos.gov.co", TOKEN)
    max_retries = 5
    backoff = 3
    
    for attempt in range(max_retries):
        try:
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
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"  [WARN] Socrata API request failed ({e}). Retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff *= 2
            else:
                raise e

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

    bin_edges   = np.arange(min_zh, max_zh + bin_width, bin_width)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2.0
    bin_idx     = np.digitize(zh_v, bin_edges) - 1
    n_bins      = len(bin_centers)
    median_zdr  = np.full(n_bins, np.nan)
    for b in range(n_bins):
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

def open_iris_dtree(filepath, decode_hclass=False):
    return xd.io.open_iris_datatree(filepath)

with open(f'metadata/2025/{str(month).zfill(2)}/{str(day).zfill(2)}.json', 'r') as f:
    data = json.load(f)

def get_stacks(json_data):
    all_stacks = []
    for station_name, station_data in json_data.items():
        current_stack = []
        last_el = float('inf')
        
        for _, value in station_data.items():
            sweeps = value['sweeps']
            el = float(sweeps[list(sweeps.keys())[0]]['elevation_angle'])
            if el <= last_el:
                if current_stack: all_stacks.append(current_stack)
                current_stack = []
            current_stack.append(value['filepath'])
            last_el = float(sweeps[list(sweeps.keys())[-1]]['elevation_angle'])
            
        if current_stack: 
            all_stacks.append(current_stack)
            
    return all_stacks

stacks      = get_stacks(data)
lowest_ppi  = np.array([vol[0] for vol in stacks])
output_list = [e for e in lowest_ppi if "Barrancabermeja" in e.split("/")]

dem = wrl.io.open_raster("Barrancabermeja.tif")
rastervalues, rastercoords, crs = wrl.georef.extract_raster_dataset(dem, nodata=-32768.0)

# =============================================================================
# TIMESTAMP UTILITIES
# =============================================================================

def parse_timestamp_from_path(file_path):
    m = re.match(r"BAR(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})", file_path.split("/")[-1])
    if not m: return None
    yy, mo, dd, hh, mm, ss = (int(x) for x in m.groups())
    return dt.datetime(2000+yy, mo, dd, hh, mm, 00, tzinfo=dt.timezone.utc)

def group_into_10min_windows(file_list):
    stamped = [(parse_timestamp_from_path(p), p) for p in file_list]
    stamped = [(ts, p) for ts, p in stamped if ts is not None]
    stamped.sort(key=lambda x: x[0])

    from collections import defaultdict
    slots = defaultdict(list)
    for ts, path in stamped:
        slot_key = (ts.year, ts.month, ts.day, (ts.hour * 60 + ts.minute) // 10)
        slots[slot_key].append(path)

    return [paths for _, paths in sorted(slots.items())]

# =============================================================================
# BEAM-BLOCKAGE PRECOMPUTED ONCE PER DAY
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
# CORE PROCESSING — (Masking and Computation logic strictly unaltered)
# =============================================================================

def _compute_sweep(file_path, precomp_CBB, precomp_lon, precomp_lat, precomp_alt,
                   is_first=False):
    
    dtree = open_iris_dtree(file_path, decode_hclass=False)
    swp = dtree["/sweep_0"]

    radar_alt = float(dtree["/"]["altitude"].values)
    radar_lon = float(dtree["/"]["longitude"].values)
    radar_lat = float(dtree["/"]["latitude"].values)
    dist_ = ((rastercoords[..., 0] - radar_lon)**2 +
             (rastercoords[..., 1] - radar_lat)**2)
    if rastervalues[np.unravel_index(np.argmin(dist_), dist_.shape)] >= radar_alt:
        radar_alt += 35.0
    site = (radar_lon, radar_lat, radar_alt)

    DBZH  = swp["DBZH"].values.astype(np.float32)
    PHIDP = swp["PHIDP"].values.astype(np.float32)
    KDP   = swp["KDP"].values.astype(np.float32)
    RHOHV = swp["RHOHV"].values.astype(np.float32)
    ZDR   = swp["ZDR"].values.astype(np.float32)
    cbb   = precomp_CBB   

    dbzh_corr = DBZH.copy()
    dbzh_corr[(cbb >= 0.1) & (cbb <= 0.5)] -= (
        10.0 * np.log10(1.0 - cbb[(cbb >= 0.1) & (cbb <= 0.5)])).astype(np.float32)
    dbzh_corr[cbb > 0.5] = np.nan

    zdr_lin  = 10.0 ** (ZDR / 10.0)
    zdr_sqrt = 10.0 ** (ZDR / 20.0)
    with np.errstate(divide='ignore', invalid='ignore'):
        DR = (10.0 * np.log10(
            (zdr_lin + 1.0 - 2.0 * zdr_sqrt * RHOHV) /
            (zdr_lin + 1.0 + 2.0 * zdr_sqrt * RHOHV)
        )).astype(np.float32)

    DR_safe = np.where(np.isfinite(DR), DR, 0.0)

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

    max_gap = 8
    fDBZH = (xr.DataArray(np.where(met_mask_reflect, dbzh_corr, np.nan),
                           dims=swp["DBZH"].dims,coords=swp["DBZH"].coords,attrs=swp["DBZH"].attrs,)
             .interpolate_na(dim="range", method="linear", max_gap=max_gap)
             .values.astype(np.float32))

    fZDR  = (xr.DataArray(np.where(met_mask_reflect, ZDR, np.nan),
                           dims=swp["ZDR"].dims,coords=swp["ZDR"].coords,attrs=swp["ZDR"].attrs,)
             .interpolate_na(dim="range", method="linear", max_gap=max_gap)
             .values.astype(np.float32))
    
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

    dr      = 300.0
    window  = 11
    half    = window // 2

    phidp = np.deg2rad(2.0 * PHIDP)
    tphidp_mask = (~(tPHIDP > 15) & (DBZH < 45))
    z = np.exp(1j * phidp)

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
    
    x = (np.arange(window) - half) * dr
    x0 = x - x.mean()
    denom = np.sum(x0**2)

    zw = sliding_window_view(z, window_shape=window, axis=-1)
    zmean = zw.mean(axis=-1, keepdims=True)
    b = np.sum(x0 * (zw - zmean), axis=-1) / denom

    z_center = z[..., half:-half]
    KDP2 = np.imag(np.conj(z_center) * b)

    KDP2_full = np.full_like(z.real, np.nan, dtype=float)
    KDP2_full[..., half:-half] = KDP2

    KDP_degkm = (KDP2_full * 0.25 * (180.0 / np.pi) * 1000.0)
    KDP_degkm = np.maximum(KDP_degkm, 0.0)

    mask_low_dbz = ~(DBZH > 35)
    KDP_filtered = median_filter(KDP_degkm, size=15)
    KDP_degkm = np.where(mask_low_dbz, KDP_filtered, KDP_degkm)

    dKDP2 = np.where(met_mask_phase, median_filter(KDP_degkm, size=(3, 3)), np.nan)

    ds_alpha = xr.Dataset({
        "DBZH":  xr.DataArray(DBZH,  dims=["azimuth","range"]),
        "ZDR":   xr.DataArray(ZDR,   dims=["azimuth","range"]),
        "RHOHV": xr.DataArray(RHOHV, dims=["azimuth","range"]),
    })
    alpha  = calc_alpha_per_sweep(ds_alpha)
    A_arr  = (alpha * cKDP).astype(np.float32)
    PIA    = (2.0 * np.nancumsum(A_arr * (dr / 1000.0), axis=1)).astype(np.float32)
    cDBZH  = (fDBZH + PIA).astype(np.float32)

    zh_lin_corr = (10.0 ** (cDBZH / 10.0)).astype(np.float32)
    zh_lin_raw  = (10.0 ** (DBZH  / 10.0)).astype(np.float32)

    def masked_phase(arr):
        return np.where(met_mask_phase & np.isfinite(arr) & (arr >= 0), arr, np.nan).astype(np.float32)
    def masked_reflect(arr):
        return np.where(met_mask_reflect & np.isfinite(arr) & (arr >= 0), arr, np.nan).astype(np.float32)
    
    result = {
            "ZH_LIN_raw": zh_lin_raw,
            "Z_LIN":      masked_reflect(zh_lin_corr),
            "KDP_raw":    KDP,
            "cKDP":       masked_phase(cKDP),
            "dKDP":       masked_phase(dKDP),
            "dKDP2":      masked_phase(dKDP2),
            "A":          masked_phase(A_arr),
            "ZDR":        masked_reflect(fZDR)
        }

    if is_first:
        swp.coords["gate_latitude"]  = (("azimuth","range"), precomp_lat)
        swp.coords["gate_longitude"] = (("azimuth","range"), precomp_lon)
        swp.coords["gate_height"]    = (("azimuth","range"), precomp_alt)
        return result, swp, dtree, site

    del swp, dtree
    return result

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
# WORKER INITIALIZATION (Ensures global safety across all OS Multiprocessing types)
# =============================================================================

def init_worker(cbb, lon, lat, alt, df_gauges):
    global GLOBAL_CBB, GLOBAL_LON, GLOBAL_LAT, GLOBAL_ALT, GLOBAL_DF_GAUGES
    GLOBAL_CBB = cbb
    GLOBAL_LON = lon
    GLOBAL_LAT = lat
    GLOBAL_ALT = alt
    GLOBAL_DF_GAUGES = df_gauges

# =============================================================================
# PARALLELLISING — Jumps any non-2 standard sweep sizes automatically
# =============================================================================

def process_window(args):
    w_idx, total_w, window = args

    window_ts     = parse_timestamp_from_path(window[0])
    window_end_ts = window_ts + dt.timedelta(minutes=10)
    ts_label      = window_ts.strftime("%Y%m%d_%H%M%S")
    
    global GLOBAL_CBB, GLOBAL_LON, GLOBAL_LAT, GLOBAL_ALT, GLOBAL_DF_GAUGES
    
    if len(window) != 2:
        print(f"  [JUMP] Skipping window {ts_label}: contains {len(window)} PPIs (strict requirement: 2).")
        return None

    window_corrupted = False
    acc = {}

    try:
        # Extract Sweep 1
        res_1, ref_swp, ref_dtree, ref_site = _compute_sweep(
            window[0], GLOBAL_CBB, GLOBAL_LON, GLOBAL_LAT, GLOBAL_ALT, is_first=True
        )
        
        # Extract Sweep 2
        res_2 = _compute_sweep(
            window[1], GLOBAL_CBB, GLOBAL_LON, GLOBAL_LAT, GLOBAL_ALT, is_first=False
        )
        
        # FIXED: Mapped all 16 variables to match ESTIMATOR_NAMES array definitions
        acc["ZH_LIN_1_raw"] = res_1["ZH_LIN_raw"]
        acc["ZH_LIN_2_raw"] = res_2["ZH_LIN_raw"]
        
        acc["ZH_LIN_1"]     = res_1["Z_LIN"]
        acc["ZH_LIN_2"]     = res_2["Z_LIN"]
        
        acc["KDP_1_raw"]    = res_1["KDP_raw"]
        acc["KDP_2_raw"]    = res_2["KDP_raw"]
        
        acc["cKDP_1"]       = res_1["cKDP"]
        acc["cKDP_2"]       = res_2["cKDP"]
        
        acc["dKDP_1"]       = res_1["dKDP"]
        acc["dKDP_2"]       = res_2["dKDP"]
        
        acc["dKDP2_1"]      = res_1["dKDP2"]
        acc["dKDP2_2"]      = res_2["dKDP2"]
        
        acc["A_1"]          = res_1["A"]
        acc["A_2"]          = res_2["A"]
        
        acc["ZDR_1"]        = res_1["ZDR"]
        acc["ZDR_2"]        = res_2["ZDR"]

    except Exception as e:
        print(f"  [ERROR] Failed to process radar sweeps for window {ts_label}: {e}")
        window_corrupted = True

    if window_corrupted:
        return None

    gc.collect() 

    if GLOBAL_DF_GAUGES.empty:
        return None

    df_window_gauge = GLOBAL_DF_GAUGES[
        (GLOBAL_DF_GAUGES["parsed_dt"] >= window_ts) & 
        (GLOBAL_DF_GAUGES["parsed_dt"] <= window_end_ts)
    ]

    if df_window_gauge.empty:
        return None

    gauge_10min = (df_window_gauge
         .groupby(["codigoestacion","latitud","longitud"], as_index=False)
         ["valorobserved"].sum() if "valorobserved" in df_window_gauge.columns else 
         df_window_gauge.groupby(["codigoestacion","latitud","longitud"], as_index=False)["valorobservado"].sum()
    ).rename(columns={"valorobservado": "acumulado_10min", "valorobserved": "acumulado_10min"})

    range_vals = ref_swp["range"].values
    rmask = range_vals <= 150_000
    acc_crop  = {name: acc[name][:, rmask] for name in ESTIMATOR_NAMES}
    lon_crop  = GLOBAL_LON[:, rmask]
    lat_crop  = GLOBAL_LAT[:, rmask]

    matched = match_radar_to_gauges(acc_crop, lon_crop, lat_crop, gauge_10min)
    matched["window_start"] = window_ts.isoformat()
    print(f"  [DONE] Window {ts_label} paired {len(matched)} gauge points.")
    
    return matched

# =============================================================================
# MAIN PIPELINE RUNNER
# =============================================================================
if __name__ == "__main__":

    if not output_list:
        print("[ERROR] No valid PPIs inside output_list. Exiting."); sys.exit(1)

    print("\n📥 Localizing radar files for the day into central cache directory...")
    os.makedirs("./radar_cache", exist_ok=True)
    fs_download = s3fs.S3FileSystem(anon=True)
    
    s3_to_local_map = {}
    total_files = len(output_list)
    
    for idx, s3_path in enumerate(output_list, 1):
        filename = s3_path.split("/")[-1]
        local_path = os.path.join("./radar_cache", filename)
        s3_to_local_map[s3_path] = local_path
        
        if not os.path.exists(local_path):
            print(f"  [{idx}/{total_files}] Downloading {filename}...", end="\r")
            for attempt in range(3):
                try:
                    fs_download.get(s3_path, local_path)
                    break
                except Exception as e:
                    if attempt == 2:
                        print(f"\n[WARN] S3 download timed out for {filename}: {e}")
                    time.sleep(1)

    local_output_list = [s3_to_local_map[p] for p in output_list if p in s3_to_local_map and os.path.exists(s3_to_local_map[p])]
    windows = group_into_10min_windows(local_output_list)
    print(f"\n✅ All {len(local_output_list)} files cached locally → Packed into {len(windows)} windows.")

    if not windows:
        print("[ERROR] No valid local PPIs found to group. Exiting."); sys.exit(1)

    print("\nPrecomputing beam-blockage (once per day)...")
    _d = open_iris_dtree(windows[0][0], decode_hclass=False)
    _s = _d["/sweep_0"]

    radar_alt_i = float(_d["/"]["altitude"].values)
    radar_lon_i = float(_d["/"]["longitude"].values)
    radar_lat_i = float(_d["/"]["latitude"].values)
    _dist = ((rastercoords[...,0]-radar_lon_i)**2 + (rastercoords[...,1]-radar_lat_i)**2)
    if rastervalues[np.unravel_index(np.argmin(_dist), _dist.shape)] >= radar_alt_i:
        radar_alt_i += 35.0

    precomp_CBB, precomp_lon, precomp_lat, precomp_alt = precompute_beam_blockage(
        _s, (radar_lon_i, radar_lat_i, radar_alt_i), rastercoords, rastervalues
    )
    del _d, _s

    # Initialize main process variables
    GLOBAL_CBB = precomp_CBB
    GLOBAL_LON = precomp_lon
    GLOBAL_LAT = precomp_lat
    GLOBAL_ALT = precomp_alt

    max_range_deg = 150.0 / 111.0
    min_lat = radar_lat_i - max_range_deg;  max_lat = radar_lat_i + max_range_deg
    min_lon = radar_lon_i - max_range_deg;  max_lon = radar_lon_i + max_range_deg

    print("\n🌐 Querying Socrata API for the full day's gauge records (1 Single Request)...")
    first_window_ts = parse_timestamp_from_path(windows[0][0])
    day_start = first_window_ts.replace(hour=0, minute=0, second=0)
    day_end   = first_window_ts.replace(hour=23, minute=59, second=59)

    try:
        df_all_gauges = download_data(day_start, day_end, min_lat, max_lat, min_lon, max_lon)
        if df_all_gauges is not None and not df_all_gauges.empty:
            for col in ["valorobservado", "latitud", "longitud"]:
                df_all_gauges[col] = pd.to_numeric(df_all_gauges[col], errors="coerce")
            df_all_gauges = df_all_gauges.dropna(subset=["valorobservado","latitud","longitud"])
            df_all_gauges["parsed_dt"] = pd.to_datetime(df_all_gauges["fechaobservacion"]).dt.tz_localize('UTC')
            print(f"✅ Downloaded {len(df_all_gauges)} absolute gauge points successfully.")
        else:
            print("[WARN] Socrata API returned no measurements for this entire day.")
            df_all_gauges = pd.DataFrame()
    except Exception as e:
        print(f"❌ Critical error querying global gauge dataset: {e}")
        df_all_gauges = pd.DataFrame()

    GLOBAL_DF_GAUGES = df_all_gauges

    WORKERS = 6
    args_list = [(w_idx, len(windows), window) for w_idx, window in enumerate(windows)]
    all_pairs = []
    print(f"\n🚀 Launching {WORKERS} parallel workers to process {len(windows)} windows...")

    # FIXED: Added `initializer` and `initargs` to secure global scopes inside individual workers across OS environments
    with ProcessPoolExecutor(
        max_workers=WORKERS, 
        initializer=init_worker, 
        initargs=(precomp_CBB, precomp_lon, precomp_lat, precomp_alt, df_all_gauges)
    ) as ex:
        futures = {ex.submit(process_window, a): a for a in args_list}
        
        for fut in as_completed(futures):
            try:
                matched_df = fut.result()
                if matched_df is not None and not matched_df.empty:
                    all_pairs.append(matched_df)
            except Exception as e:
                print(f"  [CRITICAL ERROR] Worker failed: {e}")

    # ─── MODIFIED METRICS: EXPORTS COMPACT CSV CONTAINING RAW PARAMETERS FOR SCI-PY ───
    if all_pairs:
        df_all   = pd.concat(all_pairs, ignore_index=True)
        csv_name = (f"2025{str(month).zfill(2)}{str(day).zfill(2)}"
                    f"_parametrization2_pairs_BAR.csv")
        df_all.to_csv(csv_name, index=False)
        print(f"\nValidation CSV → {csv_name}  ({len(df_all)} rows)")

        df_v = df_all.dropna(subset=["gauge_mm"] + ESTIMATOR_NAMES)
        df_v = df_v[df_v["gauge_mm"] >= 0]
        print(f"📈 Sync complete! Extracted {len(df_v)} valid non-averaged gauge matching pairs for regression models.")
    else:
        print("\n[WARN] No pairs collected.")

    print("\n🧹 Cleaning up daily local radar file cache...")
    if os.path.exists("./radar_cache"):
        shutil.rmtree("./radar_cache")

    print("\nDone.")
