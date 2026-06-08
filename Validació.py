import xradar as xd
import sys
import wradlib as wrl
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from cartopy import geodesic
import numpy as np
import xarray as xr
import cmap
from scipy.ndimage import (
    median_filter,
    gaussian_filter,
    generic_filter,
    uniform_filter,
    label,
    generate_binary_structure,
    binary_closing,
    binary_opening,
)
from scipy.spatial import cKDTree
from pyproj import Transformer
import rioxarray
from rioxarray.merge import merge_datasets
from collections import OrderedDict
import s3fs
import io
import seaborn as sns
import json
from scipy.interpolate import interpn
from osgeo import gdal
import pandas as pd
from osgeo import osr
from pyproj import CRS
from scipy import stats
import gc
from sodapy import Socrata
import datetime as dt
import pandas as pd

day='18'    

TOKEN = "MFHXNYLts4ZhySVUsR7emeZXO"

def download_data(date_init: dt.datetime, date_end: dt.datetime,min_lat,max_lat,min_lon,max_lon) -> pd.DataFrame:
    client = Socrata("www.datos.gov.co", TOKEN)

    date_init = pd.Timestamp(date_init).tz_localize('UTC').tz_convert('America/Bogota')
    date_end  = pd.Timestamp(date_end).tz_localize('UTC').tz_convert('America/Bogota')
    #print(date_end)

    query = client.get(
        dataset_identifier = "s54a-sgyg",
        select             = "codigoestacion, fechaobservacion, latitud, longitud, valorobservado, unidadmedida",
        where              = f"fechaobservacion >= '{date_init.strftime('%Y-%m-%d')}' AND fechaobservacion <= '{date_end.strftime('%Y-%m-%d')}'"
                             f"AND latitud > '{min_lat}' AND latitud < '{max_lat}' AND longitud > '{min_lon}' AND longitud < '{max_lon}'"
                             f"AND codigoestacion IN ('2319500125','0023197370','0027035050','0027037020','0023205020','0023180070','2319500207','0027030140','0027011100','0024065010','0023190440','002190380','002190130','0023195040','2319500043','0023195502','0023195110','0024057070','0023155030','0024050070','002345501','0024017707','0024015519','00240155514','0024015509','0024015300','0024017600','0024017590','0024035508','0024037030','2401500052','0027010850','0023175020','0023105070','0023085080','002315010','0023125080','0023147020','0023127050','0023097030','0023127020','0023127060','0023125120')", #per barrancabermeja

        limit              = 50000000,
    )

    data = pd.DataFrame.from_records(query)

    return data

# PLOTERS
###############################################################################

def plot_features(ax,labels_and_gridlines=False,c='gray',departments=True):
    if departments:
        departments = cfeature.NaturalEarthFeature(
            category='cultural',
            name='admin_1_states_provinces_lines',
            scale='10m',
            edgecolor='k',
            facecolor='never',
            linewidth=0.6,
            alpha=.6,
            linestyle='-'
        )
    ax.add_feature(departments, zorder=1)

    ax.add_feature(cfeature.COASTLINE, linewidth=0.8, edgecolor=c, alpha=.8,zorder=1)
    ax.add_feature(cfeature.BORDERS, linewidth=0.8, edgecolor=c, alpha=.8,zorder=1)
    # Optional: add gridlines with labels
    if labels_and_gridlines:
        gl = ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False)
        gl.top_labels = False
        gl.right_labels = False

# HELPERS
###############################################################################

def texture_std(x):

    valid = np.isfinite(x)

    if np.sum(valid) < 3:
        return np.nan

    return np.nanstd(x)

def compute_texture(field, size=(1, 5)):
    """
    Compute local texture of a radar field.
    """

    arr = field.values.copy()

    tex = generic_filter(
        arr,
        texture_std,
        size=size,
        mode="nearest"
    )

    return xr.DataArray(
        tex,
        dims=field.dims,
        coords=field.coords
    )

def texture_of_complex_phase(FIELD, phidp_field=None, phidp_texture_field=None):
    """
    Adapted from pyart !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    Calculate the texture of the differential phase field.

    Calculate the texture of the real part of the complex differential
    phase field

    Parameters
    ----------
    radar : Radar
        Radar object from which to .
    phidp_field : str, optional
        Name of field in radar which contains the differential phase shift.
        None will use the default field name in the Py-ART configuration file.
    phidp_texture_field : str, optional
        Name to use for the differential phase texture field metadata.
        None will use the default field name in the Py-ART configuration file.

    Returns
    -------
    texture_field : dict
        Field dictionary containing the texture of the real part
        of the complex differential phase.

    References
    ----------
    Gourley, J. J., P. Tabary, and J. Parent du Chatelet,
    A fuzzy logic algorithm for the separation of precipitating from
    nonprecipitating echoes using polarimetric radar observations,
    Journal of Atmospheric and Oceanic Technology 24 (8), 1439-1451

    """

    phidp = FIELD.values

    # convert to complex number
    complex_phase = np.exp(1j * (np.radians(phidp)))

    # calculate texture using wradlib
    w_texture_complex = wrl.util.texture((np.real(complex_phase) + 1.0) * 180)

    return w_texture_complex

def r_z(zh):
    """Calculate R(Z). 'zh' is reflectivity factor (linear, not dBZ)."""
    a, b = 0.027, 0.667
    return a * (zh**b)

def r_kdp(kdp, zdr_linear, use_zdr=False):
    """Calculate R(KDP). 'kdp' in deg/km, 'zdr_linear' is linearized ZDR."""
    if use_zdr:
        # R(KDP, ZDR) combined estimator
        return 0.851 * kdp * zdr_linear
    else:
        # R(KDP) single-parameter estimator
        a, b = 38.9, 0.837
        return a * (kdp**b)

def r_z_zdr(zh_lin, zdr_lin):
    """Calculate R(Z, ZDR). 'zh_lin' is linear Z, 'zdr_lin' is linear ZDR."""
    a, b, c = 0.021, 0.742, 0.148
    return a * (zh_lin**b) * (10**(c * zdr_lin))

def r_a(ah):
    """Calculate R(A). 'ah' is specific attenuation in dB/km."""
    a, b = 180.0, 0.7
    return a * (ah**b)

def merge_rainfall(ds, alpha):
    """
    Apply decision tree to merge R(Z), R(KDP), R(A), and R(Z, ZDR) estimators.
    
    Parameters:
        ds (xarray.Dataset): Must contain Z, ZDR, and PHIDP.
        alpha_default (float): The alpha parameter to convert KDP to A.
        
    Returns:
        xarray.DataArray: Merged rainfall rate (mm/h).
    """
    # --- Prepare data ---
    # Convert to linear units as needed
    zh_linear = 10**(ds.cDBZH / 10.0)   # Convert dBZ to linear mm^6/m^3
    zdr_linear = 10**(ds.fZDR / 10.0) # Convert dB to linear ratio
    
    # Calculate KDP from PHIDP (requires smoothing along range)
    # This is a simple placeholder; use your existing KDP calculation.
    kdp = ds.dKDP
    
    # Calculate A (specific attenuation)
    # This is a placeholder: A = alpha * KDP, with alpha derived from your ZDR-slope method.
    # In your code, you would replace 'alpha_default' with the per-sweep alpha you've calculated.
    ah = alpha * kdp
    
    # --- Apply decision thresholds ---
    # Threshold 1: KDP > 0.3 deg/km
    mask_kdp = kdp > 0.3
    
    # For gates where mask_kdp is True
    r_final = xr.where(
        mask_kdp,
        # Sub-threshold for high ZH (Z > 40 dBZ)
        xr.where(ds.cDBZH > 40, r_kdp(kdp, zdr_linear, use_zdr=True), r_kdp(kdp, zdr_linear, use_zdr=False)),
        # For gates where KDP <= 0.3
        xr.where(
            ds.fZDR > 0.25, # Threshold 2: ZDR > 0.25 dB
            r_z_zdr(zh_linear, zdr_linear),
            # For very light rain: Use R(Z) or a hybrid with R(A)
            r_z(zh_linear) # Replace with hybrid if needed
        )
    )
    
    # Ensure no negative or unrealistic values
    r_final = r_final.where(r_final >= 0, np.nan)
    #r_final = r_final.where(r_final < 250, 250) # Cap at 250 mm/h
    
    return r_final

def calc_alpha_per_sweep(ds, zh_var='DBZH', zdr_var='ZDR', rhohv_var='RHOHV',
                         band='C', min_gates=200, bin_width=2.0, min_bins=5,
                         min_gates_per_bin=10, min_zh=15, max_zh=60,
                         min_zdr=-0.5, max_zdr=6.0, min_rhohv=0.98):
    """
    Calculate one alpha (α) value for a given radar sweep using the ZDR-slope method.

    This function processes a single sweep contained in an xarray.Dataset. It filters
    out non-meteorological echoes, bins the data by reflectivity (ZH), calculates the
    median ZDR in each bin, performs a linear regression to find the ZDR slope (dZDR/dZH),
    and then converts that slope into the alpha (α = AH/KDP) coefficient.

    Parameters:
        ds (xarray.Dataset): Input dataset for one sweep. Must contain the following
                             1D variables for each ray: ZH, ZDR, RHOHV.
        zh_var (str):        Name of the reflectivity variable.
        zdr_var (str):       Name of the differential reflectivity variable.
        rhohv_var (str):     Name of the correlation coefficient variable.
        band (str):          Radar band: 'S', 'C', or 'X'. Determines the coefficients.
        min_gates (int):     Minimum number of total valid gates required for processing.
        bin_width (float):   Width of the ZH bins in dBZ.
        min_bins (int):      Minimum number of bins with data for a valid regression.
        min_gates_per_bin (int): Minimum number of gates within a ZH bin to consider it valid.
        min_zh (float):      Minimum ZH (dBZ) for a gate to be considered.
        max_zh (float):      Maximum ZH (dBZ) for a gate to be considered.
        min_zdr (float):     Minimum ZDR (dB) for a gate to be considered.
        max_zdr (float):     Maximum ZDR (dB) for a gate to be considered.
        min_rhohv (float):   Minimum RHOHV for a gate to be considered (pure rain mask).

    Returns:
        float or None: Calculated alpha value for the sweep. Returns None if data is
                       insufficient (e.g., not enough valid gates or bins).
    """

    # --- 1. Extract and Filter Data ---
    zh = ds[zh_var].values.flatten()
    zdr = ds[zdr_var].values.flatten()
    rhohv = ds[rhohv_var].values.flatten()

    # Create a mask for valid, rain-only gates
    valid_mask = (zh >= min_zh) & (zh <= max_zh) & \
                 (zdr >= min_zdr) & (zdr <= max_zdr) & \
                 (rhohv > min_rhohv) & \
                 np.isfinite(zh) & np.isfinite(zdr) & np.isfinite(rhohv)

    zh_valid = zh[valid_mask]
    zdr_valid = zdr[valid_mask]

    # Check if we have enough data points
    if len(zh_valid) < min_gates:
        print(f"Insufficient valid gates: {len(zh_valid)} < {min_gates}")
        alpha=0.01
        return alpha

    # --- 2. Bin Data by ZH and Calculate Median ZDR per Bin ---
    bin_edges = np.arange(min_zh, max_zh + bin_width, bin_width)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    median_zdr_list = []

    for i in np.arange(0,len(bin_centers)):
        # Find gates whose ZH falls into this bin
        in_bin = (zh_valid >= bin_edges[i]) & (zh_valid < bin_edges[i+1])
        zdr_in_bin = zdr_valid[in_bin]

        if len(zdr_in_bin) >= min_gates_per_bin:
            median_zdr_list.append(np.median(zdr_in_bin))
        else:
            median_zdr_list.append(np.nan) # Mark bins with insufficient data

    # Filter out bins without enough data
    x = bin_centers[~np.isnan(median_zdr_list)]
    y = np.array(median_zdr_list)[~np.isnan(median_zdr_list)]

    if len(x) < min_bins:
        print(f"Insufficient bins: {len(x)} < {min_bins}")
        alpha=0.01
        return alpha

    # --- 3. Perform Linear Regression to get the ZDR Slope (K) ---
    # Use scipy for regression to also get r-value and p-value for diagnostics
    slope, intercept, r_value, p_value, std_err = stats.linregress(x, y)
    K = slope

    # --- 4. Convert Slope K to Alpha (α) ---
    # Polynomial coefficients for different radar bands
    coefficients = {
        'S': [0.054, -1.31, 10.9],
        'C': [0.059, -1.22, 9.7],
        'X': [0.087, -1.78, 14.2]
    }

    if band.upper() not in coefficients:
        raise ValueError(f"Band '{band}' not recognized. Choose 'S', 'C', or 'X'.")

    a0, a1, a2 = coefficients[band.upper()]
    alpha = a0 + a1 * K + a2 * (K**2)

    # Apply physical bounds to alpha
    alpha = max(0.02, min(0.10, alpha))

    if alpha == None:
        alpha=0.01
        return alpha

    return alpha

METEO_TABLE = {
    0 : "No data available",
    1 : "Non-meteorological target",
    2 : "Rain",
    3 : "Wet Snow",
    4 : "Snow",
    5 : "Graupel",
    6 : "Hail",
    7 : "Unused",
}

PRECIP_TABLE = {
    0 : "No data available",
    1 : "Ground clutter / anomalous propagation",
    2 : "Bio scatter",
    3 : "Precipitation",
    4 : "Large drops",
    5 : "Light precipitation",
    6 : "Moderate precipitation",
    7 : "Heavy precipitation",
}

CELL_TABLE = {
    0 : "Stratiform",
    1 : "Convection",
    2 : "Unused",
    3 : "Forbidden",
}

def decode_hclass(byte_val: int) -> tuple:
    if byte_val == 0:  # Reserved value for 'No Data'
        return (-2, -2, -2)
    if byte_val == 255:  # Reserved value for 'Area not scanned'
        return (np.nan, np.nan, np.nan)

    meteo_code = (byte_val >> 5) & 0x07
    precip_code = (byte_val >> 2) & 0x07
    cell_code = byte_val & 0x03

    return (meteo_code, precip_code, cell_code)

def decode_hclass_vect(raw_data: np.ndarray) -> np.ndarray:
    """Vectorized wrapper to decode an entire field."""
    return np.vectorize(decode_hclass, otypes=[object])(raw_data)

def get_(i, da):
    get = np.vectorize(lambda t: t[i])
    return xr.apply_ufunc(get, da)
#----------------------------------------------------------------------------------------

def open_iris_odict(filepath,load_data : bool = True, rawdata : bool =False, debug : bool =False):
    """
    Aquest funció només té raó d'ésser si wrl.io.iris.read_iris() no és capaç de decodificar la variable hclass.
    hclass conté un byte (8bits) que guarda la informació relativa a 3 classificadors
    """
    
    data_odict = wrl.io.iris.read_iris(
        filename=filepath,
        load=load_data,
        rawdata=rawdata,
        debug=debug,
    )
    return data_odict

def open_iris_dtree(filepath, decode_hclass : bool = True):
    """
    Aquest funció només té raó d'ésser si open_iris_datatree() no és capaç de decodificar la variable hclass.
    hclass conté un byte (8bits) que guarda la informació relativa a 3 classificadors
    """
    dt = xd.io.open_iris_datatree(filepath)
    
    if decode_hclass:
        dt["/sweep_0"]["DB_HCLASS"].values = decode_hclass_vect(dt["/sweep_0"]["DB_HCLASS"].values)
        dt["/sweep_0"]["DB_HCLASS_meteor"] = get_(0, dt["/sweep_0"]["DB_HCLASS"])
        dt["/sweep_0"]["DB_HCLASS_precip"] = get_(1, dt["/sweep_0"]["DB_HCLASS"])
        dt["/sweep_0"]["DB_HCLASS_storm"]  = get_(2, dt["/sweep_0"]["DB_HCLASS"])

    return dt

fs = s3fs.S3FileSystem(anon=True) 

with open(f'metadata/2025/05/{day}.json', 'r') as f:
    data = json.load(f)

# Fix get_stacks to avoid TypeError
def get_stacks(json_data):
    all_stacks = []
    current_stack = []
    last_sweep_elangle = float('inf')

    for station, station_data in json_data.items():
        for key, value in station_data.items():
            sweep_elangle = value['sweeps'][list(value['sweeps'].keys())[0]]['elevation_angle']
            current_sweep_elangle = float(sweep_elangle)

            if current_sweep_elangle <= last_sweep_elangle:
                if current_stack:
                    all_stacks.append(current_stack)
                current_stack = []

            current_stack.append(value['filepath'])
            last_sweep_elangle = float(value['sweeps'][list(value['sweeps'].keys())[-1]]['elevation_angle'])

        if current_stack:
            all_stacks.append(current_stack)

    return all_stacks

stacks = get_stacks(data)

lowest_ppi = []
for volume in stacks:
    lowest_ppi.append(volume[0])

lowest_ppi = np.array(lowest_ppi)

output_list = [e for e in lowest_ppi if "Barrancabermeja" in e.split("/")]

filename = "Barrancabermeja.tif"

dem = wrl.io.open_raster(filename)

rastervalues, rastercoords, crs = wrl.georef.extract_raster_dataset(dem, nodata=-32768.0)

def R_per_sweep(s3_path):

    bytes_en_memoria = io.BytesIO(fs.cat(s3_path))
    dtree = open_iris_dtree(bytes_en_memoria, decode_hclass=False)
    swp = dtree["/sweep_0"]

    radar_alt = float(dtree["/"]["altitude"].values)
    radar_lon = float(dtree["/"]["longitude"].values)
    radar_lat = float(dtree["/"]["latitude"].values)

    raster_lon = rastercoords[..., 0]
    raster_lat = rastercoords[..., 1]
    distancies = (raster_lon - radar_lon)**2 + (raster_lat - radar_lat)**2
    idx_minim = np.unravel_index(np.argmin(distancies), distancies.shape)
    elevacio_radar_dsm = rastervalues[idx_minim]

    if elevacio_radar_dsm >= radar_alt:
        radar_alt = float(dtree["/"]["altitude"].values)+35

    site = (radar_lon, radar_lat, radar_alt)
    azimuth   = swp["azimuth"].data
    range     = swp["range"].data
    elangle   = swp["elevation"].data
    nrays = azimuth.shape[0]
    nbins = range.shape[0]
    el    = swp["sweep_fixed_angle"].values

    r_scale = range[1]-range[0]
    coord = wrl.georef.sweep_centroids(nrays, r_scale, nbins, el)
    coords = wrl.georef.spherical_to_proj(
        coord[..., 0], coord[..., 1], coord[..., 2], site
    )
    lon = coords[..., 0]
    lat = coords[..., 1]
    alt = coords[..., 2]
    swp.coords["gate_latitude"] = (('azimuth', 'range'), lat)
    swp.coords["gate_longitude"] = (('azimuth', 'range'), lon)
    swp.coords["gate_height"] = (('azimuth', 'range'), alt)
    polcoords = coords[..., :2]

    texture_thresh=30
    zdr_thresh=7
    dr_thresh=-12.0

    DBZH = swp["DBZH"]
    PHIDP = swp["PHIDP"]
    KDP = swp["KDP"]
    RHOHV = swp["RHOHV"]
    ZDR = swp["ZDR"]

    polarvalues = wrl.ipol.map_coordinates(
        rastercoords, rastervalues, polcoords, order=3, prefilter=False
    )

    beamwidth = 1.0
    r_distances = swp.coords["range"].values
    beamradius = wrl.util.half_power_radius(r_distances, beamwidth)
    PBB = wrl.qual.beam_block_frac(polarvalues, alt, beamradius)
    PBB = np.ma.masked_invalid(PBB)

    CBB = wrl.qual.cum_beam_block_frac(PBB)

    cbb = np.ma.filled(CBB, fill_value=0.0)
    swp["CBB"] = xr.DataArray(
        cbb, 
        dims=["azimuth", "range"],
        coords={
            "azimuth": swp["azimuth"],
            "range": swp["range"]
        }
    )
    #swp["CBB"].attrs["long_name"] = "Cumulative Beam Blockage Fraction"
    #swp["CBB"].attrs["units"] = "1"
    #swp["CBB"].attrs["comment"] = "Calculat mitjançant fusió estricta de dos DEMs USGS corregits per a la llanura del Carib."

    dbzh = DBZH.values
    cbb = swp["CBB"].values
    dbzh_corr = np.copy(dbzh)
    mask_comp = (cbb >= 0.1) & (cbb <= 0.5)
    dbzh_corr[mask_comp] = dbzh_corr[mask_comp] - 10.0 * np.log10(1.0 - cbb[mask_comp])
    mask_elim = cbb > 0.5
    dbzh_corr[mask_elim] = np.nan
    DBZH_terrain_corr = xr.DataArray(dbzh_corr, dims=["azimuth", "range"])

    dbzh = DBZH_terrain_corr.values
    zdr_db = ZDR.values 
    rhohv = RHOHV.values
    zdr_linear = 10.0 ** (zdr_db / 10.0)
    zdr_sqrt_linear = 10.0 ** (zdr_db / 20.0)
    numerador = zdr_linear + 1.0 - (2.0 * zdr_sqrt_linear * rhohv)
    denominador = zdr_linear + 1.0 + (2.0 * zdr_sqrt_linear * rhohv)
    with np.errstate(divide='ignore', invalid='ignore'):
        dr_linear = numerador / denominador
        dr_db = 10.0 * np.log10(dr_linear)
    llindar_tall = -12.0
    mascara_no_meteo = (dr_db < llindar_tall) & (dbzh < 30)
    swp["DR"] = xr.DataArray(dr_db, dims=["azimuth","range"])
    swp["DR"].attrs["long_name"] = "Dual-Polarization Depolarization Ratio"
    swp["DR"].attrs["units"] = "dB"

    DR = swp["DR"].fillna(0.0)
    CBB = swp["CBB"].fillna(0.0)

    textura_phidp = texture_of_complex_phase(PHIDP)
    tPHIDP = xr.DataArray(
        textura_phidp,
        dims=PHIDP.dims,
        coords=PHIDP.coords
    )
    #swp["tPHIDP"] = tPHIDP

    no_met_mask = (
          ((DR > dr_thresh) & (DBZH < 35.0))
        | ((tPHIDP > texture_thresh) & (DBZH < 30.0))
        | (DBZH <= 5)
        | (CBB == 1.0)
    )
    raw_clutter_flags = no_met_mask.values
    pad = 1
    met_mask = ~raw_clutter_flags
    met_mask_padded = np.pad(met_mask, pad_width=((pad, pad), (0, 0)), mode='wrap')
    structure = generate_binary_structure(rank=2, connectivity=1)
    clean_clutter_mask = binary_opening(met_mask_padded, structure=structure, iterations=1)
    clean_clutter_mask = binary_closing(clean_clutter_mask, structure=structure, iterations=1)
    met_mask_vals = clean_clutter_mask[pad:-pad,:]
    met_mask = xr.DataArray(
        met_mask_vals,
        dims=swp.DBZH.dims,
        coords=swp.DBZH.coords,
        attrs={
            "long_name": "Consolidated Meteorological Quality Mask",
            "description": "Binary mask generated via multi-variable thresholds and refined with a Close-Open morphological sequence."
        }
    )
    swp["met_mask"] = met_mask

    no_met_mask_no_texture = (
          ((DR > dr_thresh) & (DBZH < 35.0))
        | (DBZH <= 5)
        | (CBB == 1.0)
    )
    raw_clutter_flags_no_texture = no_met_mask_no_texture.values
    pad = 1
    met_mask_no_texture = ~raw_clutter_flags_no_texture
    met_mask_padded_no_texture = np.pad(met_mask_no_texture, pad_width=((pad, pad), (0, 0)), mode='wrap')
    structure = generate_binary_structure(rank=2, connectivity=1)
    clean_clutter_mask_no_texture = binary_opening(met_mask_padded_no_texture, structure=structure, iterations=1)
    clean_clutter_mask_no_texture = binary_closing(clean_clutter_mask_no_texture, structure=structure, iterations=1)
    met_mask_vals_no_texture = clean_clutter_mask_no_texture[pad:-pad,:]
    met_mask_no_texture = xr.DataArray(
        met_mask_vals_no_texture,
        dims=swp.DBZH.dims,
        coords=swp.DBZH.coords,
        attrs={
            "long_name": "Consolidated Meteorological Quality Mask",
            "description": "Binary mask generated via multi-variable thresholds and refined with a Close-Open morphological sequence."
        }
    )
    swp["met_mask_no_texture"] = met_mask_no_texture

    r_metres = swp.coords["range"].values
    resolucio_metres = r_metres[1] - r_metres[0]
    dr_km = resolucio_metres / 1000.0
    ds = swp.ds.copy()
    ds["PHIDP"] = ds["PHIDP"].where(met_mask)
    vulpani_phidp, vulpani_kdp = wrl.dp.phidp_kdp_vulpiani(
        ds["PHIDP"].values, 
        dr=dr_km,
        ndespeckle=5,   
        winlen=15,      
        niter=3,         
    )
    UNFOLD_PHIDP = xr.DataArray((vulpani_phidp), dims=ds.PHIDP.dims, coords=ds.PHIDP.coords)
    #swp["uKDP"] = xr.DataArray(vulpani_kdp, dims=ds.PHIDP.dims, coords=ds.PHIDP.coords)
    #swp["uPHIDP"] = UNFOLD_PHIDP

    fDBZH = xr.where(
        met_mask_no_texture,
        DBZH_terrain_corr,
        np.nan
    )
    max_interpolation_gap = 8
    fDBZH = fDBZH.interpolate_na(
        dim="range", 
        method="linear", 
        max_gap=max_interpolation_gap
    )
    fZDR = xr.where(
        met_mask_no_texture,
        ZDR,
        np.nan
    )
    fZDR = fZDR.interpolate_na(
        dim="range", 
        method="linear", 
        max_gap=max_interpolation_gap
    )
    swp["fZDR"] = fZDR
    fPHIDP = xr.where(
        met_mask,
        UNFOLD_PHIDP,
        np.nan
    )
    #swp["fPHIDP"] = fPHIDP

    phidp_inicial = fPHIDP.copy()
    vals_mediana = median_filter(
        phidp_inicial.values,
        size=(1, 5)
    )
    phidp_mediana = xr.DataArray(
        vals_mediana,
        dims=phidp_inicial.dims,
        coords=phidp_inicial.coords
    )
    sPHIDP = phidp_mediana.rolling(
        range=25,
        center=True,
        min_periods=1
    ).mean()
    sPHIDP_clean = sPHIDP.fillna(0.0)

    vals = np.copy(sPHIDP_clean.values)
    for az in np.arange(vals.shape[0]):
        ray = vals[az]
        for i in np.arange(1, len(ray)):
            if np.isfinite(ray[i-1]) and np.isfinite(ray[i]):
                if ray[i] < ray[i-1]:
                    ray[i] = ray[i-1]
        vals[az] = ray
    vals = median_filter(vals, size=(1, 4))
    vals = median_filter(vals, size=(1, 8))
    vals_with_nan = np.copy(vals)
    vals_with_nan[~met_mask.values] = np.nan

    sPHIDP = xr.DataArray(
        vals_with_nan,
        dims=fPHIDP.dims,
        coords=fPHIDP.coords
    )

    cKDP = xr.where(
        met_mask,
        KDP,
        np.nan
    )

    r_metres = swp.coords["range"].values
    resolucio_metres = r_metres[1] - r_metres[0]
    dr_km = resolucio_metres / 1000.0
    dKDP_calculat = wrl.dp.kdp_from_phidp(
        sPHIDP.values, 
        winlen=7,
        dr=dr_km,
        method="lanczos_conv"
    )
    dKDP_net = np.where(dKDP_calculat < 0.0, 0.0, dKDP_calculat)
    dKDP_net = median_filter(dKDP_net, size=(1, 3))
    dKDP_final = np.copy(dKDP_net)
    dKDP_final[~met_mask.values] = np.nan
    dKDP = xr.DataArray(
        dKDP_final,
        dims=cKDP.dims,
        coords=cKDP.coords,
    )
    swp["dKDP"] = dKDP

    alpha = calc_alpha_per_sweep(swp)

    A = alpha * dKDP
    dr = float(
        swp["range"][1] - swp["range"][0]
    ) 
    dr_km = dr / 1000.0
    pia_vals = 2 * np.nancumsum(
        A.values * dr_km,
        axis=1
    )
    PIA = xr.DataArray(
        pia_vals,
        dims=A.dims,
        coords=A.coords
    )
    cDBZH_attcorr = fDBZH + PIA
    swp["cDBZH"] = cDBZH_attcorr

    swp["R"] = merge_rainfall(swp,alpha)
    R = swp["R"].values.copy()

    del swp
    del dtree
    del bytes_en_memoria
    gc.collect()

    return R

def R_per_sweep_1st(s3_path):

    bytes_en_memoria = io.BytesIO(fs.cat(s3_path))
    dtree = open_iris_dtree(bytes_en_memoria, decode_hclass=False)
    swp = dtree["/sweep_0"]

    radar_alt = float(dtree["/"]["altitude"].values)
    radar_lon = float(dtree["/"]["longitude"].values)
    radar_lat = float(dtree["/"]["latitude"].values)

    raster_lon = rastercoords[..., 0]
    raster_lat = rastercoords[..., 1]
    distancies = (raster_lon - radar_lon)**2 + (raster_lat - radar_lat)**2
    idx_minim = np.unravel_index(np.argmin(distancies), distancies.shape)
    elevacio_radar_dsm = rastervalues[idx_minim]

    if elevacio_radar_dsm >= radar_alt:
        radar_alt = float(dtree["/"]["altitude"].values)+35

    site = (radar_lon, radar_lat, radar_alt)
    azimuth   = swp["azimuth"].data
    range     = swp["range"].data
    elangle   = swp["elevation"].data
    nrays = azimuth.shape[0]
    nbins = range.shape[0]
    el    = swp["sweep_fixed_angle"].values

    r_scale = range[1]-range[0]
    coord = wrl.georef.sweep_centroids(nrays, r_scale, nbins, el)
    coords = wrl.georef.spherical_to_proj(
        coord[..., 0], coord[..., 1], coord[..., 2], site
    )
    lon = coords[..., 0]
    lat = coords[..., 1]
    alt = coords[..., 2]
    swp.coords["gate_latitude"] = (('azimuth', 'range'), lat)
    swp.coords["gate_longitude"] = (('azimuth', 'range'), lon)
    swp.coords["gate_height"] = (('azimuth', 'range'), alt)
    polcoords = coords[..., :2]

    texture_thresh=30
    zdr_thresh=7
    dr_thresh=-12.0

    DBZH = swp["DBZH"]
    PHIDP = swp["PHIDP"]
    KDP = swp["KDP"]
    RHOHV = swp["RHOHV"]
    ZDR = swp["ZDR"]

    #gate_latitude  = swp.coords["gate_latitude"].values
    #gate_longitude = swp.coords["gate_longitude"].values
    #gate_height    = swp.coords["gate_height"].values

    polarvalues = wrl.ipol.map_coordinates(
        rastercoords, rastervalues, polcoords, order=3, prefilter=False
    )

    beamwidth = 1.0
    r_distances = swp.coords["range"].values
    beamradius = wrl.util.half_power_radius(r_distances, beamwidth)
    PBB = wrl.qual.beam_block_frac(polarvalues, alt, beamradius)
    PBB = np.ma.masked_invalid(PBB)

    CBB = wrl.qual.cum_beam_block_frac(PBB)

    cbb = np.ma.filled(CBB, fill_value=0.0)
    swp["CBB"] = xr.DataArray(
        cbb, 
        dims=["azimuth", "range"],
        coords={
            "azimuth": swp["azimuth"],
            "range": swp["range"]
        }
    )
    #swp["CBB"].attrs["long_name"] = "Cumulative Beam Blockage Fraction"
    #swp["CBB"].attrs["units"] = "1"
    #swp["CBB"].attrs["comment"] = "Calculat mitjançant fusió estricta de dos DEMs USGS corregits per a la llanura del Carib."

    dbzh = DBZH.values
    cbb = swp["CBB"].values
    dbzh_corr = np.copy(dbzh)
    mask_comp = (cbb >= 0.1) & (cbb <= 0.5)
    dbzh_corr[mask_comp] = dbzh_corr[mask_comp] - 10.0 * np.log10(1.0 - cbb[mask_comp])
    mask_elim = cbb > 0.5
    dbzh_corr[mask_elim] = np.nan
    DBZH_terrain_corr = xr.DataArray(dbzh_corr, dims=["azimuth", "range"])

    dbzh = DBZH_terrain_corr.values
    zdr_db = ZDR.values 
    rhohv = RHOHV.values
    zdr_linear = 10.0 ** (zdr_db / 10.0)
    zdr_sqrt_linear = 10.0 ** (zdr_db / 20.0)
    numerador = zdr_linear + 1.0 - (2.0 * zdr_sqrt_linear * rhohv)
    denominador = zdr_linear + 1.0 + (2.0 * zdr_sqrt_linear * rhohv)
    with np.errstate(divide='ignore', invalid='ignore'):
        dr_linear = numerador / denominador
        dr_db = 10.0 * np.log10(dr_linear)
    llindar_tall = -12.0
    mascara_no_meteo = (dr_db < llindar_tall) & (dbzh < 30)
    swp["DR"] = xr.DataArray(dr_db, dims=["azimuth","range"])
    swp["DR"].attrs["long_name"] = "Dual-Polarization Depolarization Ratio"
    swp["DR"].attrs["units"] = "dB"

    DR = swp["DR"].fillna(0.0)
    CBB = swp["CBB"].fillna(0.0)

    textura_phidp = texture_of_complex_phase(PHIDP)
    tPHIDP = xr.DataArray(
        textura_phidp,
        dims=PHIDP.dims,
        coords=PHIDP.coords
    )
    #swp["tPHIDP"] = tPHIDP

    no_met_mask = (
          ((DR > dr_thresh) & (DBZH < 35.0))
        | ((tPHIDP > texture_thresh) & (DBZH < 30.0))
        | (DBZH <= 5)
        | (CBB == 1.0)
    )
    raw_clutter_flags = no_met_mask.values
    pad = 1
    met_mask = ~raw_clutter_flags
    met_mask_padded = np.pad(met_mask, pad_width=((pad, pad), (0, 0)), mode='wrap')
    structure = generate_binary_structure(rank=2, connectivity=1)
    clean_clutter_mask = binary_opening(met_mask_padded, structure=structure, iterations=1)
    clean_clutter_mask = binary_closing(clean_clutter_mask, structure=structure, iterations=1)
    met_mask_vals = clean_clutter_mask[pad:-pad,:]
    met_mask = xr.DataArray(
        met_mask_vals,
        dims=swp.DBZH.dims,
        coords=swp.DBZH.coords,
        attrs={
            "long_name": "Consolidated Meteorological Quality Mask",
            "description": "Binary mask generated via multi-variable thresholds and refined with a Close-Open morphological sequence."
        }
    )
    swp["met_mask"] = met_mask

    no_met_mask_no_texture = (
          ((DR > dr_thresh) & (DBZH < 35.0))
        | (DBZH <= 5)
        | (CBB == 1.0)
    )
    raw_clutter_flags_no_texture = no_met_mask_no_texture.values
    pad = 1
    met_mask_no_texture = ~raw_clutter_flags_no_texture
    met_mask_padded_no_texture = np.pad(met_mask_no_texture, pad_width=((pad, pad), (0, 0)), mode='wrap')
    structure = generate_binary_structure(rank=2, connectivity=1)
    clean_clutter_mask_no_texture = binary_opening(met_mask_padded_no_texture, structure=structure, iterations=1)
    clean_clutter_mask_no_texture = binary_closing(clean_clutter_mask_no_texture, structure=structure, iterations=1)
    met_mask_vals_no_texture = clean_clutter_mask_no_texture[pad:-pad,:]
    met_mask_no_texture = xr.DataArray(
        met_mask_vals_no_texture,
        dims=swp.DBZH.dims,
        coords=swp.DBZH.coords,
        attrs={
            "long_name": "Consolidated Meteorological Quality Mask",
            "description": "Binary mask generated via multi-variable thresholds and refined with a Close-Open morphological sequence."
        }
    )
    swp["met_mask_no_texture"] = met_mask_no_texture

    r_metres = swp.coords["range"].values
    resolucio_metres = r_metres[1] - r_metres[0]
    dr_km = resolucio_metres / 1000.0
    ds = swp.ds.copy()
    ds["PHIDP"] = ds["PHIDP"].where(met_mask)
    vulpani_phidp, vulpani_kdp = wrl.dp.phidp_kdp_vulpiani(
        ds["PHIDP"].values, 
        dr=dr_km,
        ndespeckle=5,   
        winlen=15,      
        niter=3,         
    )
    UNFOLD_PHIDP = xr.DataArray((vulpani_phidp), dims=ds.PHIDP.dims, coords=ds.PHIDP.coords)
    #swp["uKDP"] = xr.DataArray(vulpani_kdp, dims=ds.PHIDP.dims, coords=ds.PHIDP.coords)
    #swp["uPHIDP"] = UNFOLD_PHIDP

    fDBZH = xr.where(
        met_mask_no_texture,
        DBZH_terrain_corr,
        np.nan
    )
    max_interpolation_gap = 8
    fDBZH = fDBZH.interpolate_na(
        dim="range", 
        method="linear", 
        max_gap=max_interpolation_gap
    )
    fZDR = xr.where(
        met_mask_no_texture,
        ZDR,
        np.nan
    )
    fZDR = fZDR.interpolate_na(
        dim="range", 
        method="linear", 
        max_gap=max_interpolation_gap
    )
    swp["fZDR"] = fZDR
    fPHIDP = xr.where(
        met_mask,
        UNFOLD_PHIDP,
        np.nan
    )
    #swp["fPHIDP"] = fPHIDP

    phidp_inicial = fPHIDP.copy()
    vals_mediana = median_filter(
        phidp_inicial.values,
        size=(1, 5)
    )
    phidp_mediana = xr.DataArray(
        vals_mediana,
        dims=phidp_inicial.dims,
        coords=phidp_inicial.coords
    )
    sPHIDP = phidp_mediana.rolling(
        range=25,
        center=True,
        min_periods=1
    ).mean()
    sPHIDP_clean = sPHIDP.fillna(0.0)

    vals = np.copy(sPHIDP_clean.values)
    for az in np.arange(vals.shape[0]):
        ray = vals[az]
        for i in np.arange(1, len(ray)):
            if np.isfinite(ray[i-1]) and np.isfinite(ray[i]):
                if ray[i] < ray[i-1]:
                    ray[i] = ray[i-1]
        vals[az] = ray
    vals = median_filter(vals, size=(1, 4))
    vals = median_filter(vals, size=(1, 8))
    vals_with_nan = np.copy(vals)
    vals_with_nan[~met_mask.values] = np.nan

    sPHIDP = xr.DataArray(
        vals_with_nan,
        dims=fPHIDP.dims,
        coords=fPHIDP.coords
    )

    cKDP = xr.where(
        met_mask,
        KDP,
        np.nan
    )

    r_metres = swp.coords["range"].values
    resolucio_metres = r_metres[1] - r_metres[0]
    dr_km = resolucio_metres / 1000.0
    dKDP_calculat = wrl.dp.kdp_from_phidp(
        sPHIDP.values, 
        winlen=7,
        dr=dr_km,
        method="lanczos_conv"
    )
    dKDP_net = np.where(dKDP_calculat < 0.0, 0.0, dKDP_calculat)
    dKDP_net = median_filter(dKDP_net, size=(1, 3))
    dKDP_final = np.copy(dKDP_net)
    dKDP_final[~met_mask.values] = np.nan
    dKDP = xr.DataArray(
        dKDP_final,
        dims=cKDP.dims,
        coords=cKDP.coords,
    )
    swp["dKDP"] = dKDP

    alpha = calc_alpha_per_sweep(swp)

    A = alpha * dKDP
    dr = float(
        swp["range"][1] - swp["range"][0]
    ) 
    dr_km = dr / 1000.0
    pia_vals = 2 * np.nancumsum(
        A.values * dr_km,
        axis=1
    )
    PIA = xr.DataArray(
        pia_vals,
        dims=A.dims,
        coords=A.coords
    )
    cDBZH_attcorr = fDBZH + PIA
    swp["cDBZH"] = cDBZH_attcorr

    swp["R"] = merge_rainfall(swp,alpha)

    return swp["R"],swp,dtree,site
#----------------------------------------------------------------------------------

R,swp,dtree,site = R_per_sweep_1st(output_list[0])

acc_R = np.zeros_like(R.values, dtype=np.float32)
acc_R += np.nan_to_num(R.values, nan=0.0) * (5/60)

for s3_path in output_list[1:]:

    R = R_per_sweep(s3_path)
    acc_R += np.nan_to_num(R, nan=0.0) * (5/60)


da = xr.DataArray(
    acc_R,
    dims=swp["DBZH"].dims,
    coords=swp["DBZH"].coords,
    attrs=swp["DBZH"].attrs
)

da.attrs["sweep_mode"] = swp["sweep_mode"].values
da.coords["longitude"] = dtree["longitude"].values
da.coords["latitude"]  = dtree["latitude"].values
da.coords["altitude"]  = dtree["altitude"].values

da.to_netcdf(f"202505{day}_acc_barrancabermeja.nc")
###------------------------------------------------------------------------------------

#gauge_R = []
#R = []
#for day in np.arange(1,11):
#    print(day)
#    da = xr.open_dataarray(f"2025050{day}_acc_barrancabermeja.nc",engine="netcdf4").sel(range=slice(None,150e3))
#    da = da.where(da != 0)
#
#    print(da)
#
#    lat = da.latitude.values
#    lon = da.longitude.values
#    max_range = 150/111
#    max_lat = lat + max_range
#    max_lon = lon + max_range
#    min_lat = lat - max_range
#    min_lon = lon - max_range
#
#    start = dt.datetime(2025,5,day+1)
#    print("start:", start)
#    end = dt.datetime(2025,5,day+2)
#    print("end:", end)
#    df = download_data(start,end,min_lat,max_lat,min_lon,max_lon) # a dia 12/05/2026 no funciona del tot correctament i has de posar un dia més perquè et retorni el dia que pertoca
#    print(df)
#    df["fechaobservacion"] = (
#        pd.to_datetime(df["fechaobservacion"])
#        .dt.tz_localize("America/Bogota")
#        .dt.tz_convert("UTC")
#    )
#    print(df)
#    df["valorobservado"] = pd.to_numeric(df["valorobservado"], errors="coerce")
#    df = df.dropna(subset=["valorobservado"])
#    df["latitud"] = pd.to_numeric(df["latitud"], errors="coerce")
#    df =df.dropna(subset=["latitud"])
#    df["longitud"] = pd.to_numeric(df["longitud"], errors="coerce")
#    df = df.dropna(subset=["longitud"])
#
#    hourly_geo_df = df.groupby([
#        'codigoestacion', 
#        'latitud', 
#        'longitud',
#    ])['valorobservado'].sum().reset_index()
#    diary_geo_df = hourly_geo_df.rename(columns={'valorobservado': 'acumulado_diario'})
#    print(diary_geo_df)
#
#    ranges = [10000, 50000, 100000, 150000]
#    number_of_colors=72
#
#    site= (da.longitude.values,
#           da.latitude.values,
#           da.altitude.values)
#    
#    gate_lon = da["gate_longitude"].values
#    gate_lat = da["gate_latitude"].values
#
#    gate_points = np.column_stack([gate_lon.ravel(), gate_lat.ravel()])
#    tree = cKDTree(gate_points)
#
#    gauge_coords = diary_geo_df[['longitud', 'latitud']].values
#    distances, indices = tree.query(gauge_coords, k=1)
#
#    # extend gauge_R with scalar values so gauge_R is a flat list matching R
#    gauge_R.extend(diary_geo_df['acumulado_diario'].values.tolist())
#
#    # ensure indices is 1D and iterate, converting radar values to scalar floats
#    indices = np.array(indices).ravel()
#    for idx in indices:
#        az_idx, rng_idx = np.unravel_index(int(idx), gate_lon.shape)
#        val_arr = np.asarray(da.isel(azimuth=int(az_idx), range=int(rng_idx)).values).ravel()
#        val = float(val_arr[0]) if val_arr.size > 0 else np.nan
#        R.append(val)

##da_geo = da.wrl.georef.georeference()
##fig = plt.figure(figsize=(20,10))       
##ax = fig.add_subplot(121, projection=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values))
##
##ax.set_facecolor('xkcd:light gray') 
##
##plot_features(ax)
##da_geo.plot.pcolormesh(
##    x="x",
##    y="y",
##    ax=ax,
##    vmin=0,
##    vmax=200,
##    cmap=cmap.Colormap("ncar").to_mpl(number_of_colors),
##    transform=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values),
##    add_colorbar=True,
##)
##
##zeros = diary_geo_df[diary_geo_df["acumulado_diario"] == 0]
##nonzeros = diary_geo_df[diary_geo_df["acumulado_diario"] > 0]
##
##sc = ax.scatter(
##    nonzeros["longitud"],
##    nonzeros["latitud"],
##    c=nonzeros["acumulado_diario"],
##    vmin=0,
##    vmax=200,
##    cmap=cmap.Colormap("ncar").to_mpl(number_of_colors),
##    s=50,
##    edgecolors="red",
##    linewidth=0.9,
##    alpha=0.9,
##    transform=ccrs.PlateCarree(),
##    zorder=1,
##)
##
##ax.scatter(
##    zeros["longitud"],
##    zeros["latitud"],
##    facecolors="none",
##    edgecolors="black",
##    s=20,
##    linewidth=0.5,
##    alpha=0.7,
##    transform=ccrs.PlateCarree(),
##    zorder=2,
##)
##
##cb = plt.colorbar(sc,ax=ax,extend="max")
##
##ax.set_extent([min_lon, max_lon, min_lat, max_lat], crs=ccrs.PlateCarree())
##
##proj_crs = ccrs.AzimuthalEquidistant(
##    central_longitude=da.longitude.values, 
##    central_latitude=da.latitude.values
##)
##
##wrl.vis.plot_ppi_crosshair(
##    site=site,
##    ranges=ranges,
##    line={"color": "None"},
##    circle={"edgecolor": "black", "linewidth" : 1, "linestyle" : "-"},
##    ax=ax,
##    crs=proj_crs,
##)
##
### 3. Add the text labels
### Choose an angle to place the text (e.g., 45 degrees, top-right quadrant)
##angle_deg = 45 
##angle_rad = np.radians(angle_deg)
##
##for r in ranges:
##    # Calculate x and y coordinates in meters from the center
##    # In meteorology, 0 degrees is usually North. 
##    # So x = r * sin(angle), y = r * cos(angle)
##    x = r * np.sin(angle_rad)
##    y = r * np.cos(angle_rad)
##    
##    # Format the label to km for readability (e.g., "12.5 km")
##    label_text = f"{r / 1000:g} km"
##    
##    # Plot the text
##    ax.text(
##        x, y, label_text,
##        transform=proj_crs,         # Ensures it maps to your cartopy projection correctly
##        fontsize=9,
##        ha='center',                # Horizontally center the text on the coordinate
##        va='center',                # Vertically center the text on the coordinate
##        color='black',
##        # Adding a small, semi-transparent white box behind the text makes it 
##        # readable even if it overlaps with intense radar MLes.
##        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1.5) 
##    )
####print(diary_geo_df["longitud"].to_numpy())
####print(type(diary_geo_df["codigoestacion"].to_numpy()[0]))
####
##### annotate each station individually (matplotlib expects scalar x, y, text)
####for _, row in diary_geo_df.iterrows():
####    ax.text(
####        row["longitud"],
####        row["latitud"],
####        str(row["codigoestacion"]),
####        transform=ccrs.PlateCarree(),
####        fontsize=8,
####        ha='left',
####        va='bottom',
####        color='black',
####        bbox=dict(facecolor='white', alpha=0.5, edgecolor='none', pad=1)
####    )
#####........................................... 

#fig = plt.figure(figsize=(20,10))
#ax2 = fig.add_subplot(111)
#
#ax2.scatter(gauge_R, R)
#ax2.plot([-20,60],[-20,60])
#ax2.set_xscale('log')
#ax2.set_yscale('log')
#
#plt.tight_layout()
#plt.show()###