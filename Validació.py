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
from scipy.stats import pearsonr
from sklearn.linear_model import LinearRegression
from concurrent.futures import ProcessPoolExecutor, as_completed
import warnings
from numpy.lib.stride_tricks import sliding_window_view

warnings.filterwarnings("ignore")

day=sys.argv[1]   
month=sys.argv[2]

TOKEN = "MFHXNYLts4ZhySVUsR7emeZXO"

def download_data(date_init: dt.datetime, date_end: dt.datetime,min_lat,max_lat,min_lon,max_lon) -> pd.DataFrame:
    client = Socrata("www.datos.gov.co", TOKEN)

    #date_init = pd.Timestamp(date_init).tz_localize('UTC').tz_convert('America/Bogota')
    #date_end  = pd.Timestamp(date_end).tz_localize('UTC').tz_convert('America/Bogota')
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

def compute_texture(field, size=(1, 3)):
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

    #phidp = FIELD.values

    # convert to complex number
    #complex_phase = np.exp(1j * (np.radians(phidp)))

    # calculate texture using wradlib
    #w_texture_complex = wrl.util.texture((np.real(complex_phase) + 1.0) * 180)
    complex_phase = np.exp(1j * (np.radians(FIELD)))
    w_texture_complex = compute_texture((np.real(complex_phase) + 1.0) * 180/2.0)

    return w_texture_complex


# =====================================================================
# 1. ESTIMADORS QPE CORREGITS I CONFIGURATS PER A COLÒMBIA (BANDA C)
# =====================================================================

#def r_z(zh):
#    """
#    R(Z) - Ajustat per a Pluja Estratiforme Tropical a Colòmbia.
#    Equació física: Z = 140 * R^1.45  ->  R = (Z/140)^(1/1.45)
#    'zh' ha d'entrar en unitats lineals (mm^6/m^3).
#    """
#    return (zh / 140.0) ** (1.0 / 1.45)
#
#
#def r_z_rosenfeld(zh):
#    """
#    R(Z) Convectiu - Relació de Rosenfeld per a Convecció Tropical.
#    Equació física: Z = 250 * R^1.2  ->  R = (Z/250)^(1/1.2)
#    'zh' ha d'entrar en unitats lineals (mm^6/m^3).
#    """
#    return (zh / 250.0) ** (1.0 / 1.2)
#
#
#def r_kdp(kdp, zdr_db=None, use_zdr=False):
#    """
#    Estimadors basats en Fase Diferencial per a la Banda C de Colòmbia.
#    - use_zdr=False: R(KDP) pur  -> R = 29.0 * KDP^0.85
#    - use_zdr=True:  R(KDP, ZDR) -> R = 52.0 * KDP^0.94 * 10^(-0.39 * ZDR)
#    NOTA: 'zdr_db' HA D'ENTRAR EN DECIBELS (dB), no linealitzat.
#    """
#    if use_zdr and zdr_db is not None:
#        # Estimador combinat multiparamètric (El més precís en convecció neta)
#        return 52.0 * (kdp ** 0.94) * (10 ** (-0.39 * zdr_db))
#    else:
#        # Estimador de paràmetre únic (Robust contra granís pur o ZDR contaminada)
#        return 29.0 * (kdp ** 0.85)
#
#
#def r_z_zdr(zh_lin, zdr_db):
#    """
#    R(Z, ZDR) - Ajustat per a Tròpics (GPM-NASA).
#    'zh_lin' en unitats lineals, 'zdr_db' DIRECTAMENT EN dB.
#    Equació correcta: R = 0.0067 * Z^0.93 * 10^(-0.34 * ZDR_db)
#    """
#    return 0.0067 * (zh_lin ** 0.93) * (10 ** (-0.34 * zdr_db))
#
#
#def r_a(ah):
#    """
#    R(A) - Estimació per Atenuació Específica per a la Banda C de Colòmbia.
#    Equació física: R = 210 * A^0.90
#    'ah' és l'atenuació específica en dB/km.
#    """
#    return 210.0 * (ah ** 0.90)


# =====================================================================
# 2. ARBRE DE DECISIONS OPERACIONAL (MERGE)
# =====================================================================

#def merge_rainfall(ds, alpha=None, kdp_thresh=0.3):
#    """
#    Fusió d'estimadors QPE optimitzada per a la climatologia de Colòmbia.
#    
#    Arbre de decisions:
#    1. Si KDP >= kdp_thresh (Convecció Activa):
#       - Si Z > 50 dBZ o ZDR < 0.2 dB (Risc alt de granís), s'utilitza R(KDP) pur.
#       - En cas contrari, s'utilitza l'estimador estrella R(KDP, ZDR).
#    2. Si KDP < kdp_thresh (Pluja Estratiforme o de Transició):
#       - Si Z > 38 dBZ: Banda brillant (fusió). S'utilitza 100% R(A).
#       - Si 18 <= Z <= 38 dBZ: Pluja moderada. Es fa un Blending (pes lineal en dBZ)
#         entre R(A) i R(Z) estratiforme (o opcionalment R(Z, ZDR) si hi ha confiança).
#       - Si Z < 18 dBZ: Pluja molt feble. Només R(Z) estratiforme pur per evitar soroll.
#    """
#    if alpha is None:
#        alpha = globals().get("alpha", 0.01)
#
#    # 2.1. Conversions de dades d'entrada
#    z_dbz = ds.cDBZH                     # Reflectivitat corregida en dBZ
#    zh_lin = 10 ** (z_dbz / 10.0)        # Z lineal
#    zdr_db = ds.fZDR                     # ZDR en dB (Directa de l'Xarray)
#    kdp = ds.cKDP                        # KDP corregida
#
#    # Càlcul de l'atenuació específica (A)
#    ah = ds.A if "A" in ds else (alpha * kdp)
#
#    # 2.2. Precalculem tots els estimadors element a element
#    r_z_estrat_vals = r_z(zh_lin)
#    r_z_rosenfeld_vals = r_z_rosenfeld(zh_lin)
#    r_a_vals = r_a(ah)
#    r_kdp_pur_vals = r_kdp(kdp, use_zdr=False)
#    r_kdp_zdr_vals = r_kdp(kdp, zdr_db=zdr_db, use_zdr=True)
#
#    # 2.3. DISSENY DELS BLOCKS DE BRANQUES (De baix a dalt de l'arbre)
#    
#    # --- BRANCA ESTRATIFORME (KDP < 0.3) ---
#    # Càlcul del pes del Blending per a la zona moderada (18 a 38 dBZ)
#    w_a = (z_dbz - 18.0) / (38.0 - 18.0)
#    w_a = xr.where(w_a < 0, 0.0, xr.where(w_a > 1, 1.0, w_a)) # Clamping entre 0 i 1
#    r_blending = (w_a * r_a_vals) + ((1.0 - w_a) * r_z_estrat_vals)
#
#    # Unifiquem la branca estratiforme segons el nivell de dBZ
#    #r_fallback = xr.where(
#    #    z_dbz > 38, 
#    #    r_a_vals,  # Si és > 38 dBZ i KDP és baix, és Banda Brillant -> 100% R(A)
#    #    xr.where(
#    #        z_dbz >= 18, 
#    #        r_blending,  # Zona de transició
#    #        r_z_estrat_vals  # < 18 dBZ -> Només R(Z) feble
#    #    )
#    #)
#
#    # --- BRANCA CONVECTIVA (KDP >= 0.3) ---
#    # Decidim si hi ha risc de granís o artefactes polarimètrics aliens a la pluja líquida
#    risc_granis = (z_dbz > 50) | (zdr_db < 0.2)
#    r_convectiu = xr.where(risc_granis, r_kdp_pur_vals, r_kdp_zdr_vals)
#
#    # --- SENSE FASE PERÒ AMB REFLECTIVITAT CONVECTIVA ---
#    # Si KDP és baix (< 0.3) però per algun motiu d'atenuació l'A no està disponible 
#    # i estem en un nucli fort (>38 dBZ), Rosenfeld és el nostre millor mètode d'emergència.
#    # En aquest codi, si KDP és baix i Z > 38, prioritzem R(A) tal com hem quedat, 
#    # però si no existís R(A), canviaríem a Rosenfeld. Ho afegim com a protecció:
#    r_fallback = xr.where(~(np.isfinite(r_a_vals))|(r_a_vals<=0), r_z_rosenfeld_vals, r_blending)
#
#    # 2.4. DECISIÓ FINAL DEL MERGE
#    use_kdp_branch = kdp >= kdp_thresh
#    r_final = xr.where((use_kdp_branch) & (z_dbz>=35.0), r_convectiu, r_fallback)
#
#    # 2.5. SANEJAMENT DE DADES (Sanitization)
#    # Evitem soroll de valors negatius o infinits numèrics
#    r_final = r_final.where((r_final >= 0) & (np.isfinite(r_final)), np.nan)
#
#    return r_final

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
        'C': [0.054, -1.31, 10.9],
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

with open(f'metadata/2025/{str(month).zfill(2)}/{str(day).zfill(2)}.json', 'r') as f:
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

    DBZH = swp["DBZH"].values.astype(np.float32)
    PHIDP = swp["PHIDP"].values.astype(np.float32)
    KDP = swp["KDP"].values.astype(np.float32)
    RHOHV = swp["RHOHV"].values.astype(np.float32)
    ZDR = swp["ZDR"].values.astype(np.float32)

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

    textura_phidp = texture_of_complex_phase(PHIDP.where(PHIDP>=0.0,np.nan)*2)

    tPHIDP = xr.DataArray(
        textura_phidp,
        dims=PHIDP.dims,
        coords=PHIDP.coords
    )

    swp["tPHIDP"] = tPHIDP

    textura_dbzh = wrl.util.texture(DBZH)
    tDBZH = xr.DataArray(
        textura_dbzh,
        dims=DBZH.dims,
        coords=DBZH.coords
    )

    swp["tDBZH"] = tDBZH

    no_met_mask = (
          ((DR > -10) & (DBZH<35.0))
        | ((tPHIDP > 15) & (DBZH < 30.0)) 
        | (PHIDP < 0.0)
        | (CBB == 1.0)
    )
    raw_clutter_flags = no_met_mask.values
    pad = 1
    met_mask = ~raw_clutter_flags
    met_mask_padded = np.pad(met_mask, pad_width=((pad, pad), (0, 0)), mode='wrap')
    # Using a 4-connectivity cross avoids excessive distortion along radial cell boundaries
    structure = generate_binary_structure(rank=2, connectivity=1)

    # B. Opening: Targets and erases single-pixel transient speckles across clean areas
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

    #met_mask = met_mask.reindex_like(DBZH, fill_value=True)

    swp["met_mask"] = met_mask

    no_met_mask_no_texture = (
          ((DR > -12) & (DBZH < 35.0))
        | ((tDBZH > 20.0) & (DBZH < 30.0))
        | (DBZH <= 0.0)
        | (CBB == 1.0)
    )
    raw_clutter_flags_no_texture = no_met_mask_no_texture.values
    pad = 1
    met_mask_no_texture = ~raw_clutter_flags_no_texture
    met_mask_padded_no_texture = np.pad(met_mask_no_texture, pad_width=((pad, pad), (0, 0)), mode='wrap')
    # Using a 4-connectivity cross avoids excessive distortion along radial cell boundaries
    structure = generate_binary_structure(rank=2, connectivity=1)

    # B. Opening: Targets and erases single-pixel transient speckles across clean areas
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

    #met_mask = met_mask.reindex_like(DBZH, fill_value=True)

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
    swp["fPHIDP"] = fPHIDP

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
    swp["cKDP"] = cKDP

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

    # Parameters
    dr = 300.0          # gate spacing [m]
    window = 11         # odd number

    # Phase -> complex signal
    phidp = np.deg2rad(2.0 * PHIDP.values)
    tphidp_mask = (~(tPHIDP > 15)&(DBZH < 45)).values
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


    half = window // 2

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

    KDP_degkm = median_filter(KDP_degkm, size=(3, 3))

    dKDP2 = xr.DataArray(
        KDP_degkm,
        dims=PHIDP.dims,
        coords=PHIDP.coords,
        attrs={
            "units": "deg km-1",
            "window": window,
            "gate_spacing_m": dr,
            "description": "KDP from complex least-squares phase-gradient estimator"
        }
    )

    swp["dKDP2"] = dKDP2     

    R_A_100 = 1900 * (A ** 1.00) 
    R_Z_ZDR = 0.0067 * ((10**(cDBZH_attcorr/10))** 0.93) * (10.0 **(-0.34 * fZDR))
    R_KDP_ZDR = 52.0 * (KDP ** 0.94) * (10.0 ** (-0.39 * fZDR))
    R_raw_KDP = 42.1 * (KDP  ** 0.79)
    R_cKDP = 42.1 * (cKDP  ** 0.79)
    R_KDP =  42.1 * (dKDP  ** 0.79)
    R_KDP2 = 42.1 * (dKDP2.where(met_mask) ** 0.79)    
    R_Z = (10**(cDBZH_attcorr/10)/250)**(1/1.2)
    R_Z_raw = (10**(DBZH/10)/250)**(1/1.2)

    #r_a_val = R_A.values.astype(np.float32)
    #r_raw_kdp_val = R_raw_KDP.values.astype(np.float32)
    #r_ckdp_val = R_cKDP.values.astype(np.float32)
    #r_kdp_val = R_KDP.values.astype(np.float32)
    #r_kdp2_val = R_KDP2.values.astype(np.float32)
    #r_z_val = R_Z.values.astype(np.float32)
    #r_z_raw_val = R_Z_raw.values.astype(np.float32)

    del swp
    del dtree
    del bytes_en_memoria
    gc.collect()

    return R_A_100,R_raw_KDP,R_cKDP,R_KDP,R_KDP2,R_Z,R_Z_raw,R_Z_ZDR,R_KDP_ZDR

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

    textura_phidp = texture_of_complex_phase(PHIDP.where(PHIDP>=0.0,np.nan)*2)

    #textura_phidp = wrl.util.texture(PHIDP)

    tPHIDP = xr.DataArray(
        textura_phidp,
        dims=PHIDP.dims,
        coords=PHIDP.coords
    )

    swp["tPHIDP"] = tPHIDP

    textura_dbzh = wrl.util.texture(DBZH)
    tDBZH = xr.DataArray(
        textura_dbzh,
        dims=DBZH.dims,
        coords=DBZH.coords
    )

    swp["tDBZH"] = tDBZH

    no_met_mask = (
          ((DR > -10) & (DBZH<35.0))
        | ((tPHIDP > 15) & (DBZH < 30.0)) 
        | (PHIDP < 0.0)
        | (CBB == 1.0)
    )
    raw_clutter_flags = no_met_mask.values
    pad = 1
    met_mask = ~raw_clutter_flags
    met_mask_padded = np.pad(met_mask, pad_width=((pad, pad), (0, 0)), mode='wrap')
    # Using a 4-connectivity cross avoids excessive distortion along radial cell boundaries
    structure = generate_binary_structure(rank=2, connectivity=1)

    # B. Opening: Targets and erases single-pixel transient speckles across clean areas
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

    #met_mask = met_mask.reindex_like(DBZH, fill_value=True)

    swp["met_mask"] = met_mask

    no_met_mask_no_texture = (
          ((DR > dr_thresh) & (DBZH < 35.0))
        | ((tDBZH > 20.0) & (DBZH < 30.0))
        | (DBZH <= 0.0)
        | (CBB == 1.0)
    )
    raw_clutter_flags_no_texture = no_met_mask_no_texture.values
    pad = 1
    met_mask_no_texture = ~raw_clutter_flags_no_texture
    met_mask_padded_no_texture = np.pad(met_mask_no_texture, pad_width=((pad, pad), (0, 0)), mode='wrap')
    # Using a 4-connectivity cross avoids excessive distortion along radial cell boundaries
    structure = generate_binary_structure(rank=2, connectivity=1)

    # B. Opening: Targets and erases single-pixel transient speckles across clean areas
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

    #met_mask = met_mask.reindex_like(DBZH, fill_value=True)

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
    swp["fPHIDP"] = fPHIDP
    
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
    swp["cKDP"] = cKDP

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

    A = alpha * cKDP
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

    #swp["R"] = merge_rainfall(swp,alpha) #((10**(cDBZH_attcorr/10))/250)**(1/1.2) #

    # Parameters
    dr = 300.0          # gate spacing [m]
    window = 11         # odd number

    # Phase -> complex signal
    phidp = np.deg2rad(2.0 * PHIDP.values)
    tphidp_mask = (~(tPHIDP > 15)&(DBZH < 45)).values
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


    half = window // 2

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

    KDP_degkm = median_filter(KDP_degkm, size=(3, 3))


    dKDP2 = xr.DataArray(
        KDP_degkm,
        dims=PHIDP.dims,
        coords=PHIDP.coords,
        attrs={
            "units": "deg km-1",
            "window": window,
            "gate_spacing_m": dr,
            "description": "KDP from complex least-squares phase-gradient estimator"
        }
    )

    swp["dKDP2"] = dKDP2

    R_A_100 = 1900 * (A ** 1.00) 
    R_Z_ZDR = 0.0067 * ((10**(cDBZH_attcorr/10))** 0.93) * (10.0 **(-0.34 * fZDR))
    R_KDP_ZDR = 52.0 * (KDP ** 0.94) * (10.0 ** (-0.39 * fZDR))
    R_raw_KDP = 42.1 * (KDP  ** 0.79)
    R_cKDP = 42.1 * (cKDP  ** 0.79)
    R_KDP =  42.1 * (dKDP  ** 0.79)
    R_KDP2 = 42.1 * (dKDP2.where(met_mask) ** 0.79)    
    R_Z = (10**(cDBZH_attcorr/10)/250)**(1/1.2)
    R_Z_raw = (10**(DBZH/10)/250)**(1/1.2)
    
    return R_A_100,R_raw_KDP,R_cKDP,R_KDP,R_KDP2,R_Z,R_Z_raw,R_Z_ZDR,R_KDP_ZDR,swp,dtree,site 
#----------------------------------------------------------------------------------
if __name__ == "__main__":
    R_A_100,R_raw_KDP,R_cKDP,R_KDP,R_KDP2,R_Z,R_Z_raw,R_Z_ZDR,R_KDP_ZDR,swp,dtree,site = R_per_sweep_1st(output_list[0])

    acc_R_Z_ZDR  = np.zeros_like(R_Z_ZDR.values, dtype=np.float32)
    acc_R_Z_ZDR += np.nan_to_num(R_Z_ZDR.values, nan=0.0) * (5/60)

    acc_R_A_100  = np.zeros_like(R_A_100.values, dtype=np.float32)
    acc_R_A_100 += np.nan_to_num(R_A_100.values, nan=0.0) * (5/60)

    acc_R_KDP_ZDR  = np.zeros_like(R_Z_ZDR.values, dtype=np.float32)
    acc_R_KDP_ZDR += np.nan_to_num(R_Z_ZDR.values, nan=0.0) * (5/60)

    acc_R_raw_KDP  = np.zeros_like(R_raw_KDP.values, dtype=np.float32)
    acc_R_raw_KDP += np.nan_to_num(R_raw_KDP.values, nan=0.0) * (5/60)

    acc_R_cKDP  = np.zeros_like(R_cKDP.values, dtype=np.float32)
    acc_R_cKDP += np.nan_to_num(R_cKDP.values, nan=0.0) * (5/60)

    acc_R_dKDP  = np.zeros_like(R_KDP.values, dtype=np.float32)
    acc_R_dKDP += np.nan_to_num(R_KDP.values, nan=0.0) * (5/60)

    acc_R_KDP2  = np.zeros_like(R_KDP2.values, dtype=np.float32)
    acc_R_KDP2 += np.nan_to_num(R_KDP2.values, nan=0.0) * (5/60)

    acc_R_Z  = np.zeros_like(R_Z.values, dtype=np.float32)
    acc_R_Z += np.nan_to_num(R_Z.values, nan=0.0) * (5/60)

    acc_R_Z_raw  = np.zeros_like(R_Z_raw.values, dtype=np.float32)
    acc_R_Z_raw += np.nan_to_num(R_Z_raw.values, nan=0.0) * (5/60)

    errors = 0

    WORKERS = 14
    BATCH_SIZE = 28

    files_to_process = output_list[1:]

    for i in range(0, len(files_to_process), BATCH_SIZE):
        batch = files_to_process[i:i + BATCH_SIZE]
        print(f"Processing batch {i//BATCH_SIZE + 1} / {(len(files_to_process) // BATCH_SIZE) + 1}...")

        # Creating the executor inside the loop ensures that after BATCH_SIZE files, 
        # the processes are killed and all fragmented RAM is returned to the OS.
        with ProcessPoolExecutor(max_workers=WORKERS) as executor:
            futures = {
                executor.submit(R_per_sweep, s3_path): s3_path
                for s3_path in batch
            }

            for future in as_completed(futures):
                s3_path = futures[future]
                try:
                    # Retrieve the lightweight float32 numpy arrays
                    r_kdp2 = future.result()#r_a_103,r_a_100,r_a_097,r_a_121, r_raw_kdp, r_ckdp, r_kdp, r_kdp2, r_z, r_z_raw = future.result()

                    # Accumulate directly
                    #acc_R_A_103   += np.nan_to_num(r_a_103, nan=0.0)   * (5/60)
                    #acc_R_A_100   += np.nan_to_num(r_a_100, nan=0.0)   * (5/60)
                    #acc_R_A_097   += np.nan_to_num(r_a_097, nan=0.0)   * (5/60)
                    #acc_R_A_121   += np.nan_to_num(r_a_121, nan=0.0)   * (5/60)
                    #acc_R_raw_KDP += np.nan_to_num(r_raw_kdp, nan=0.0) * (5/60)
                    #acc_R_cKDP    += np.nan_to_num(r_ckdp, nan=0.0)    * (5/60)
                    #acc_R_KDP     += np.nan_to_num(r_kdp, nan=0.0)     * (5/60)
                    acc_R_KDP2    += np.nan_to_num(r_kdp2, nan=0.0)    * (5/60)
                    #acc_R_Z       += np.nan_to_num(r_z, nan=0.0)       * (5/60)
                    #acc_R_Z_raw   += np.nan_to_num(r_z_raw, nan=0.0)   * (5/60)

                    print(f"Finished {s3_path}")
                    
                    # Delete local references to free memory quickly
                    del r_kdp2 #r_a_103,r_a_100,r_a_097,r_a_121, r_raw_kdp, r_ckdp, r_kdp, r_kdp2, r_z, r_z_raw

                except Exception as e:
                    print(f"Error processing {s3_path}: {e}")
                    errors += 1
        
        # Force garbage collection between batches
        gc.collect()

    print(f"Càlcul finalitzat. Errors totals detectats: {errors}")

    ds = xr.Dataset(
        data_vars={
            #"acc_R_A_103":     (swp["DBZH"].dims, acc_R_A_103),
            #"acc_R_A_100":     (swp["DBZH"].dims, acc_R_A_100),
            #"acc_R_A_097":     (swp["DBZH"].dims, acc_R_A_097),
            #"acc_R_A_121":     (swp["DBZH"].dims, acc_R_A_121),
            #"acc_R_raw_KDP":   (swp["DBZH"].dims, acc_R_raw_KDP),
            #"acc_R_cKDP":      (swp["DBZH"].dims, acc_R_cKDP),
            #"acc_R_KDP":       (swp["DBZH"].dims, acc_R_KDP),
            "acc_R_KDP2":      (swp["DBZH"].dims, acc_R_KDP2),
            #"acc_R_Z":         (swp["DBZH"].dims, acc_R_Z),
            #"acc_R_Z_raw":     (swp["DBZH"].dims, acc_R_Z_raw)
        },
        coords=swp["DBZH"].coords,
        attrs=swp["DBZH"].attrs
    )

    # Afegir metadades geogràfiques del radar de Barrancabermeja
    ds.attrs["sweep_mode"] = swp["sweep_mode"].values
    ds.coords["longitude"] = dtree["longitude"].values
    ds.coords["latitude"]  = dtree["latitude"].values
    ds.coords["altitude"]  = dtree["altitude"].values

    # Exportar el fitxer final NetCDF
    output_filename = f"2025{str(month).zfill(2)}{str(day).zfill(2)}_R_KDP_acc_barrancabermeja.nc"
    ds.to_netcdf(output_filename)
    print(f"Fitxer guardat correctament: {output_filename}")
###------------------------------------------------------------------------------------
#gauge_R = []
#R = []
#for month in np.arange(5,11):
#    for day in np.arange(1,32):
#        print(str(day).zfill(2))
#        if ((month==5) & ((day == 22) | (day == 23))) | (((day == 31))):
#            continue
#        try:
#            ds = xr.open_dataset(f"2025{str(month).zfill(2)}{str(day).zfill(2)}_R_acc_barrancabermeja.nc",engine="netcdf4")
#            da=ds["acc_R_KDP2"].sel(range=slice(None,150e3))#*1892/430
#        except Exception as e:
#            print(month,day)
#            continue
#
#        #da = da.where(da != 0)
#
#        print(da)
#
#        lat = da.latitude.values
#        lon = da.longitude.values
#        max_range = 150/111
#        max_lat = lat + max_range
#        max_lon = lon + max_range
#        min_lat = lat - max_range
#        min_lon = lon - max_range
#    
#        start = dt.datetime(2025,month,day)#+1)
#        print("start:", start)
#        end = dt.datetime(2025,month,day+1)#+2)
#        print("end:", end)
#        df = download_data(start,end,min_lat,max_lat,min_lon,max_lon) # a dia 12/05/2026 no funciona del tot correctament i has de posar un dia més perquè et retorni el dia que pertoca
#        print(df)
#        #df["fechaobservacion"] = (
#        #    pd.to_datetime(df["fechaobservacion"])
#        #    .dt.tz_localize("America/Bogota")
#        #    .dt.tz_convert("UTC")
#        #)
#        print(df)
#        df["valorobservado"] = pd.to_numeric(df["valorobservado"], errors="coerce")
#        df = df.dropna(subset=["valorobservado"])
#        df["latitud"] = pd.to_numeric(df["latitud"], errors="coerce")
#        df =df.dropna(subset=["latitud"])
#        df["longitud"] = pd.to_numeric(df["longitud"], errors="coerce")
#        df = df.dropna(subset=["longitud"])
#
#        hourly_geo_df = df.groupby([
#            'codigoestacion', 
#            'latitud', 
#            'longitud',
#        ])['valorobservado'].sum().reset_index()
#        diary_geo_df = hourly_geo_df.rename(columns={'valorobservado': 'acumulado_diario'})
#        print(diary_geo_df)
#
#        ranges = [10000, 50000, 100000, 150000]
#        number_of_colors=72
#
#        site= (da.longitude.values,
#               da.latitude.values,
#               da.altitude.values)
#
#        gate_lon = da["gate_longitude"].values
#        gate_lat = da["gate_latitude"].values
#
#        gate_points = np.column_stack([gate_lon.ravel(), gate_lat.ravel()])
#        tree = cKDTree(gate_points)
#
#        gauge_coords = diary_geo_df[['longitud', 'latitud']].values
#        distances, indices = tree.query(gauge_coords, k=1)
#
#        # extend gauge_R with scalar values so gauge_R is a flat list matching R
#        gauge_R.extend(diary_geo_df['acumulado_diario'].values.tolist())
#
#        # ensure indices is 1D and iterate, converting radar values to scalar floats
#        indices = np.array(indices).ravel()
#        for idx in indices:
#            az_idx, rng_idx = np.unravel_index(int(idx), gate_lon.shape)
#            val_arr = np.asarray(da.isel(azimuth=int(az_idx), range=int(rng_idx)).values).ravel()
#            val = float(val_arr[0]) if val_arr.size > 0 else np.nan
#            R.append(val)
###da_geo = da.wrl.georef.georeference()
###fig = plt.figure(figsize=(20,10))       
###ax = fig.add_subplot(121, projection=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values))
###
###ax.set_facecolor('xkcd:light gray') 
###
###plot_features(ax)
###da_geo.plot.pcolormesh(
###    x="x",
###    y="y",
###    ax=ax,
###    vmin=0,
###    vmax=200,
###    cmap=cmap.Colormap("ncar").to_mpl(number_of_colors),
###    transform=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values),
###    add_colorbar=True,
###)
###
###zeros = diary_geo_df[diary_geo_df["acumulado_diario"] == 0]
###nonzeros = diary_geo_df[diary_geo_df["acumulado_diario"] > 0]
###
###sc = ax.scatter(
###    nonzeros["longitud"],
###    nonzeros["latitud"],
###    c=nonzeros["acumulado_diario"],
###    vmin=0,
###    vmax=200,
###    cmap=cmap.Colormap("ncar").to_mpl(number_of_colors),
###    s=50,
###    edgecolors="red",
###    linewidth=0.9,
###    alpha=0.9,
###    transform=ccrs.PlateCarree(),
###    zorder=1,
###)
###
###ax.scatter(
###    zeros["longitud"],
###    zeros["latitud"],
###    facecolors="none",
###    edgecolors="black",
###    s=20,
###    linewidth=0.5,
###    alpha=0.7,
###    transform=ccrs.PlateCarree(),
###    zorder=2,
###)
###
###cb = plt.colorbar(sc,ax=ax,extend="max")
###
###ax.set_extent([min_lon, max_lon, min_lat, max_lat], crs=ccrs.PlateCarree())
###
###proj_crs = ccrs.AzimuthalEquidistant(
###    central_longitude=da.longitude.values, 
###    central_latitude=da.latitude.values
###)
###
###wrl.vis.plot_ppi_crosshair(
###    site=site,
###    ranges=ranges,
###    line={"color": "None"},
###    circle={"edgecolor": "black", "linewidth" : 1, "linestyle" : "-"},
###    ax=ax,
###    crs=proj_crs,
###)
###
#### 3. Add the text labels
#### Choose an angle to place the text (e.g., 45 degrees, top-right quadrant)
###angle_deg = 45 
###angle_rad = np.radians(angle_deg)
###
###for r in ranges:
###    # Calculate x and y coordinates in meters from the center
###    # In meteorology, 0 degrees is usually North. 
###    # So x = r * sin(angle), y = r * cos(angle)
###    x = r * np.sin(angle_rad)
###    y = r * np.cos(angle_rad)
###    
###    # Format the label to km for readability (e.g., "12.5 km")
###    label_text = f"{r / 1000:g} km"
###    
###    # Plot the text
###    ax.text(
###        x, y, label_text,
###        transform=proj_crs,         # Ensures it maps to your cartopy projection correctly
###        fontsize=9,
###        ha='center',                # Horizontally center the text on the coordinate
###        va='center',                # Vertically center the text on the coordinate
###        color='black',
###        # Adding a small, semi-transparent white box behind the text makes it 
###        # readable even if it overlaps with intense radar MLes.
###        bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1.5) 
###    )
#####print(diary_geo_df["longitud"].to_numpy())
#####print(type(diary_geo_df["codigoestacion"].to_numpy()[0]))
#####
###### annotate each station individually (matplotlib expects scalar x, y, text)
#####for _, row in diary_geo_df.iterrows():
#####    ax.text(
#####        row["longitud"],
#####        row["latitud"],
#####        str(row["codigoestacion"]),
#####        transform=ccrs.PlateCarree(),
#####        fontsize=8,
#####        ha='left',
#####        va='bottom',
#####        color='black',
#####        bbox=dict(facecolor='white', alpha=0.5, edgecolor='none', pad=1)
#####    )
######........................................... 
#
## 1. Assegurem que les dades siguin arrays de NumPy per als càlculs
#R_arr = np.array(R)
#gauge_R_arr = np.array(gauge_R)
#
#mask = np.where((gauge_R_arr > 10) & (R_arr > 10))
#
#fig = plt.figure(figsize=(10,10))
#ax2 = fig.add_subplot(111)
#
#ax2.scatter(gauge_R_arr, R_arr, marker='o',color='k')
##ax2.plot([0,500],[0,500], "red")
##ax2.set_xscale('log')
##ax2.set_yscale('log')
##ax2.set_xlim(5e-2,1e2)
##ax2.set_ylim(5e-2,1e2)
##
##ax2.set_xticks([0.1,1.0,10.0,100.0,500.0])
##ax2.set_yticks([0.1,1.0,10.0,100.0,500.0])
##
##ax2.set_xticklabels(["0.1", "1", "10", "100", "500"],size=15)
##ax2.set_yticklabels(["0.1", "1", "10", "100", "500"],size=15)
##
##ax2.set_xlabel(r"Accumulated precipitation (gauge) [mm]", fontsize=15)
##ax2.set_ylabel(r"Accumulated precipitation (radar) [mm]", fontsize=15)
#
#
#ax2.grid(which="major")
#
## 2. Càlcul de mètriques (Pearson, RMSE i BIAS)
#print(f"Valors únics de R: {np.unique(R_arr)}")  # Corregit 'unique_values' per 'unique' 
#
#rmse = np.sqrt(np.mean((R_arr - gauge_R_arr) ** 2))
#print(f"L'RMSE és: {rmse:.4f}")
#
#bias = np.mean(R_arr - gauge_R_arr)
#print(f"El biaix (BIAS) és: {bias:.4f}")
#
## 3. REGRESSIÓ LINEAL (Aquí apliquem el .reshape(-1, 1) necessari)
#gauge_R_2d = gauge_R_arr.reshape(-1, 1)
#
#model = LinearRegression(fit_intercept=False)
#model.fit(gauge_R_2d, R_arr)  # Ara ja no donarà l'error de Reshape
#
#m = model.coef_[0]
#c = model.intercept_
#print(f"Equació obtinguda: y = {m:.4f}*x + {c:.4f}")
#
#model2 = LinearRegression(fit_intercept=True)
#model2.fit(gauge_R_2d, R_arr)  # Ara ja no donarà l'error de Reshape
#
#m2 = model2.coef_[0]
#c2 = model2.intercept_
#print(f"Equació obtinguda: y = {m2:.4f}*x + {c2:.4f}")
#
#model3 = LinearRegression(fit_intercept=True)
#model3.fit(gauge_R_2d[mask], R_arr[mask])  # Ara ja no donarà l'error de Reshape
#
#m3 = model3.coef_[0]
#c3 = model3.intercept_
#print(f"Equació obtinguda: y = {m3:.4f}*x + {c3:.4f}")
#
## 4. Definició de la funció ajustada
#def f_ajustada(x_val,m,c):
#    return m * x_val + c
#
## 5. Gràfic (He corregit el conflicte on 'y' trepitjava la teva funció)
#x_plot = np.linspace(5e-2, 2e2, 1000)
#
## Opció A: Si vols pintar la línia ideal y = x
#y_ideal = x_plot
#ax2.plot(x_plot, y_ideal, label="Truth Slope", linestyle="--", color="c")
#
## Opció B: Si vols pintar la línia real que ha trobat la teva regressió lineal
#y_regressio = f_ajustada(x_plot,m,c)
##ax2.plot(x_plot, y_regressio, label=f"y={m:.3f}x", color="red")
#
#y_regressio2 = f_ajustada(x_plot,m2,c2)
#ax2.plot(x_plot, y_regressio2, label=f"y={m2:.3f}x + {c2:3f}", color="green")
#
#
#y_regressio3 = f_ajustada(x_plot,m3,c3)
#ax2.plot(x_plot, y_regressio3, label=f"y={m3:.3f}x + {c3:3f}", color="blue")
#
#ax2.legend(fontsize=15)
#plt.tight_layout()
#plt.show()#