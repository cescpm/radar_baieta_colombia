import xradar as xd
import sys
import wradlib as wrl
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import xarray as xr
import cmap
from scipy.ndimage import (
    median_filter,
    gaussian_filter,
    generic_filter,
    uniform_filter,
    label,
)
# PLOTERS
###############################################################################

def plot_features(ax):
    states = cfeature.STATES.with_scale('10m')
    ax.add_feature(states, edgecolor="black", lw=2, zorder=4)

# HELPERS
###############################################################################

def texture_std(x):

    #valid = np.isfinite(x)

    #if np.sum(valid) < 1000:
    #    return np.nan

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

###############################################################################
# MAIN QC + PRECIP PIPELINE
###############################################################################

def radar_qc_pipeline(
    swp,
    alpha=0.01,
    rhohv_thresh=0.8,
    snr_thresh=8,
    ncp_thresh=0.3,
    texture_thresh=3,
):
    """
    Polarimetric radar QC and precipitation estimation pipeline.

    INPUT:
        swp : xarray.Dataset

    OUTPUT:
        Dataset with:
            - filtered reflectivity
            - filtered PHIDP
            - filtered KDP
            - specific attenuation A
            - PIA
            - rainfall estimates
    """

    ###########################################################################
    # VARIABLES
    ###########################################################################

    DBZH = swp["DBZH"]

    PHIDP = swp["PHIDP"]

    KDP = swp["KDP"]

    RHOHV = swp["RHOHV"]

    NCP = xr.ufuncs.minimum(
        swp["NCPH"],
        swp["NCPV"]
    )

    SNR = xr.ufuncs.minimum(
        swp["SNRHC"],
        swp["SNRVC"]
    )

    ###########################################################################
    # 1. METEOROLOGICAL MASK
    ###########################################################################

    met_mask = (
          (RHOHV > rhohv_thresh)
        & (SNR > snr_thresh)
        #& (NCP > ncp_thresh) 
        & (DBZH > 0)
    )

    ###########################################################################
    # 2. REMOVE ISOLATED PIXELS
    ###########################################################################

    #density = uniform_filter(
    #    met_mask.astype(float),
    #    size=(3,5)
    #)

    #met_mask = density > 0.5

    mask_vals = met_mask.values.copy()

    labels, num = label(mask_vals)

    sizes = np.bincount(labels.ravel())

    min_size = 3

    small = sizes < min_size

    # Preserve background
    small[0] = False

    remove_mask = small[labels]

    mask_vals[remove_mask] = False

    met_mask = xr.DataArray(
        mask_vals,
        dims=met_mask.dims,
        coords=met_mask.coords
    )

    ###########################################################################
    # 3. FILTER REFLECTIVITY
    ###########################################################################

    cDBZH = xr.where(
        met_mask,
        DBZH,
        np.nan
    )

    ###########################################################################
    # 4. INITIAL PHIDP SMOOTHING
    ###########################################################################

    phi_vals = PHIDP.values.copy()

    phi_vals = np.nan_to_num(
        phi_vals,
        nan=0
    )

    phi_med = median_filter(
        phi_vals,
        size=(1,8)
    )

    sPHIDP = xr.DataArray(
        phi_med,
        dims=PHIDP.dims,
        coords=PHIDP.coords
    )

    ###########################################################################
    # 5. PHIDP TEXTURE
    ###########################################################################

    tPHIDP = compute_texture(
        sPHIDP,
        size=(1,5)
    )

    ###########################################################################
    # 6. PHIDP QUALITY MASK
    ###########################################################################

    phi_mask = (
           met_mask
        #& (tPHIDP < texture_thresh)
    )

    ###########################################################################
    # 7. FILTERED PHIDP
    ###########################################################################

    fPHIDP = xr.where(
        phi_mask,
        sPHIDP,
        np.nan
    )

    ###########################################################################
    # 8. FINAL PHIDP SMOOTHING
    ###########################################################################

    vals = np.nan_to_num(
        fPHIDP.values,
        nan=0
    )

    vals = median_filter(
        vals,
        size=(1,5)
    )

    vals = gaussian_filter(
        vals,
        sigma=(0,1)
    )

    vals[~phi_mask.values] = np.nan

    ###########################################################################
    # 9. MONOTONIC ENFORCEMENT
    ###########################################################################

    vals = np.maximum.accumulate(
        vals,
        axis=1
    )

    fPHIDP = xr.DataArray(
        vals,
        dims=PHIDP.dims,
        coords=PHIDP.coords
    )

    ###########################################################################
    # 10. CLEAN KDP
    ###########################################################################

    cKDP = xr.where(
        phi_mask,
        KDP,
        np.nan
    )

    ###########################################################################
    # 11. SPECIFIC ATTENUATION
    ###########################################################################

    A = alpha * cKDP

    ###########################################################################
    # 12. RANGE RESOLUTION
    ###########################################################################

    dr = float(
        swp["range"][1] - swp["range"][0]
    )

    dr_km = dr / 1000.0

    ###########################################################################
    # 13. PIA
    ###########################################################################

    pia_vals = 2 * np.nancumsum(
        A.values * dr_km,
        axis=1
    )

    PIA = xr.DataArray(
        pia_vals,
        dims=A.dims,
        coords=A.coords
    )

    ###########################################################################
    # 14. ATTENUATION CORRECTED REFLECTIVITY
    ###########################################################################

    cDBZH_attcorr = cDBZH + PIA

    ###########################################################################
    # 15. RAINFALL ESTIMATION
    ###########################################################################

    # R(A)
    R_A = 4120 * (A ** 1.03)

    # R(KDP)
    R_KDP = 40.6 * (cKDP ** 0.85)

    ###########################################################################
    # SAVE OUTPUTS
    ###########################################################################

    swp["met_mask"] = met_mask

    swp["sPHIDP"] = sPHIDP

    swp["tPHIDP"] = tPHIDP

    swp["fPHIDP"] = fPHIDP

    swp["cKDP"] = cKDP

    swp["A"] = A

    swp["PIA"] = PIA

    swp["cDBZH"] = cDBZH_attcorr

    swp["R_A"] = R_A

    swp["R_KDP"] = R_KDP

    return swp

filepath = sys.argv[1]

dt = xd.io.open_cfradial1_datatree(filepath, decode_times=False)

swp = dt["/sweep_0"]

fswp = radar_qc_pipeline(swp)

var = sys.argv[2]
da = fswp[f"{var}"]

print(da)

da.attrs["sweep_mode"] = fswp["sweep_mode"].values
da_geo = da.wrl.georef.georeference()
fig = plt.figure(figsize=(20,10))       
ax = fig.add_subplot(111, projection=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values))

plot_features(ax)
da_geo.plot.pcolormesh(
    x="x",
    y="y",
    ax=ax,
    #vmin=0,
    #vmax=100,
    cmap=cmap.Colormap("ncar").to_mpl(),
    transform=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values),
    add_colorbar=True,
)
plt.tight_layout()
plt.show()

#for sweep in n_sweeps:
#    if "KDP" in dt[sweep].data_vars:
#        dt[sweep]['cPHIDP'] = integrate_kdp_to_phidp(dt[sweep])
#        dt[sweep]['cPHIDP'].attrs["long_name"] = "corrected_differential_phase"
#        dt[sweep]['cPHIDP'].attrs["units"] = "degrees"
