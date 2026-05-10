import xradar as xd
import sys
import wradlib as wrl
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import xarray as xr
import cmap
from scipy.ndimage import median_filter, gaussian_filter
from scipy.ndimage import generic_filter
from scipy.ndimage import uniform_filter

def texture(x):

    #valid = np.isfinite(x)

    #if np.sum(valid) < 1000:
    #    return np.nan

    return np.nanstd(x)

def smooth_snrhc(swp):
    snrhc = swp["SNRHC"]
    # Median filter
    snrhc_smooth = median_filter(
        snrhc.values,
        size=(1,4)
    )

    snrhc_smooth = gaussian_filter(
        snrhc_smooth,
        sigma=(1,1)
    )

    # Back to xarray
    snrhc_smooth = xr.DataArray(
        snrhc_smooth,
        dims=snrhc.dims,
        coords=snrhc.coords
    )
    swp["sSNRHC"] = snrhc_smooth


def smooth_ncp(swp):
    ncp = swp["NCPH"]

    # Remove impossible values
    ncp = xr.where(
        (ncp >= 0) & (ncp <= 1),
        ncp,
        np.nan
    )

    # Fill NaN temporarily
    ncp_fill = ncp.fillna(0)

    # Median filter
    ncp_smooth = median_filter(
        ncp_fill.values,
        size=(3,5)
    )

    ncp_smooth = gaussian_filter(
        ncp_smooth,
        sigma=(1,1)
    )

    # Back to xarray
    ncp_smooth = xr.DataArray(
        ncp_smooth,
        dims=ncp.dims,
        coords=ncp.coords
    )

    swp["sNCPH"] = ncp_smooth

def smooth_rho(swp):
    rho = swp["RHOHV"]
    # Median filter
    rho_smooth = median_filter(
        rho.values,
        size=(3,4)
    )

    rho_smooth = gaussian_filter(
        rho_smooth,
        sigma=(1,1)
    )

    # Back to xarray
    rho_smooth = xr.DataArray(
        rho_smooth,
        dims=rho.dims,
        coords=rho.coords
    )
    swp["sRHOHV"] = rho_smooth

def smooth_phidp(swp):

    phidp = swp["PHIDP"]

    phidp_med = median_filter(
        np.nan_to_num(phidp.values, nan=0),
        size=(1,8)
    )

    swp["sPHIDP"] = xr.DataArray(
        phidp_med,
        dims=phidp.dims,
        coords=phidp.coords
    )

def texture_phidp(swp):

    sphi = swp["sPHIDP"]

    tex = generic_filter(
        sphi.values,
        texture,
        size=(1,8)
    )

    swp["tPHIDP"] = xr.DataArray(
        tex,
        dims=sphi.dims,
        coords=sphi.coords
    )

def filter_phidp(swp):

    phi = swp["sPHIDP"]

    tex = swp["tPHIDP"]

    rho = swp["sRHOHV"]

    snr = xr.ufuncs.minimum(
        swp["SNRHC"],
        swp["SNRVC"]
    )

    mask = (
        (rho > 0.8) &
        (snr > 10) #&
        #(tex < 10)
    )

    phi_f = xr.where(mask, phi, np.nan)

    vals = np.nan_to_num(phi_f.values, nan=0)

    vals = median_filter(
        vals,
        size=(1,8)
    )

    vals = gaussian_filter(
        vals,
        sigma=(0,1)
    )

    vals[~mask.values] = np.nan

    swp["fPHIDP"] = xr.DataArray(
        vals,   
        dims=phi.dims,
        coords=phi.coords
    )

def filter_KDP_RHO(dt):
    swp = dt["/sweep_0"]
    rho = swp["RHOHV"]
    kdp = swp["KDP"]
    z = swp["DBZH"]
    snr = xr.ufuncs.minimum(
        swp["SNRHC"],
        swp["SNRVC"]
    )
    mask = (
    (rho > 0.8) &
    (z > 10) &
    (snr > 10)
)

    ckdp = xr.where(
        mask,
        kdp,
        np.nan,
    )

    swp["cKDP"] = ckdp
    return dt

def plot_features(ax):
    states = cfeature.STATES.with_scale('10m')
    ax.add_feature(states, edgecolor="black", lw=2, zorder=4)

filepath = sys.argv[1]

dt = xd.io.open_cfradial1_datatree(filepath, decode_times=False)

swp = dt["/sweep_0"]
filter_KDP_RHO(dt)
smooth_ncp(swp)
smooth_rho(swp)
smooth_snrhc(swp)
smooth_phidp(swp)
texture_phidp(swp)
filter_phidp(swp)


def integrate_kdp_to_phidp(ds):
    """Integrate KDP along range dimension."""
    dr = ds.range.diff('range').mean().item()  # constant spacing
    phi = 2 * (ds.cKDP * dr/1000.).cumsum(dim='range')
    phi = phi - phi.isel(range=0)  # start at zero
    return phi
    

n_sweeps = [
    sweep_name 
    for sweep_name 
    in dt.groups 
    if sweep_name.startswith("/sweep_")
]

for sweep in n_sweeps:
    if "KDP" in dt[sweep].data_vars:
        dt[sweep]['cPHIDP'] = integrate_kdp_to_phidp(dt[sweep])
        dt[sweep]['cPHIDP'].attrs["long_name"] = "corrected_differential_phase"
        dt[sweep]['cPHIDP'].attrs["units"] = "degrees"

def ZDR_alpha(dt):
    swp = dt["/sweep_0"]
    zdr = swp["ZDR"]
    
    alpha = xr.where(
        zdr > 0.3, 
        0.008 + (0.009 / (zdr - 0.03)), 
        np.nan,
    )
    
    swp["alpha"] = alpha
    return dt
ZDR_alpha(dt)

def PIA(dt):
    swp = dt["/sweep_0"]
    alpha = 0.1 #swp["alpha"]
    cphi = swp["cPHIDP"]
    
    pia = alpha * cphi
    
    swp["PIA"] = pia
    return dt
PIA(dt)

def cDBZH(dt):
    swp = dt["/sweep_0"]
    pia = swp["PIA"]
    dbzh = swp["DBZH"]
    
    cdbzh = pia + dbzh
    
    swp["cDBZH"] = cdbzh
    swp['cDBZH'].attrs["long_name"] = "corrected_reflectivity"
    swp['cDBZH'].attrs["units"] = "dBZ"
    return dt
cDBZH(dt)

def A_KDP(dt):
    swp = dt["/sweep_0"]
    alpha = 0.1 #swp["alpha"]
    ckdp = swp["cKDP"]

    A = alpha*ckdp

    swp["A_kdp"] = A
    return dt

A_KDP(dt)

def R_A(dt):
    swp = dt["/sweep_0"]
    a = swp["A_kdp"]

    R = xr.where(
        np.abs(a) < 0.045,
        4120*a**1.03,
        np.nan,
    )
    
    swp["R_A"] = R
    return dt
R_A(dt)

def R_kdp(dt):
    swp = dt["/sweep_0"]
    r_kdp = swp["cKDP"]

    rkdp = xr.where(
        r_kdp > 0.1,
        34.57 * r_kdp**0.73,
        0
    )
    
    swp["R_KDP"] = rkdp
    return dt
R_kdp(dt)

print(dt)
print(dt["/sweep_0"].data_vars)
print(dt["/sweep_0"].pulse_width.values[0]*299792458./2.)
print(dt["/sweep_0"].range.values[0])

#while dt["/sweep_0"].range.values[0] < 125.0:
    #dt["/sweep_0"] = dt["/sweep_0"].ds.assign_coords(range=dt["/sweep_0"].range * 2.0)

print(dt["/sweep_0/KDP"].shape)

var = sys.argv[2]
da = dt["/sweep_0"][f"{var}"]
#da = xr.where(
#    dt["/sweep_0/NCPH"] >= 0.0
#)
print(da)

da.attrs["sweep_mode"] = dt["/sweep_0"]["sweep_mode"].values
da_geo = da.wrl.georef.georeference()
fig = plt.figure(figsize=(20,10))       
ax2 = fig.add_subplot(111, projection=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values))

plot_features(ax2)
da_geo.plot.pcolormesh(
    x="x",
    y="y",
    ax=ax2,
    #vmin=0,
    #vmax=100,
    cmap=cmap.Colormap("ncar").to_mpl(),
    transform=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values),
    add_colorbar=True,
)
plt.tight_layout()
plt.show()

