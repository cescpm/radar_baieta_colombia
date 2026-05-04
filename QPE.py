import xradar as xd
import sys
import wradlib as wrl
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import xarray as xr

def filter_KDP_RHO(dt):
    swp = dt["/sweep_0"]
    rho = swp["DBZH"]
    kdp = swp["KDP"]
    z = swp["DBZH"]

    ckdp = xr.where(
        ((rho > 0.95) & (z > 10)),
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

filter_KDP_RHO(dt)

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

#def PIA(dt):
#    swp = dt["/sweep_0"]
#    alpha = swp["alpha"]
#    cphi = swp["cPHIDP"]
#    
#    pia = 
#    
#    swp["PIA"] = pia
#    return dt
#PIA(dt)

def A_KDP(dt):
    swp = dt["/sweep_0"]
    alpha = swp["alpha"]
    ckdp = swp["cKDP"]

    A = alpha*ckdp

    swp["A"] = A
    return dt

A_KDP(dt)

def R_A(dt):
    swp = dt["/sweep_0"]
    a = swp["A"]

    R = 4120*a**1.03

    swp["R_A"] = R
    return dt
R_A(dt)

print(dt)
print(dt["/sweep_0"].data_vars)
print(dt["/sweep_0"].pulse_width.values[0]*299792458./2.)
print(dt["/sweep_0"].range.values[0])

dt["/sweep_0"] = dt["/sweep_0"].ds.assign_coords(range=dt["/sweep_0"].range * 2.0)

print(dt["/sweep_0/KDP"].shape)

var = sys.argv[2]
da = dt["/sweep_0"][f"{var}"]
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
    cmap='turbo_r',
    transform=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values),
    add_colorbar=True,
)
plt.tight_layout()
plt.show()

