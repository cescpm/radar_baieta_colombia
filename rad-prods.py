from wradlib.georef.polar import spherical_to_xyz 
from wradlib.vpr import make_3d_grid, PseudoCAPPI, CAPPI
from wradlib import ipol
from wradlib import vis
from RAW_PVOL import retrieve_PVol_dtree
from osgeo import osr
import numpy as np
from pyproj import Transformer
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import xarray as xr
#----------------------------------------------------------------------------------------

def PVol_to_CVol(pvol_dtree):

    radar_alt = float(pvol_dtree["/"]["altitude"].values)
    radar_lon = float(pvol_dtree["/"]["longitude"].values)
    radar_lat = float(pvol_dtree["/"]["latitude"].values)

    site = (radar_lon, radar_lat, radar_alt)
        
    lon_Cscan, x_Cscan  = [], []
    lat_Cscan, y_Cscan  = [], []
    alt_Cscan           = []
    elev_Cscan          = []
    dbzh_Cscan          = []
        
    n_sweeps = [
        sweep_name 
        for sweep_name 
        in pvol_dtree.groups 
        if sweep_name.startswith("/sweep_")
    ]

    for sweep in n_sweeps:
        scan = pvol_dtree[sweep]

        azimuth   = scan["azimuth"].data
        range     = scan["range"].data
        elevation = scan["elevation"].data
        dbzh      = scan["DBZH"].data

        azimuth_2d, range_2d = np.meshgrid(azimuth, range, indexing='ij')
        elevation_2d = np.broadcast_to(elevation[:,np.newaxis], azimuth_2d.shape)

        xyz,aeqd = spherical_to_xyz(
            range_2d,
            azimuth_2d,
            elevation_2d,
            site,
        )

        x = xyz[...,0]
        y = xyz[...,1]
        z = xyz[...,2]

        transformer = Transformer.from_crs(
            aeqd,
            "EPSG:4326",
            always_xy=True,
        )
        lon,lat = transformer.transform(x,y)
        alt = z

        lon_Cscan.append(lon.ravel())
        lat_Cscan.append(lat.ravel())
        alt_Cscan.append(alt.ravel())
        x_Cscan.append(x.ravel())
        y_Cscan.append(y.ravel())
        dbzh_Cscan.append(dbzh.ravel())
        elev_Cscan.append(elevation_2d.ravel())

    x_Cvol    = np.concatenate(x_Cscan)
    y_Cvol    = np.concatenate(y_Cscan)
    lon_Cvol  = np.concatenate(lon_Cscan)
    lat_Cvol  = np.concatenate(lat_Cscan)
    alt_Cvol  = np.concatenate(alt_Cscan)
    dbzh_Cvol = np.concatenate(dbzh_Cscan)
    elev_Cvol = np.concatenate(elev_Cscan)

    Cvol_ds = xr.Dataset(
        data_vars  = {
            "DBZH" : ("gate", dbzh_Cvol),
        },
        coords     = {
            "x"    : ("gate", x_Cvol),
            "y"    : ("gate", y_Cvol),
            "lon"  : ("gate", lon_Cvol),
            "lat"  : ("gate", lat_Cvol),
            "alt"  : ("gate", alt_Cvol),
            "elev" : ("gate", elev_Cvol),
        },
        attrs                 = {
            "instrument_name" : pvol_dtree["/"].attrs["instrument_name"],
            "lon_loc"         : pvol_dtree["/radar_parameters"].coords["longitude"].values,
            "lat_loc"         : pvol_dtree["/radar_parameters"].coords["latitude"].values,
            "alt_loc"         : pvol_dtree["/radar_parameters"].coords["altitude"].values,
            "aeqd"            : aeqd,
        },
    )

    return Cvol_ds


def CVol_to_PseudoCAPPI(ds : xr.Dataset, maxrange : float=240000, maxalt : float=20000, horiz_res=2000, vert_res=500):
    polcoords = np.column_stack([ds.x.values,ds.y.values,ds.alt.values])
    site = (ds.attrs["lon_loc"], ds.attrs["lat_loc"])
    minalt = ds.attrs["alt_loc"]
    aeqd = ds.attrs["aeqd"]
            
    wkt = aeqd.to_wkt()
    crs_osr = osr.SpatialReference()
    crs_osr.ImportFromWkt(wkt)
            
    gridcoords, gridshape = make_3d_grid(
        site,
        crs_osr,
        maxrange,
        maxalt,
        horiz_res,
        vert_res,
        minalt=minalt,
    )

    pcappi = PseudoCAPPI(
        polcoords,
        gridcoords,
        maxrange=maxrange,
        ipclass=ipol.Idw,
        nnearest=8,
        p=2,
    )

    volume = np.ma.masked_invalid(pcappi(ds.DBZH.values)).reshape(gridshape)

    return volume, gridcoords

def CVol_to_CAPPI(ds : xr.Dataset, maxrange : float=240000, maxalt : float=20000, horiz_res=2000, vert_res=500):
    polcoords = np.column_stack([ds.x.values,ds.y.values,ds.alt.values])
    site = (ds.attrs["lon_loc"], ds.attrs["lat_loc"])
    minalt = ds.attrs["alt_loc"]
    aeqd = ds.attrs["aeqd"]
    minelev=ds.coords["elev"].values[0]
    maxelev=ds.coords["elev"].values[-1]
            
    wkt = aeqd.to_wkt()
    crs_osr = osr.SpatialReference()
    crs_osr.ImportFromWkt(wkt)
            
    gridcoords, gridshape = make_3d_grid(
        site,
        crs_osr,
        maxrange,
        maxalt,
        horiz_res,
        vert_res,
        minalt=minalt,
    )

    cappi = CAPPI(
        polcoords,
        gridcoords,
        maxrange=maxrange,
        minelev=minelev,
        maxelev=maxelev,    
        ipclass=ipol.Idw,
        nnearest=8,
        p=2,
    )

    volume = np.ma.masked_invalid(cappi(ds.DBZH.values)).reshape(gridshape)

    return volume, gridcoords

def CVol_to_EchoTOP(threshold, volume_cappi, gridcoords): 

    volume = volume_cappi.transpose((2, 1, 0))

    mask = volume >= threshold
    mask_rev = mask[...,::-1]

    idx_rev = np.argmax(mask_rev, axis=2)
    idx = (volume.shape[2] - 1) - idx_rev

    has_echo = np.any(mask, axis=2)
    idx = np.where(has_echo, idx, -1)

    z = np.unique(gridcoords[:,2])
    echo_top = np.where(has_echo, z[idx], np.nan)

    return echo_top

def Pvol_to_EchoTOP(threshold, pvol_dtree):
   
   return 

def main(pvol_dtree):
    ds = PVol_to_CVol(pvol_dtree)
    
    volume_pcappi, gridcoords_pcappi = CVol_to_PseudoCAPPI(ds)
    volume_cappi , gridcoords_cappi  = CVol_to_CAPPI(ds)

    print(np.unique_values(volume_cappi))
    print(np.unique_values(volume_pcappi))

    x_pcappi = np.unique(gridcoords_pcappi[:,0])
    y_pcappi = np.unique(gridcoords_pcappi[:,1])
    z_pcappi = np.unique(gridcoords_pcappi[:,2])

    x_cappi = np.unique(gridcoords_cappi[:,0])
    y_cappi = np.unique(gridcoords_cappi[:,1])
    z_cappi = np.unique(gridcoords_cappi[:,2])

    vis.plot_max_plan_and_vert(x_pcappi, y_pcappi, z_pcappi, volume_pcappi, cmap="turbo", unit="dBZH")  
    vis.plot_max_plan_and_vert(x_cappi, y_cappi, z_cappi, volume_cappi, cmap="turbo", unit="dBZH")
    plt.show()

    threshold = 25
    echo_top  = CVol_to_EchoTOP(threshold,volume_cappi,gridcoords_cappi)

    plt.pcolormesh(x_cappi, y_cappi, echo_top.T, shading='auto', cmap="turbo")
    plt.colorbar(label=f'Echo Top {threshold} dBZ (m)')
    plt.show()

if __name__ == "__main__":
    pvol_dtree = retrieve_PVol_dtree()
    main(pvol_dtree)
