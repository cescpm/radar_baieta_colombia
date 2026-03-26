from wradlib.georef.polar import spherical_to_xyz 
from RAW_PVOL import main
import numpy as np
from pyproj import Transformer
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import xarray as xr
#----------------------------------------------------------------------------------------

def polcoords_dtree_to_CartesianVol(pvol_dtree):

    radar_alt = float(pvol_dtree["/"]["altitude"].values)
    radar_lon = float(pvol_dtree["/"]["longitude"].values)
    radar_lat = float(pvol_dtree["/"]["latitude"].values)

    site = (radar_lon, radar_lat, radar_alt)
    
    lon_Cscan, x_Cscan  = [], []
    lat_Cscan, y_Cscan  = [], []
    alt_Cscan, z_Cscan  = [], []
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
        z_Cscan.append(z.ravel())
        dbzh_Cscan.append(dbzh.ravel())

    x_Cvol    = np.concatenate(x_Cscan)
    y_Cvol    = np.concatenate(y_Cscan)
    z_Cvol    = np.concatenate(z_Cscan)
    lon_Cvol  = np.concatenate(lon_Cscan)
    lat_Cvol  = np.concatenate(lat_Cscan)
    alt_Cvol  = np.concatenate(alt_Cscan)
    dbzh_Cvol = np.concatenate(dbzh_Cscan)

    Cvol_ds = xr.Dataset(
        data_vars={
            DBZH=(["x","y","z","longitude","latitude","altitude"], dbzh_Cvol),
        },
        coords={
            x
        }
    )

    return lon_Cvol, lat_Cvol, alt_Cvol, dbzh_Cvol


#def echoTOP():


if __name__ == "__main__":
    pvol_dtree = main()
    lon, lat, alt, dbzh = polcoords_dtree_to_CartesianVol(pvol_dtree)

    