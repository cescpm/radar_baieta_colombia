from wradlib.georef.polar import spherical_to_xyz 
from wradlib.vpr import make_3d_grid, PseudoCAPPI
from RAW_PVOL import main
from osgeo import osr
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

    x_Cvol    = np.concatenate(x_Cscan)
    y_Cvol    = np.concatenate(y_Cscan)
    lon_Cvol  = np.concatenate(lon_Cscan)
    lat_Cvol  = np.concatenate(lat_Cscan)
    alt_Cvol  = np.concatenate(alt_Cscan)
    dbzh_Cvol = np.concatenate(dbzh_Cscan)

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
        },
        attrs                 = {
            "instrument_name" : pvol_dtree["/"].attrs["instrument_name"],
            "lon_loc"         : pvol_dtree["/radar_parameters"].coords["longitude"].values,
            "lat_loc"         : pvol_dtree["/radar_parameters"].coords["latitude"].values,
            "alt_loc"         : pvol_dtree["/radar_parameters"].coords["altitude"].values,
            "crs"             : aeqd,
        },
    )

    return Cvol_ds


def CartesianVol_to_PseudoCAPPI(ds, maxrange : float =25000, maxalt : float =20000, horiz_res=250, vert_res=500):
        polcoords = np.column_stack([ds.x.values,ds.y.values,ds.alt.values])
        site = (ds.attrs["lon_loc"], ds.attrs["lat_loc"])
        minalt = ds.attrs["alt_loc"]
        crs = ds.attrs["crs"].to_osr()
        
        gridcoords = make_3d_grid(
             site,
             crs,
             maxrange,
             maxalt,
             horiz_res,
             vert_res,
             minalt,
        )

        return gridcoords
if __name__ == "__main__":
    pvol_dtree = main()
    ds = polcoords_dtree_to_CartesianVol(pvol_dtree)
    print(CartesianVol_to_PseudoCAPPI(ds))