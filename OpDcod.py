import os
import sys
import csv
import json
import xradar as xd
import xarray as xr
import wradlib as wrl
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
from collections import OrderedDict
from datetime import datetime, timezone
#----------------------------------------------------------------------------------------

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

def plot_features(ax):
    states = cfeature.STATES.with_scale('10m')
    ax.add_feature(states, edgecolor="black", lw=2, zorder=4)

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
        load_data=load_data,
        rawdata=rawdata,
        debug=debug,
    )
    return data_odict

def open_iris_dtree(filepath, decode_hclasse : bool = True):
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


def main():
    filepath = "data/raw/Tablazo/2025/10/16/TAB251016000004.RAWA68G"

    data_odict = open_iris_odict(filepath)

    #print(data_odict.keys())
    #print(data_odict["raw_product_bhdrs"])

    dt = open_iris_dtree(filepath)

    #print(dt)
    #print(dt["/sweep_0"].data_vars)

    variable = str(sys.argv[1])

    da = dt["/sweep_0"][variable]
    #print(da)

    if variable == "DB_HCLASS":

        get = np.vectorize(lambda t: t[0])

        get_values = get(dt["/sweep_0"]["DB_HCLASS"].values)

        # 3. Creem el nou DataArray amb els mateixos eixos i coordenades
        da = xr.DataArray(
            data = get_values[:,:332],
            coords={
                'azimuth'   : dt["/sweep_0"].azimuth.values,
                'range'     : dt["/sweep_0"].range.values[::2],
                'longitude' : dt["/sweep_0"].longitude.values,
                'latitude'  : dt["/sweep_0"].latitude.values,
                'altitude'  : dt["/sweep_0"].altitude.values,
                'elevation' : ('azimuth',dt["/sweep_0"].elevation.values),
            },
            dims = ['azimuth', 'range'],
            name = "DB_HCLASS"
        )

        da.attrs["sweep_mode"]        = dt["/sweep_0"]["sweep_mode"].values
        da.attrs["sweep_number"]      = dt["/sweep_0"]["sweep_number"].values
        da.attrs["prt_mode"]          = dt["/sweep_0"]["prt_mode"].values
        da.attrs["follow_mode"]       = dt["/sweep_0"]["follow_mode"].values
        da.attrs["sweep_fixed_angle"] = dt["/sweep_0"]["sweep_fixed_angle"].values

        fig = plt.figure(figsize=(20,10))

        ax1 = fig.add_subplot(121)
        ax2 = fig.add_subplot(122, projection=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values))

        da_geo = da.wrl.georef.georeference()

        pm1 = wrl.vis.plot(da, x="range", y="azimuth", ax=ax1, levels=METEO_TABLE.keys(), add_colorbar=False)

        plot_features(ax2)
        pm2 = wrl.vis.plot(da_geo, ax=ax2, alpha=0.95, levels=METEO_TABLE.keys(), transform=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values), add_colorbar=False)

        cb = plt.colorbar(pm2, ax=ax2,extend="both")
        cb.ax.set_yticklabels(METEO_TABLE.values())

        plt.tight_layout()
        plt.show()
    
    else:

        da.attrs["sweep_mode"] = dt["/sweep_0"]["sweep_mode"].values

        da_geo = da.wrl.georef.georeference()

        fig = plt.figure(figsize=(20,10))

        ax1 = fig.add_subplot(121)
        ax2 = fig.add_subplot(122, projection=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values))

        wrl.vis.plot(
            da,
            x="range",
            y="azimuth",
            ax=ax1,
            cmap="tab20c",
            add_colorbar=False,
        )
        
        plot_features(ax2)
        wrl.vis.plot(
            da_geo,
            ax=ax2,
            cmap="tab20c",
            transform=ccrs.AzimuthalEquidistant(central_longitude=da.longitude.values, central_latitude=da.latitude.values),
            add_colorbar=True,
        )
        plt.tight_layout()
        plt.show()


if __name__ == '__main__':
    filepath = "data/raw/Tablazo/2025/10/16/TAB251016000004.RAWA68G"

    