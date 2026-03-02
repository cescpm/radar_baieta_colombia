ruta = r'./*.RAW*'

import glob
arch = glob.glob(ruta)

list(arch)

import pyart
radar = pyart.io.read(arch[0])

radar.info('compact')
radar.time['units']
radar.metadata['instrument_name']
list(radar.fields)

import numpy as np

z_linear = 10.0**(radar.fields['reflectivity']['data'] / 10.0)

a = 250.0  #300.0
b = 1.2  #1.4
rain_data = (z_linear / a) ** (1.0 / b)

# 3. Creem el camp correctament
precipitation = radar.fields['reflectivity'].copy()
precipitation['data'] = rain_data
precipitation['long_name'] = 'Rainfall Rate'
precipitation['units'] = 'mm/h'
precipitation['standard_name'] = 'rainfall_rate'

# Afegim el camp al radar
radar.add_field('precipitation', precipitation, replace_existing=True)

import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

def graf(data, var, vmin, vmax):
    radar_lat = data.latitude["data"][0]
    radar_lon = data.longitude["data"][0]
    elevation = data.elevation["data"][:]
    azimuth = data.azimuth["data"][:]
    print(radar_lat, radar_lon, elevation, azimuth)

    display = pyart.graph.RadarMapDisplay(data)

    fig = plt.figure(figsize=(10,10))
    ax = plt.axes(projection=ccrs.PlateCarree())

    display.plot_ppi_map(
        var,
        sweep=0,
        vmin=vmin,
        vmax=vmax,
        lat_0=radar_lat,
        lon_0=radar_lon,
        min_lon=radar_lon-2,
        max_lon=radar_lon+2,
        min_lat=radar_lat-2,
        max_lat=radar_lat+2,
        ax=ax
    )
    # Strong geographic references
    ax.coastlines(resolution='10m', linewidth=1)
    ax.add_feature(cfeature.BORDERS, linewidth=1)
    ax.add_feature(cfeature.STATES, linewidth=0.5)

    # Lat/Lon gridlines
    gl = ax.gridlines(draw_labels=True, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    ax.plot(radar_lon, radar_lat, 'ro', markersize=8)

graf(radar, 'precipitation', vmin=0, vmax=70) 
graf(radar, 'reflectivity', vmin=-10, vmax=70)

plt.show()
