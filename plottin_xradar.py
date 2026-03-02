import gzip
import shutil
import tempfile
import os
import xradar as xd
import xarray as xr

def open_compressed_radar(gz_path):
    with tempfile.NamedTemporaryFile(suffix='.nc', delete=False) as tmp:
        with gzip.open(gz_path, 'rb') as f_in:
            shutil.copyfileobj(f_in, tmp)
        temp_name = tmp.name

    try:
        # CHANGE THIS LINE:
        # Use cfradial1 instead of iris
        dt = xd.io.open_cfradial1_datatree(temp_name)
        return dt
    except Exception as e:
        print(f"Error opening file: {e}")
        # If cfradial1 fails, you can try:
        # dt = xd.io.open_cfradial2_datatree(temp_name)
    finally:
        if os.path.exists(temp_name):
            os.remove(temp_name)


# Usage:
dt = open_compressed_radar('1399BOG-20251016-000457-PPIVol-4007.nc.gz')
print(dt['/sweep_0']['range'])

import wradlib as wrl
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# 1. Get the sweep dataset (as you did before)
ds = dt['sweep_0'].ds

# 2. Georeference: Calculate the Lat/Lon of every radar gate
# This adds 'x', 'y', and 'z' coordinates to your dataset
ds = ds.wrl.georef.georeference()

fig = plt.figure(figsize=(10, 8))
# Use a specific projection (e.g. AzimuthalEquidistant centered on the radar)
proj = ccrs.AzimuthalEquidistant(
    central_longitude=ds.longitude.values, 
    central_latitude=ds.latitude.values
)
ax = plt.axes(projection=proj)
# 2. National Borders (Black)
ax.add_feature(cfeature.BORDERS.with_scale('10m'), edgecolor='black', linewidth=2, zorder=3)

import cartopy.io.shapereader as shpreader
from shapely.geometry import Point

# Path to the shapefile you download (Admin 3 / Localidades)
path_to_localidades = 'shp_crveredas_2024/shp_CRVeredas_2024.shp'

# Load and plot the internal city sectors
reader = shpreader.Reader(path_to_localidades)

radar_center = Point(ds.longitude.values,ds.latitude.values)

geoms_to_draw = []
for record in reader.records():
    # Quick distance check in degrees (approximate but very fast)
    if record.geometry.distance(radar_center) < 0.2: 
        geoms_to_draw.append(record.geometry)

if geoms_to_draw:
    ax.add_geometries(geoms_to_draw, ccrs.PlateCarree(),
                      facecolor='none', edgecolor='darkgrey', 
                      linewidth=0.8, zorder=1)

# 3. INTERNAL DEPARTMENTS (Red - so you can see them!)
# This is what you need since you are inland
departments = cfeature.NaturalEarthFeature(
    category='cultural',
    name='admin_1_states_provinces_lines',
    scale='10m',
    facecolor='none')
ax.add_feature(departments, edgecolor='red', linewidth=1, zorder=3)

# 4. Coastlines (Blue - will be off-screen unless you zoom out)
ax.add_feature(cfeature.COASTLINE.with_scale('10m'), edgecolor='blue', linewidth=1, zorder=3)
ax.set_extent([-60000, 60000, -60000, 60000], crs=proj)

# 4. PLOT using x and y (calculated by georeference)
# Note: x and y are now 2D arrays (azimuth x range)
plot = ds.DBZH.plot.pcolormesh(
    x='x', y='y', 
    ax=ax,
    cmap='NWSRef',
    vmin=-10, vmax=60,
    add_colorbar=True,
    cbar_kwargs={'label': 'Reflectivity (dBZ)', 'shrink': 0.8}
)

# Mark the radar location (center)
ax.plot(ds.longitude.values, ds.latitude.values, 'ro', transform=ccrs.PlateCarree())

plt.title(f"Radar Reflectivity: {dt.attrs['instrument_name']}")
plt.show()

