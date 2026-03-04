import gzip
import shutil
import tempfile
import os
import xradar as xd
import xarray as xr
import numpy as np

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
dt = xd.io.open_cfradial1_datatree('data/raw/Bogota/2023/09/22/1399BOG-20230922-234953-PPIVol-9d1b.nc')

print(dt['sweep_0'].elevation.attrs['units'])

# 1. Get the sweep dataset (as you did before)
ds = dt['sweep_0'].ds

##############################################################################################
DBZH_plot=True
if DBZH_plot:
    import wradlib as wrl
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

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
    #ax.add_feature(cfeature.GSHHSFeature(scale='f'), edgecolor='black', linewidth=2, zorder=3)

    import cartopy.io.shapereader as shpreader
    from shapely.geometry import Point

    # Path to the shapefile you download (Admin 3 / Localidades)
    path_to_localidades = 'col-administrative-divisions-shapefiles/col_admbnda_adm2_mgn_20200416.shp'

    # Load and plot the internal city sectors
    reader = shpreader.Reader(path_to_localidades)

    radar_center = Point(ds.longitude.values,ds.latitude.values)

    geoms_to_draw = []
    for record in reader.records():
        # Quick distance check in degrees (approximate but very fast)
        if record.geometry.distance(radar_center) < 0.4: 
            geoms_to_draw.append(record.geometry)

    if geoms_to_draw:
        ax.add_geometries(geoms_to_draw, ccrs.PlateCarree(),
                        facecolor='none', edgecolor='darkgrey', 
                        linewidth=0.8, zorder=1)

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
#---------------------------------------------------------------------------------------------

##############################################################################################
KDP_plot=True
if KDP_plot:
    import wradlib as wrl
    import xarray as xr
    import numpy as np

    # 7. Plotting
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature

    # 1. Extract the sweep dataset
    ds = dt['sweep_0'].ds

    # 1. Georeference to create the 2D 'x' and 'y' coordinates
    ds = ds.wrl.georef.georeference()

    # 2. Calculate KDP (assuming PHIDP and RHOHV exist in your variables)
    # If your variables have different names (like 'PHIH'), change them here:
    phidp = ds.PHIDP 
    rhohv = ds.RHOHV

    # Filter and calculate
    kdp = phidp.where(rhohv > 0.9).wrl.dp.kdp_from_phidp(window=15)
    rain_kdp = 40.5 * (kdp.clip(min=0)**0.85)

    # Add to dataset
    ds = ds.assign(RAIN_KDP=rain_kdp)

    # 3. Plotting using the 2D 'x' and 'y' coordinates
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
    path_to_localidades = 'col-administrative-divisions-shapefiles/col_admbnda_adm2_mgn_20200416.shp'

    # Load and plot the internal city sectors
    reader = shpreader.Reader(path_to_localidades)

    radar_center = Point(ds.longitude.values,ds.latitude.values)

    geoms_to_draw = []
    for record in reader.records():
        # Quick distance check in degrees (approximate but very fast)
        if record.geometry.distance(radar_center) < 0.4: 
            geoms_to_draw.append(record.geometry)

    if geoms_to_draw:
        ax.add_geometries(geoms_to_draw, ccrs.PlateCarree(),
                        facecolor='none', edgecolor='darkgrey', 
                        linewidth=0.8, zorder=1)

    # 4. Coastlines (Blue - will be off-screen unless you zoom out)
    ax.add_feature(cfeature.COASTLINE.with_scale('10m'), edgecolor='blue', linewidth=1, zorder=3)
    ax.set_extent([-60000, 60000, -60000, 60000], crs=proj)

    # Plot using x and y
    ds.RAIN_KDP.plot(
        x='x', y='y', 
        ax=ax, 
        cmap='viridis', 
        vmin=0, vmax=70
    )

    # Set extent to 60km (units are meters)
    ax.set_extent([-60000, 60000, -60000, 60000], crs=proj)
    plt.show()


##############################################################################################
import matplotlib.colors as mcolors
import numpy as np

_rr_bounds = [0.0, 0.1, 0.2, 0.5, 1.0, 2.0, 4.0, 10.0, 24.0, 200.0]
_rr_colors = [
    (0,       0,       0      ),
    (10/255,  155/255, 225/255),   # 0.1  light     – blue
    (5/255,   205/255, 170/255),   # 0.2  light     – teal
    (140/255, 230/255,  20/255),   # 0.5  moderate  – lime
    (240/255, 240/255,  20/255),   # 1    moderate  – yellow
    (255/255, 205/255,  20/255),   # 2    moderate  – amber
    (255/255, 150/255,  50/255),   # 4    heavy     – orange
    (255/255,  80/255,  60/255),   # 10   heavy     – red
    (250/255, 120/255, 255/255)   # 24   heavy     – pink/purple
]
# BoundaryNorm maps each interval to its color (no interpolation between steps)
RR_CMAP = mcolors.ListedColormap(_rr_colors, name='RainRate')
RR_NORM  = mcolors.BoundaryNorm(_rr_bounds, ncolors=len(_rr_colors))

ZR_plot=True
if ZR_plot:
    import wradlib as wrl
    import xarray as xr
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs

    # 1. Clean the Reflectivity
    # Radar data often has 'No Data' values set to -9999 or very low negatives.
    # We set anything below 0 dBZ to 0 so the math doesn't break.
    dbz = ds.DBZH.where(ds.DBZH > 0, 0)

    # 2. Convert to Linear Z
    # If your DBZH values are already very large (e.g. 1000s), skip this step.
    z_linear = wrl.trafo.idecibel(dbz)

    # 3. Convert to Rain (R)
    # Using a=300, b=1.4 (Convective) makes the formula MORE sensitive to rain
    rain_z = wrl.zr.z_to_r(z_linear, a=250, b=1.2)

    # 4. Add to dataset
    ds = ds.assign(precip_z=rain_z)

    # 5. Georeference (Crucial for x/y coordinates)
    ds = ds.wrl.georef.georeference()

    # 6. Plotting with Fixed Color Scale
    fig = plt.figure(figsize=(10, 8))
    proj = ccrs.AzimuthalEquidistant(central_longitude=ds.longitude.values, 
                                    central_latitude=ds.latitude.values)
    ax = plt.axes(projection=proj)

    import cartopy.io.shapereader as shpreader
    from shapely.geometry import Point

    # Path to the shapefile you download (Admin 3 / Localidades)
    path_to_localidades = 'col-administrative-divisions-shapefiles/col_admbnda_adm2_mgn_20200416.shp'

    # Load and plot the internal city sectors
    reader = shpreader.Reader(path_to_localidades)

    radar_center = Point(ds.longitude.values,ds.latitude.values)

    geoms_to_draw = []
    for record in reader.records():
        # Quick distance check in degrees (approximate but very fast)
        if record.geometry.distance(radar_center) < 0.4: 
            geoms_to_draw.append(record.geometry)

    if geoms_to_draw:
        ax.add_geometries(geoms_to_draw, ccrs.PlateCarree(),
                        facecolor='none', edgecolor='white', 
                        linewidth=0.8, zorder=10)

    # We set vmin to 0.1 so we don't plot the 'noise' 
    # We set vmax to 20 to see the rain structure clearly
    mesh = ds.precip_z.plot(
        x='x', y='y', ax=ax, 
        cmap=RR_CMAP,
        norm=RR_NORM,
        add_colorbar=True,
        cbar_kwargs={'label': 'Rainfall Rate (mm/h)'}
    )

    ax.set_extent([-60000, 60000, -60000, 60000], crs=proj)
    plt.show()