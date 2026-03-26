from wradlib.georef.polar import spherical_to_xyz       
from RAW_PVOL import main
import numpy as np
from pyproj import Transformer
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
#----------------------------------------------------------------------------------------

def polcoords_dtree_to_CartesianVol(pvol_dtree):

    radar_alt = float(pvol_dtree["/"]["altitude"].values)
    radar_lon = float(pvol_dtree["/"]["longitude"].values)
    radar_lat = float(pvol_dtree["/"]["latitude"].values)

    site = (radar_lon, radar_lat, radar_alt)
    
    lon_list, lat_list, alt_list, dbzh_list = [], [], [], []
    n_sweeps = [g for g in pvol_dtree.groups if g.startswith("/sweep_")]

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

        transformer = Transformer.from_crs(aeqd, "EPSG:4326")
    
        x = xyz[...,0]
        y = xyz[...,1]
        z = xyz[...,2]

        lon, lat = transformer.transform(x, y)
        alt = z

        lon_list.append(lon.ravel())
        lat_list.append(lat.ravel())
        alt_list.append(alt.ravel())
        dbzh_list.append(dbzh.ravel())

    lon_all = np.concatenate(lon_list)
    lat_all = np.concatenate(lat_list)
    alt_all = np.concatenate(alt_list)
    dbzh_all = np.concatenate(dbzh_list)

    return lon_all, lat_all, alt_all, dbzh_all


def cappi_from_points(lon, lat, alt, dbzh, target_alt, tol=100, resolution=0.01):
    """
    Genera un CAPPI a una altura constant (target_alt, en metres MSL).
    lon, lat, alt, dbzh : arrays 1D amb les dades de tots els punts.
    tol: franja d'altura (en metres) al voltant de target_alt.
    resolution: espaiat de la malla en graus.
    Retorna (lon_grid, lat_grid, cappi_grid) on cappi_grid és la reflectivitat interpolada.
    """
    # Filtrar punts propers a l'altura desitjada
    mask = np.abs(alt - target_alt) < tol
    lon_filt = lon[mask]
    lat_filt = lat[mask]
    dbzh_filt = dbzh[mask]

    if len(lon_filt) == 0:
        raise ValueError(f"No hi ha punts a l'altura {target_alt} ± {tol} m")

    # Límits de la malla
    lon_min, lon_max = lon_filt.min(), lon_filt.max()
    lat_min, lat_max = lat_filt.min(), lat_filt.max()

    # Crear malla regular en longitud/latitud
    lon_grid = np.arange(lon_min, lon_max + resolution, resolution)
    lat_grid = np.arange(lat_min, lat_max + resolution, resolution)
    LON, LAT = np.meshgrid(lon_grid, lat_grid, indexing='ij')

    # Interpolació
    points = np.column_stack((lon_filt, lat_filt))
    cappi_grid = griddata(points, dbzh_filt, (LON, LAT), method='linear')

    return LON, LAT, cappi_grid

if __name__ == "__main__":
    pvol_dtree = main()
    lon, lat, alt, dbzh = polcoords_dtree_to_CartesianVol(pvol_dtree)
    print("Altura mínima:", alt.min())
    print("Altura màxima:", alt.max())
    print("Punts entre 4400 i 4600 m:", np.sum((alt > 3400) & (alt < 3000)))

    # Definir l'altura del CAPPI (en metres sobre el nivell del mar)
    target_alt = 6000.0  # per exemple

    # Generar el CAPPI
    LON, LAT, cappi = cappi_from_points(lon, lat, alt, dbzh, target_alt, tol=100, resolution=0.01)

    # Visualitzar
    plt.figure(figsize=(10, 8))
    plt.pcolormesh(LON, LAT, cappi, shading='auto', cmap='viridis')
    plt.colorbar(label='DBZH (dBZ)')
    plt.xlabel('Longitud')
    plt.ylabel('Latitud')
    plt.title(f'CAPPI a {target_alt} m MSL')
    plt.show()
    
    