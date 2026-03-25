from wradlib.georef import sweep_centroids
from RAW_PVOL import main
#----------------------------------------------------------------------------------------

def polcoords_dtree_to_CartesianVol(pvol_dtree):

    radar_alt = float(pvol_dtree["/"]["altitude"].values)
    radar_lon = float(pvol_dtree["/"]["longitude"].values)
    radar_lat = float(pvol_dtree["/"]["latitude"].values)

    site = (radar_lon, radar_lat, radar_alt)
    
    polcoord_list = []
    data_list = []

    n_sweeps = [g for g in pvol_dtree.groups if g.startswith("/sweep_")]

    for sweep in n_sweeps:
        scan = pvol_dtree[sweep]

        azimuth = scan["azimuth"].data
        range = scan["range"].data
        elevation = scan["elevation"].data

        range_bins = range[:-1]-range[1:]

        dbzh = scan["DBZH"].data

        coords = sweep_centroids(
            nrays=len(azimuth),
            rscale=range_bins[0],
            nbins=len(range),
            elangle=elevation.mean,
        )

    return azimuth, range_bins, elevation

if __name__ == "__main__":
    pvol_dtree = main()
    print(pvol_dtree)

    print(polcoords_dtree_to_CartesianVol(pvol_dtree))