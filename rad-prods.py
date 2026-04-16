from wradlib.georef.polar import spherical_to_xyz 
from wradlib.vpr import make_3d_grid, PseudoCAPPI, CAPPI
from wradlib import ipol
from wradlib import vis
from wradlib import comp
from wradlib.zr import z_to_r
from wradlib.trafo import idecibel, kdp_to_r
from rad_BAndRe import retrieve_ScanVol_dtree, retrieve_lower_scans
from OpDcod import open_iris_dtree, METEO_TABLE, PRECIP_TABLE, CELL_TABLE
from osgeo import osr
import numpy as np
from pyproj import Transformer
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
import xarray as xr
import xradar as xd
import cartopy.crs as ccrs
import cartopy.feature as cfeature
#----------------------------------------------------------------------------------------

def ScanVol_to_CVol(Svol_dtree):

    radar_alt = float(Svol_dtree["/"]["altitude"].values)
    radar_lon = float(Svol_dtree["/"]["longitude"].values)
    radar_lat = float(Svol_dtree["/"]["latitude"].values)

    site = (radar_lon, radar_lat, radar_alt)
        
    lon_Cscan, x_Cscan  = [], []
    lat_Cscan, y_Cscan  = [], []
    alt_Cscan           = []
    elev_Cscan          = []
    dbzh_Cscan          = []
        
    n_sweeps = [
        sweep_name 
        for sweep_name 
        in Svol_dtree.groups 
        if sweep_name.startswith("/sweep_")
    ]

    for sweep in n_sweeps:
        scan = Svol_dtree[sweep]

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
            "instrument_name" : Svol_dtree["/"].attrs["instrument_name"],
            "lon_loc"         : Svol_dtree["/radar_parameters"].coords["longitude"].values,
            "lat_loc"         : Svol_dtree["/radar_parameters"].coords["latitude"].values,
            "alt_loc"         : Svol_dtree["/radar_parameters"].coords["altitude"].values,
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

def ScanVol_to_EchoTOP(threshold, pvol_dtree):
   #llibreria echotop https://github.com/vlouf/eth_radar
   return 

def ReflCVol_to_PrecipCVol(volume_cappi_dBZ,a,b):
    volume_cappi_Z = idecibel(volume_cappi_dBZ)
    volume_cappi_R = z_to_r(volume_cappi_Z,a=a,b=b)
    return volume_cappi_R

def KDPCVol_to_PrecipCVol(volume_cappi_KDP,a,b):
    volume_cappi_R = kdp_to_r(volume_cappi_KDP,f=5)
    return volume_cappi_R

def main(Svol_dtree):
    ds = ScanVol_to_CVol(Svol_dtree)
    
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

    threshold = 45
    echo_top  = CVol_to_EchoTOP(threshold,volume_cappi,gridcoords_cappi)

    plt.pcolormesh(x_cappi, y_cappi, echo_top.T, shading='auto', cmap="turbo")
    plt.colorbar(label=f'Echo Top {threshold} dBZ (m)')
    plt.show()

    volume_cappi_R = ReflCVol_to_PrecipCVol(volume_cappi,250,1.2)

    vis.plot_max_plan_and_vert(x_cappi, y_cappi, z_cappi, volume_cappi_R, cmap="turbo", unit="mm/h")
    plt.show()


if __name__ == "__main__":
    #Svol_dtree = retrieve_ScanVol_dtree()
    #main(Svol_dtree)

    lowscans_per_hour = retrieve_lower_scans()

    class_acumulat_meteor = None
    class_acumulat_precip = None
    class_acumulat_storm  = None

    azimuths_angles = np.arange(0.5, 360, 1.0) 

    for lowscan in lowscans_per_hour[-1]:
        Scan_dtree = open_iris_dtree(lowscan)
        
        n_sweeps = [
            sweep_name 
            for sweep_name 
            in Scan_dtree.groups 
            if sweep_name.startswith("/sweep_")
        ]

        first_sweep = n_sweeps[0]

        da_meteor = Scan_dtree[first_sweep]["DB_HCLASS_meteor"]
        da_precip = Scan_dtree[first_sweep]["DB_HCLASS_precip"]
        da_storm  = Scan_dtree[first_sweep]["DB_HCLASS_storm"]

        meteor_values = da_meteor.values[:,:332]
        precip_values = da_precip.values[:,:332]
        storm_values = da_storm.values[:,:332]

        da_meteor = da_meteor.isel(range=slice(0, 664, 2))
        da_precip = da_precip.isel(range=slice(0, 664, 2))
        da_storm = da_storm.isel(range=slice(0, 664, 2))

        da_meteor.values = meteor_values
        da_precip.values = precip_values
        da_storm.values = storm_values

        da_meteor_maskd = da_meteor.where(da_meteor >= 5)
        da_precip_maskd = da_precip.where(da_precip >= 5)
        da_storm_maskd = da_storm.where(da_storm  == 1)

        da_meteor_maskd_alignd = da_meteor_maskd.reindex(azimuth=azimuths_angles, method="nearest", tolerance=0.5)
        da_precip_maskd_alignd = da_precip_maskd.reindex(azimuth=azimuths_angles, method="nearest", tolerance=0.5)
        da_storm_maskd_alignd = da_storm_maskd.reindex(azimuth=azimuths_angles, method="nearest", tolerance=0.5)

        if class_acumulat_meteor is None:
            class_acumulat_meteor = da_meteor_maskd_alignd

            class_acumulat_meteor.attrs["sweep_mode"]        = Scan_dtree[first_sweep]["sweep_mode"].values
            class_acumulat_meteor.attrs["sweep_number"]      = Scan_dtree[first_sweep]["sweep_number"].values
            class_acumulat_meteor.attrs["prt_mode"]          = Scan_dtree[first_sweep]["prt_mode"].values
            class_acumulat_meteor.attrs["follow_mode"]       = Scan_dtree[first_sweep]["follow_mode"].values
            class_acumulat_meteor.attrs["sweep_fixed_angle"] = Scan_dtree[first_sweep]["sweep_fixed_angle"].values
        else:
            class_acumulat_meteor = xr.apply_ufunc(np.fmax, class_acumulat_meteor, da_meteor_maskd_alignd, keep_attrs=True)

        if class_acumulat_precip is None:
            class_acumulat_precip = da_precip_maskd_alignd

            class_acumulat_precip.attrs["sweep_mode"]        = Scan_dtree[first_sweep]["sweep_mode"].values
            class_acumulat_precip.attrs["sweep_number"]      = Scan_dtree[first_sweep]["sweep_number"].values
            class_acumulat_precip.attrs["prt_mode"]          = Scan_dtree[first_sweep]["prt_mode"].values
            class_acumulat_precip.attrs["follow_mode"]       = Scan_dtree[first_sweep]["follow_mode"].values
            class_acumulat_precip.attrs["sweep_fixed_angle"] = Scan_dtree[first_sweep]["sweep_fixed_angle"].values
        else:
            class_acumulat_precip = xr.apply_ufunc(np.fmax, class_acumulat_precip, da_precip_maskd_alignd, keep_attrs=True)

        if class_acumulat_storm is None:
            class_acumulat_storm = da_storm_maskd_alignd

            class_acumulat_storm.attrs["sweep_mode"]        = Scan_dtree[first_sweep]["sweep_mode"].values
            class_acumulat_storm.attrs["sweep_number"]      = Scan_dtree[first_sweep]["sweep_number"].values
            class_acumulat_storm.attrs["prt_mode"]          = Scan_dtree[first_sweep]["prt_mode"].values
            class_acumulat_storm.attrs["follow_mode"]       = Scan_dtree[first_sweep]["follow_mode"].values
            class_acumulat_storm.attrs["sweep_fixed_angle"] = Scan_dtree[first_sweep]["sweep_fixed_angle"].values
        else:
            class_acumulat_storm = xr.apply_ufunc(np.fmax, class_acumulat_storm, da_storm_maskd_alignd, keep_attrs=True)


    class_acumulat_meteor.to_netcdf("meteor.nc")

    #fig = plt.figure(figsize=(20,10))
#
    #ax1 = fig.add_subplot(131, projection=ccrs.AzimuthalEquidistant(central_longitude=class_acumulat_meteor.longitude.values, central_latitude=class_acumulat_meteor.latitude.values))
#
    #class_acumulat_meteor_geo = class_acumulat_meteor.wrl.georef.georeference()
#
#
    #states = cfeature.STATES.with_scale('10m')
    #ax1.add_feature(states, edgecolor="black", lw=2, zorder=4)
    #pm1 = vis.plot(class_acumulat_meteor_geo, ax=ax1, alpha=0.95, levels=METEO_TABLE.keys(), transform=ccrs.AzimuthalEquidistant(central_longitude=class_acumulat_meteor.longitude.values, central_latitude=class_acumulat_meteor.latitude.values), add_colorbar=False)
#
#
    #cb = plt.colorbar(pm1, ax=ax1,extend="both",shrink=0.5,  orientation="horizontal", location="bottom")
    ##cb.ax.set_yticklabels(METEO_TABLE.values())
#
    #ax2 = fig.add_subplot(132, projection=ccrs.AzimuthalEquidistant(central_longitude=class_acumulat_precip.longitude.values, central_latitude=class_acumulat_precip.latitude.values))
  #
    #class_acumulat_precip_geo = class_acumulat_precip.wrl.georef.georeference()
  #
  #
    #states = cfeature.STATES.with_scale('10m')
    #ax2.add_feature(states, edgecolor="black", lw=2, zorder=4)
    #pm2 = vis.plot(class_acumulat_precip_geo, ax=ax2, alpha=0.95, levels=PRECIP_TABLE.keys(), transform=ccrs.AzimuthalEquidistant(central_longitude=class_acumulat_precip.longitude.values, central_latitude=class_acumulat_precip.latitude.values), add_colorbar=False)
  #
  #
    #cb = plt.colorbar(pm2, ax=ax2,extend="both",shrink=0.5,orientation="horizontal", location="bottom")
    ##cb.ax.set_yticklabels(PRECIP_TABLE.values())
  #
    #ax3 = fig.add_subplot(133, projection=ccrs.AzimuthalEquidistant(central_longitude=class_acumulat_storm.longitude.values, central_latitude=class_acumulat_storm.latitude.values))
  #
    #class_acumulat_storm_geo = class_acumulat_storm.wrl.georef.georeference()
  #
  #
    #states = cfeature.STATES.with_scale('10m')
    #ax3.add_feature(states, edgecolor="black", lw=2, zorder=4)
    #pm3 = vis.plot(class_acumulat_storm_geo, ax=ax3, alpha=0.95, levels=CELL_TABLE.keys(), transform=ccrs.AzimuthalEquidistant(central_longitude=class_acumulat_storm.longitude.values, central_latitude=class_acumulat_storm.latitude.values), add_colorbar=False)
  #
  #
    #cb = plt.colorbar(pm3, ax=ax3,extend="both",shrink=0.5, orientation="horizontal", location="bottom")
    ##cb.ax.set_yticklabels(CELL_TABLE.values())
#
    #plt.tight_layout()
    #plt.show()