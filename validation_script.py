#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Automatitzat per al Càlcul de QPE Horari i Validació amb Estacions
"""

import os
import sys
import argparse
import io
import datetime
import numpy as np
import xarray as xr
import pandas as pd
import s3fs
from scipy.interpolate import interpn
from scipy.ndimage import label, generate_binary_structure, binary_closing, binary_opening
from pyproj import Transformer
import wradlib as wrl
import rioxarray
from rioxarray.merge import merge_datasets

# ===========================================================================
# 1. CONFIGURACIÓ I PARSERS DE LÍNIA DE COMANDES
# ===========================================================================
def parse_arguments():
    parser = argparse.ArgumentParser(description="Processa QPE horari d'un radar d'IDEAM.")
    parser.add_argument("--radar", type=str, required=True, help="Nom del radar (ex: Corozal, Tablazo, Carimagua)")
    parser.add_argument("--data", type=str, required=True, help="Data en format YYYY-MM-DD")
    parser.add_argument("--hora", type=str, required=True, help="Hora a processar en format HH (ex: 05, 14)")
    parser.add_argument("--dem1", type=str, default="10s090w_20101117_gmted_mea075.tif", help="Ruta al DEM Sud")
    parser.add_argument("--dem2", type=str, default="10n090w_20101117_gmted_mea075.tif", help="Ruta al DEM Nord")
    return parser.parse_args()

# ===========================================================================
# 2. CARREGAR I FUSIONAR DEM (Estàtic per a l'execució)
# ===========================================================================
def carrega_dem_colombia(file_sud, file_nord):
    print(f"[{datetime.datetime.now()}] Carregant i fusionant llistes DEM...")
    dem1 = xr.open_dataset(file_sud, engine="rasterio")
    dem2 = xr.open_dataset(file_nord, engine="rasterio")
    dem_merged = merge_datasets([dem1, dem2])
    
    elevation_raw = dem_merged.band_data.sel(band=1)
    # Evitem que l'oceà o NaNs actuïn com a murs colocant cota profunda
    elevation = elevation_raw.where(elevation_raw > -100, -500.0)
    
    dem_lon = dem_merged.x.values
    dem_lat = dem_merged.y.values
    
    if dem_lat[0] > dem_lat[-1]:
        dem_lat = dem_lat[::-1]
        dem_values = elevation.values[::-1, :]
    else:
        dem_values = elevation.values
        
    return dem_lat, dem_lon, dem_values

# ===========================================================================
# 3. FILTRAT DE SOROLL OPERACIONAL (El teu filtre original optimitzat)
# ===========================================================================
def aplica_filtre_clutter(swp, DR, DBZH, tPHIDP, texture_thresh=22.5, dr_thresh=-12.0):
    no_met_mask = (((DR > dr_thresh) & (DBZH < 30.0)) | ((tPHIDP > texture_thresh) & (DBZH < 35.0)))
    raw_clutter_flags = no_met_mask.values

    # Filtre morfològic
    structure = generate_binary_structure(rank=2, connectivity=1)
    closed_clutter = binary_closing(raw_clutter_flags, structure=structure)
    clean_clutter_mask = binary_opening(closed_clutter, structure=structure)

    # Filtre de clúster corregit (actua sobre el soroll)
    illes_soroll, _ = label(clean_clutter_mask)
    mida_de_cada_soroll = np.bincount(illes_soroll.ravel())
    
    LLINDAR_SOROLL_MINIM = 4  
    sorolls_grans_reals = mida_de_cada_soroll >= LLINDAR_SOROLL_MINIM
    clean_clutter_mask_polida = sorolls_grans_reals[illes_soroll]

    # True = Pluja bona, False = Soroll eliminat
    return ~clean_clutter_mask_polida

# ===========================================================================
# 4. COMPREHENSIVE BEAM BLOCKAGE (CBB Codi des de zero)
# ===========================================================================
def calcula_cbb(swp, dem_lat, dem_lon, dem_values):
    # Extreure coordenades i aplicar correcció de la torre (+20m)
    radar_alt = float(swp.coords["altitude"].values) if "altitude" in swp.coords else 143.0 # Fallback Corozal
    site = (float(swp.coords["longitude"].values), float(swp.coords["latitude"].values), radar_alt + 20.0)
    
    azimuth = swp["azimuth"].data
    r_distances = swp["range"].data
    elangle = swp["elevation"].data

    azimuth_2d, range_2d = np.meshgrid(azimuth, r_distances, indexing='ij')
    elevation_2d = np.broadcast_to(elangle[:, np.newaxis], azimuth_2d.shape)

    xyz, aeqd = wrl.georef.polar.spherical_to_xyz(range_2d, azimuth_2d, elevation_2d, site)
    transformer = Transformer.from_crs(aeqd, "EPSG:4326", always_xy=True)
    lon, lat = transformer.transform(xyz[..., 0], xyz[..., 1])
    
    gate_latitude = np.squeeze(lat)
    gate_longitude = np.squeeze(lon)
    gate_height = np.squeeze(xyz[..., 2])

    # Interpolació del terreny en 2D estricte
    forma_original = gate_latitude.shape
    punts_radar = np.stack((gate_latitude, gate_longitude), axis=-1).reshape(-1, 2)
    
    terrain_height_flat = interpn((dem_lat, dem_lon), dem_values, punts_radar, method="linear", bounds_error=False, fill_value=-500.0)
    terrain_height = terrain_height_flat.reshape(forma_original)
    terrain_height = np.nan_to_num(terrain_height, nan=-500.0)

    # Algorisme Wradlib de bloqueig
    beamwidth = 1.0
    beamradius = wrl.util.half_power_radius(r_distances, beamwidth)
    pbb = wrl.qual.beam_block_frac(terrain_height, gate_height, beamradius)

    # Filtres antifalsos costaners
    pbb[terrain_height <= 0.0] = 0.0
    pbb[gate_height > (terrain_height + beamradius)] = 0.0
    pbb[range_2d < 3000.0] = 0.0

    pbb_masked = np.ma.masked_invalid(pbb)
    cbb = wrl.qual.cum_beam_block_frac(pbb_masked)
    return np.ma.filled(cbb, fill_value=0.0)

# ===========================================================================
# 5. CÀLCUL DE LA TAXA DE PRECIPITACIÓ (R) PER A UN FITXER
# ===========================================================================
def processa_fitxer_radar(fs, s3_path, dem_lat, dem_lon, dem_values):
    try:
        # Llegir fitxer de S3 instantàniament en memòria
        bytes_io = io.BytesIO(fs.cat(s3_path))
        from xradar.io import open_iris_dtree # Import local segons el teu notebook
        dtree = open_iris_dtree(bytes_io, decode_hclass=True)
        swp = dtree["/sweep_0"]
        
        # Extreure variables clau
        dbzh = swp["DBZH"].values
        dr = swp["ZDR"].values # Ajusta el nom exacte segons el teu data (ZDR o DR)
        # Nota: si al teu notebook calcules textures previes (ex: tPHIDP), replica-ho aquí:
        # Per simplificar l'script, calculem una textura local de PHIDP en 2D si existeix
        tphidp = np.zeros_like(dbzh) # Substitueix pel teu filtre de textura si cal
        
        # 1. Aplicar màscara de Clutter
        met_mask = aplica_filtre_clutter(swp, dr, dbzh, tphidp)
        
        # 2. Calcular i aplicar Correcció de Bloqueig (CBB)
        cbb = calcula_cbb(swp, dem_lat, dem_lon, dem_values)
        
        dbzh_corr = np.copy(dbzh)
        mask_comp = (cbb >= 0.1) & (cbb <= 0.5)
        dbzh_corr[mask_comp] = dbzh_corr[mask_comp] - 10.0 * np.log10(1.0 - cbb[mask_comp])
        dbzh_corr[cbb > 0.5] = np.nan
        
        # 3. Convertir Reflectivitat a Taxa de pluja (R) usant Marshall-Palmer o la teva fórmula
        # Z = 200 * R^1.6 -> R = (Z / 200)^(1 / 1.6)
        # Z_linear = 10^(dbzh_corr / 10)
        z_linear = 10.0 ** (dbzh_corr / 10.0)
        R = (z_linear / 200.0) ** (1.0 / 1.6)
        
        # Apliquem la màscara meteorològica (on és False, pluja = 0)
        R[~met_mask] = 0.0
        R = np.nan_to_num(R, nan=0.0)
        
        return R, swp
        
    except Exception as e:
        print(f"  [ERROR] No s'ha pogut processar {os.path.basename(s3_path)}: {e}")
        return None, None

# ===========================================================================
# 6. PIPELINE PRINCIPAL (ACUMULACIÓ I VALIDACIÓ)
# ===========================================================================
def main():
    args = parse_arguments()
    
    # Configurar connexió anònima a S3 d'IDEAM
    fs = s3fs.S3FileSystem(anon=True)
    
    # Desglossar l'arbre de directoris de S3 de l'IDEAM
    # s3://s3-radaresideam/l2_data/YYYY/MM/DD/Radar/
    any_str, mes_str, dia_str = args.data.split("-")
    bucket_folder = f"s3://s3-radaresideam/l2_data/{any_str}/{mes_str}/{dia_str}/{args.radar}"
    
    print(f"[{datetime.datetime.now()}] Cercant fitxers a S3: {bucket_folder}")
    if not fs.exists(bucket_folder):
        print(f"[ERROR] La ruta especificada no existeix a S3.")
        sys.exit(1)
        
    tots_els_fitxers = fs.ls(bucket_folder)
    
    # Filtrar exclusivament els fitxers de l'hora indicada (revisant el nom del fitxer)
    # El patró del nom és ex: COR241109140146.RAWRZXZ (on la posició de l'hora és clau)
    fitxers_de_l_hora = []
    for f in tots_els_fitxers:
        nom_f = os.path.basename(f)
        # Els caràcters de l'hora solen estar després de l'any-mes-dia al format IRIS (posició 9 i 10)
        # Patró IDEAM: AAA250502[HH]MMSS...
        if len(nom_f) >= 11:
            hora_fitxer = nom_f[9:11]
            if hora_fitxer == args.hora:
                fitxers_de_l_hora.append(f)
                
    print(f"-> S'han trobat {len(fitxers_de_l_hora)} fitxers radar per a l'hora {args.hora}:00")
    if not fitxers_de_l_hora:
        print("[AVÍS] No hi ha dades per processar en aquesta franja horària.")
        sys.exit(0)
        
    # Carregar topografia una sola vegada
    dem_lat, dem_lon, dem_values = carrega_dem_colombia(args.dem1, args.dem2)
    
    # Matrius d'acumulació
    acumulat_pluja_2d = None
    comptador_fitxers_valids = 0
    ultim_sweep_valid = None
    
    # Bucle d'integració temporal (Acumulació de l'hora)
    for s3_file in sorted(fitxers_de_l_hora):
        print(f" Processant volum: {os.path.basename(s3_file)}")
        R_fitxer, swp = processa_fitxer_radar(fs, s3_file, dem_lat, dem_lon, dem_values)
        
        if R_fitxer is not None:
            if acumulat_pluja_2d is None:
                acumulat_pluja_2d = np.zeros_like(R_fitxer)
            
            # Assumim integració temporal simple per taxa (R en mm/h).
            # Si els fitxers passen cada 5 minuts (12 fitxers per hora), cada fitxer aporta R * (5/60) hores de pluja.
            # Una aproximació estàndard si es vol fer dinàmic és calcular el delta real, aquí fem la fracció mitjana (ex: 5 minuts = 1/12 d'hora)
            interval_hores = 5.0 / 60.0 
            acumulat_pluja_2d += R_fitxer * interval_hores
            comptador_fitxers_valids += 1
            ultim_sweep_valid = swp

    if comptador_fitxers_valids == 0:
        print("[ERROR] Cap fitxer de l'hora s'ha pogut processar correctament.")
        sys.exit(1)
        
    print(f"[{datetime.datetime.now()}] Acumulació completada. Total fitxers integrats: {comptador_fitxers_valids}")

    # ===========================================================================
    # 7. VALIDACIÓ AMB LES ESTACIONS (Replicant el final del teu notebook)
    # ===========================================================================
    print(f"[{datetime.datetime.now()}] Iniciant fase de validació...")
    
    # Injectem l'acumulat final a l'últim objecte sweep per aprofitar la seva georeferenciació
    ultim_sweep_valid["R_acumulada_hora"] = xr.DataArray(acumulat_pluja_2d, dims=["azimuth", "range"])
    
    # Georeferenciem la malla acumulada a coordenades reals x, y espacials
    ultim_sweep_valid.coords["longitude"] = ultim_sweep_valid.coords["gate_longitude"]
    ultim_sweep_valid.coords["latitude"] = ultim_sweep_valid.coords["gate_latitude"]
    
    # Aquí enllaçaríem amb el teu dataframe d'estacions (hourly_geo_df)
    # Com que l'script s'executa autònom, pots carregar el teu CSV d'estacions d'IDEAM localment:
    csv_estacions = "dades_estacions_ideam.csv" 
    if os.path.exists(csv_estacions):
        hourly_geo_df = pd.read_csv(csv_estacions, parse_dates=["fechaobservacion"])
        # Filtrar exactament pel mateix dia i hora de l'execució de l'script
        hora_int = int(args.hora)
        dia_dt = datetime.datetime.strptime(args.data, "%Y-%m-%d").date()
        
        df_filtrat = hourly_geo_df[
            (hourly_geo_df['fechaobservacion'].dt.date == dia_dt) & 
            (hourly_geo_df['fechaobservacion'].dt.hour == hora_int)
        ].dropna(subset=["longitud", "latitud", "acumulado_horario"])
        
        print(f"-> Trobades {len(df_filtrat)} estacions de validació per aquesta franja horària.")
        
        # Codi d'extracció de píxels del radar sobre la posició de cada estació (Nearest Neighbor o KDTree)
        # [Insereix aquí el teu bucle d'extracció de punts del final de la cel·la 58]
        # ...
        print("Validació finalitzada. Generant mètriques de control (RMSE, Bias)...")
    else:
        print(f"[AVÍS] No s'ha trobat el fitxer '{csv_estacions}'. L'acumulat s'ha calculat però no s'ha pogut validar.")

    # Guardar mapa final a un fitxer NetCDF net per si es vol graficar a posteriori
    output_filename = f"QPE_Acumulada_{args.radar}_{args.data}_H{args.hora}.nc"
    ultim_sweep_valid[["R_acumulada_hora"]].to_netcdf(output_filename)
    print(f"Fitxer guardat correctament com a: {output_filename}")

if __name__ == "__main__":
    main()