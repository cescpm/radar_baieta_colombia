from datetime import datetime, timedelta
from RAW_PVOL import retrieve_PVol_dtree

import cartopy.crs as ccrs
import cartopy.feature as feature
import matplotlib.pyplot as plt
import pandas as pd
from sodapy import Socrata
import pandas as pd
import numpy as np
#----------------------------------------------------------------------------------------

def inspect_IDEAM_stations(csv_filepath):
    """
    Currently relies on a disk-saved csv-file so the information in it may eventually be deprecated
    """
    
    df = pd.read_csv(
        csv_filepath,
        encoding='utf-8',
        on_bad_lines='skip',
        dtype=str,
        keep_default_na=False,
    )

    mask = df['CATEGORIA'].str.strip().str.lower() == 'pluviométrica'
    df_pluv = df[mask].copy()

    return df_pluv

def fence_in_stations(site, maxrange):
    lat,lon = site
    df_pluv = inspect_IDEAM_stations("CNE_IDEAM.csv")

    fence = (((df_pluv["LATITUD"].replace(",",".",regex=True).astype(float) - lat)*111e3)**2 + ((df_pluv["LONGITUD"].replace(",",".",regex=True).astype(float) - lon)*111e3)**2) < maxrange**2

    df_fencd = df_pluv[fence].copy()
    return df_fencd

def main(codis):
    APP_TOKEN = "MFHXNYLts4ZhySVUsR7emeZXO"
    client = Socrata("www.datos.gov.co", APP_TOKEN)

    data_inici = "2025-04-09"
    data_fi = "2025-04-11"


    query = client.get(
        dataset_identifier="s54a-sgyg",
        select="codigoestacion, fechaobservacion, latitud, longitud, valorobservado",
        where=f" fechaobservacion >= '{data_inici}' AND fechaobservacion < '{data_fi}'",
        limit=500000  # s'ha d'ajustar segons el què es demani
    )

    df = pd.DataFrame.from_records(query).sort_values(by=["codigoestacion","fechaobservacion"])
    print(df)

    mask = df["codigoestacion"].isin(codis)

    df_maskd = df[mask].copy()
    print(np.unique_values(df_maskd["valorobservado"]))
    print(np.unique_counts(df_maskd["valorobservado"]))

    

if __name__ == "__main__":

    rad_site = (retrieve_PVol_dtree().data_vars["latitude"].values.item(),retrieve_PVol_dtree().data_vars["longitude"].values.item())
    codis = fence_in_stations(rad_site, 240e3)["CODIGO"].to_numpy()
    codis = [codi.zfill(10) for codi in codis]
    main(codis)