import os
import sys
import csv
import json
import xradar as xd
import wradlib as wrl
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
from collections import OrderedDict
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
#----------------------------------------------------------------------------------------

if __name__ == '__main__':
    filepath = "data/raw/Tablazo/2026/02/01/TAB260201235739.RAWVVKM"
    data_odict = wrl.io.iris.read_iris(
        filename=filepath,
        load_data=True,
        rawdata=False,
        debug=False,
    )
    print(data_odict['product_hdr'])

    dt = xd.io.open_iris_datatree(filepath)
    print(dt)

    df = pl.from_pandas(dt["/sweep_0"]["DBZH"].to_dataframe().reset_index())

    # 3. Clean up NaNs (Radar 'no-echo' areas)
    df = df.drop_nulls(subset=["DBZH"])
    
    # 4. Manual Curvilinear Projection (Polar to Cartesian)
    # x = r * sin(theta), y = r * cos(theta)
    df = df.with_columns([
        (pl.col("range") * (pl.col("azimuth") * np.pi / 180).sin()).alias("x"),
        (pl.col("range") * (pl.col("azimuth") * np.pi / 180).cos()).alias("y")
    ])

    # 5. Plotting
    plt.figure(figsize=(10, 8))
    plt.scatter(
        df["x"], 
        df["y"], 
        c=df["DBZH"], 
        cmap="viridis", 
        s=1,           # Adjust size based on density
        edgecolors="none"
    )

    plt.colorbar(label="Reflectivity (DBZH)")
    plt.axis("equal")
    plt.xlabel("Distance (m)")
    plt.ylabel("Distance (m)")
    plt.title("Radar Sweep - Polars/Matplotlib")
    plt.show()
