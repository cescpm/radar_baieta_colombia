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
    filepath = "data/raw/Tablazo/2026/02/01/TAB260201235554.RAWVVK4"
    data_odict = wrl.io.iris.read_iris(
        filename=filepath,
        load_data=True,
        rawdata=False,
        debug=False,
    )
    print(data_odict.keys())

    dt = xd.io.open_iris_datatree(filepath)
    print(dt)
    print(dt["/sweep_0"].data_vars)

    variable = str(sys.argv[1])
    print(np.unique_values(dt["/sweep_0"][variable].values))

    df = pl.from_pandas(dt["/sweep_0"][variable].to_dataframe().reset_index())
    print(df)
    # 3. Clean up NaNs (Radar 'no-echo' areas)
    #df = df.drop_nulls(subset=[variable])

    if variable == "DB_HCLASS":
        df = df.with_columns(
            (pl.col(variable).cast(pl.Int16) & 0xFF).alias(variable)
        )

        print(df)
        print(np.unique_values(df[variable]))
    
    # 4. Manual Curvilinear Projection (Polar to Cartesian)
    # x = r * sin(theta), y = r * cos(theta)
    df = df.with_columns([
        (pl.col("range") * (pl.col("azimuth") * np.pi / 180).sin()).alias("x"),
        (pl.col("range") * (pl.col("azimuth") * np.pi / 180).cos()).alias("y"),
    ])

    # 5. Plotting
    plt.figure(figsize=(10, 8))
    plt.scatter(
        df["x"], 
        df["y"], 
        c=df[variable], 
        cmap="tab10", 
        s=1,           # Adjust size based on density
        edgecolors="none"
    )

    plt.colorbar(label=variable)
    plt.axis("equal")
    plt.xlabel("Distance (m)")
    plt.ylabel("Distance (m)")
    plt.title("Radar Sweep - Polars/Matplotlib")
    plt.show()
