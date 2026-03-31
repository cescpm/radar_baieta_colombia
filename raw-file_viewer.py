import os
import sys
import csv
import json
import xradar as xd
import wradlib as wrl
import matplotlib.pyplot as plt
from collections import OrderedDict
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
#----------------------------------------------------------------------------------------

if __name__ == '__main__':
    filepath = "data/raw/Tablazo/2026/02/01/TAB260201235502.RAWVVJX"
    data_odict = wrl.io.iris.read_iris(
        filename=filepath,
        load_data=True,
        rawdata=False,
        debug=False,
    )
    print(data_odict['product_hdr'])

    dt = xd.io.open_iris_datatree(filepath)
    print(dt["/sweep_0"]["sweep_mode"])
    print(dt["/sweep_0"].attrs)
    da = dt["/sweep_0"]["DBZH"]
    da.attrs["sweep_mode"] = "azimuth_surveillance"
    print(da)
    wrl.vis.plot(da)
    