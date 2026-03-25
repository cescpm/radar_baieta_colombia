import os
import sys
import csv
import json
import xradar as xd
import wradlib as wrl
from collections import OrderedDict
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
#----------------------------------------------------------------------------------------

if __name__ == '__main__':
    filepath = "data/raw/Tablazo/2026/02/01/TAB260201234240.RAWVVEW"
    data_odict = wrl.io.iris.read_iris(
        filename=filepath,
        load_data=True,
        rawdata=False,
        debug=False,
    )
    print(data_odict['product_hdr'])

    dt = xd.io.open_iris_datatree(filepath)
    print(dt)