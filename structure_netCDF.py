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
    filepath = "data/raw/Bogota/2026/01/01/1399BOG-20260101-001200-PPIVol-aeec.nc"
    data = wrl.io.netcdf.read_edge_netcdf(filepath)
    print(data)

    dt = xd.io.open_cfradial1_datatree(filepath)
    print(dt)
