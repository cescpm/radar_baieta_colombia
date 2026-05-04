import json
import xradar as xd
import numpy as np
from datetime import datetime
from collections import defaultdict
#----------------------------------------------------------------------------------------
with open('metadata/raw/Corozal/2024/10/21/meta.json', 'r') as f:
    data = json.load(f)

def get_stacks(json_data):
    all_stacks = []
    current_stack = []
    last_sweep_num = float('inf')

    # Iterate through the dictionary items
    for key, value in json_data.items():
        # Get the sweep number (it's the first key inside the "sweeps" object)
        # We convert to int for comparison
        sweep_id = list(value['sweeps'].keys())[0]
        current_sweep_num = int(sweep_id)

        # Logic: If current sweep is not higher than the last, the previous volume is finished
        if current_sweep_num <= last_sweep_num:
            if current_stack:
                all_stacks.append(current_stack)
            current_stack = []

        # Add the filename to the current volume stack
        current_stack.append(value['filepath'])
        last_sweep_num = current_sweep_num

    # Add the final stack if it's not empty
    if current_stack:
        all_stacks.append(current_stack)
        
    return all_stacks

def get_lower_scans_per_hour(json_data,variable):
    """
    És poc robust si tinc dos scans consecutius 0.0 i 0.5 en dos arxius diferents amb interval
    de temps de l'ordre de segons, ho agafa. He posat 0.0 pel cas de Tablazo perquè em fa més fàcil treballar-ho
    Però s'ha de millorar la robustesa 
    """
    hourly_data = defaultdict(list)   # hour -> list of (angle, filepath)

    for key, value in json_data.items():
        filepath = value['filepath']
        for sweep_id, sweep_data in value['sweeps'].items():
            if variable in sweep_data['fields']:
                timestamp_str = sweep_data['timestamp']
                hour = datetime.fromisoformat(timestamp_str).hour
                angle = sweep_data['elevation_angle']
                hourly_data[hour].append((angle, filepath))

    result = []
    for hour in range(24):
        items = hourly_data.get(hour, [])
        if not items:
            result.append([])
            continue
        min_angle = min(angle for angle, _ in items)
        unique_paths = {fp for angle, fp in items if angle == min_angle}
        result.append(list(unique_paths))

    return result


def create_ScanVol_from_PPIs(sweeps_list):
    dtree = xd.io.open_iris_datatree(sweeps_list[0])
    for iter, sweep_filepath in enumerate(sweeps_list[1:]):
        sweep_group_name = f"/sweep_{iter}"
        dtree[sweep_group_name] = xd.io.open_iris_datatree(sweep_filepath)["/sweep_0"]
    
    return dtree

def ScanVol_to_netCDF4(dtree):
    variables = dtree.data_vars
    dtree.to_netcdf(
        f"pvol_{dtree["time_coverage_start"]}",
        engine="netcdf4",
    )

def retrieve_ScanVol_dtree():
    stacks = get_stacks(data)
    #for i, stack in enumerate(stacks, 1):
    #    print(f"Stack {i} ({len(stack)} sweeps):")
    #    for filename in stack:
    #        print(f"  - {filename}")

    pvol_dtree = create_ScanVol_from_PPIs(stacks[-1])
    return pvol_dtree

def retrieve_lower_scans():
    stacks = get_lower_scans_per_hour(data,"DB_HCLASS")
    #for i, stack in enumerate(stacks, 1):
    #    print(f"Stack {i} ({len(stack)} sweeps):")
    #    for filename in stack:
    #        print(f"  - {filename}")
            
    return stacks

if __name__ == "__main__":
    retrieve_lower_scans()
