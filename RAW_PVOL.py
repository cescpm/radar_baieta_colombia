import json
import xradar as xd
#----------------------------------------------------------------------------------------
with open('metaTablazo.json', 'r') as f:
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

def create_PVOL_from_PPIs(sweeps_list):
    dtree = xd.io.open_iris_datatree(sweeps_list[0])
    for iter, sweep_filepath in enumerate(sweeps_list[1:]):
        sweep_group_name = f"/sweep_{iter}"
        dtree[sweep_group_name] = xd.io.open_iris_datatree(sweep_filepath)["/sweep_0"]
    
    return dtree

def PVOL_to_netCDF4(dtree):
    variables = dtree.data_vars
    dtree.to_netcdf(
        f"pvol_{dtree["time_coverage_start"]}",
        engine="netcdf4",
    )

def main():
    stacks = get_stacks(data)
    #for i, stack in enumerate(stacks, 1):
    #    print(f"Stack {i} ({len(stack)} sweeps):")
    #    for filename in stack:
    #        print(f"  - {filename}")

    pvol_dtree = create_PVOL_from_PPIs(stacks[15])
    return pvol_dtree

if __name__ == "__main__":
    print(main())


       
