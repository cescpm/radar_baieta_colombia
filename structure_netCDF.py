import os
import sys
import csv
import json
import pickle
import wradlib as wrl
from hashlib import sha1
from collections import OrderedDict
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
#----------------------------------------------------------------------------------------
def extract_metadata(filepath: str) -> OrderedDict|None:
    """
    Extract only metadata from NetCDF files in CF/Radial format
    
    Parameters
    ----------
    filepath : str
        Path to the .nc or .nc.bz2 file
    
    Returns
    -------
    file_meta_odict : OrderedDict
        Stores the information needed to create a Pseudo-PVOL
    None
        If file is empty or cannot be read
    """
    if os.path.getsize(filepath) == 0:
        return None
    
    try:
        # Read metadata from NetCDF file
        meta_odict = wrl.io.netcdf.read_generic_netcdf(filepath)
        
        filename = os.path.basename(filepath)
        files_meta_odict = OrderedDict()
        hash1 = sha1(pickle.dumps(meta_odict)).hexdigest()
        key = f"{filename}"
        
        files_meta_odict[key] = {
            # File info
            'filename': filename,
            'hash': hash1,
            # Sweep info
            'sweeps': OrderedDict(),
        }
        
        # Get dimensions and variables
        dimensions = meta_odict.get('dimensions', {})
        variables = meta_odict.get('variables', {})
        # Get number of sweeps
        n_sweeps = dimensions.get('sweep', {}).get('size', 0)
        # Get sweep angles
        sweep_fixed_angles = variables.get('fixed_angle', {}).get('data', [])
        
        # Get timestamp
        timestamp = meta_odict.get('time_coverage_start', 'unknown')
        
        # Get available fields (exclude coordinate variables)
        available_fields = [
            var for var in variables.keys()
            if var not in ['volume_number', 'latitude', 'longitude',
                           'altitude', 'time_coverage_start', 'time_coverage_end',
                           'time', 'azimuth', 'elevation',
                           'radar_antenna_gain_h', 'radar_antenna_gain_v',
                           'radar_beam_width_h', 'radar_beam_width_v',
                           'radar_receiver_bandwidth', 'frequency',
                           'radar_measured_transmit_power_h',
                           'radar_measured_transmit_power_v',
                           'pulse_width', 'prt', 'prt_ratio', 
                           'nyquist_velocity', 'n_samples',
                           'prt_mode', 'polarization_mode',
                           'range', 'sweep_number', 'sweep_mode',
                           'fixed_angle', 'sweep_start_ray_index',
                           'sweep_end_ray_index']
        ]
        
        # Extract sweep information
        if n_sweeps > 0:
            # Multi-sweep file
            for sweep_num in range(n_sweeps):
                # Get elevation angle for this sweep
                if hasattr(sweep_fixed_angles, '__iter__') and len(sweep_fixed_angles) > sweep_num:
                    elev = float(sweep_fixed_angles[sweep_num])
                else:
                    elev = float(sweep_fixed_angles) if sweep_fixed_angles is not None else 0.0
                
                # Get dimensions
                nrays = dimensions.get('time', {}).get('size', 0)
                nbins = dimensions.get('range', {}).get('size', 0)
                
                files_meta_odict[key]["sweeps"][sweep_num] = OrderedDict({
                    # Sweep info
                    'elevation_angle': round(float(elev), 4),
                    'nrays': int(nrays),
                    'nbins': int(nbins),
                    # Timestamp
                    'timestamp': timestamp,
                    # Products
                    'fields': available_fields,
                })
        else:
            # Single sweep file (IDEAM case)
            sweep_num = 0
            
            # Get elevation angle
            if sweep_fixed_angles is not None:
                if hasattr(sweep_fixed_angles, '__iter__'):
                    elev = float(sweep_fixed_angles[0])
                else:
                    elev = float(sweep_fixed_angles)
            else:
                elev = 0.0
            
            # Get dimensions
            nrays = dimensions.get('time', {}).get('size', 0)
            nbins = dimensions.get('range', {}).get('size', 0)
            
            files_meta_odict[key]["sweeps"][sweep_num] = OrderedDict({
                # Sweep info
                'elevation_angle': round(float(elev), 4),
                'nrays': int(nrays),
                'nbins': int(nbins),
                # Timestamp
                'timestamp': timestamp,
                # Products
                'fields': available_fields,
            })
        
        return files_meta_odict
        
    except Exception as e:
        print(f"ERROR {filepath}: {e}")
        return None
def sweeps_to_PVOL(meta):
    """
    Creates a PVOL product from single PPI scans.
    """
    with open(meta, 'r') as file:
        meta = json.load(
            file,
            object_hook=OrderedDict,
            object_pairs_hook=dict,
        )
    return meta
def main():
    dirpath = sys.argv[1]
    output_file = sys.argv[2]

    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    all_records = OrderedDict()
    
    for f in sorted(os.listdir(dirpath)):
        # Check for NetCDF files (both .nc and .nc.bz2)
        if not (f.endswith('.nc') or f.endswith('.nc.bz2')):
            continue
        
        filepath = os.path.join(dirpath, f)

        if os.path.getsize(filepath) == 0:
            continue
        
        result = extract_metadata(filepath)
        if result:
            all_records.update(result)
    
    # Sort by timestamp of first sweep
    all_records = OrderedDict(
        sorted(all_records.items(),
            key=lambda x: next(iter(x[1]['sweeps'].values()))['timestamp'])
    )
    
    with open(output_file + '.json', 'w') as file:
        json.dump(
            all_records,
            file,
            indent=2,
        )
    
    print(f"Written: {output_file}.json  ({len(all_records)} files)")


if __name__ == '__main__':
    main()