import os
import sys
import csv
import json
import wradlib as wrl
from collections import OrderedDict
from datetime import datetime, timezone
from concurrent.futures import ProcessPoolExecutor, as_completed
#----------------------------------------------------------------------------------------

def extract_metadata(filepath : str) -> OrderedDict|None:
    """
    Extract only metadata—no data arrays loaded—from .RAWXXXX files
    built in IRIS format

    -----------
    Parameters:

    filepath : str
        Path to the .RAWXXXX file

    --------
    Returns:

    file_meta_odict : OrderedDict
        Stores the information needed to create a Pseudo-PVOL
    
     : None
    """
    
    if os.path.getsize(filepath) == 0:
        return None
    
    try:
        meta_odict = wrl.io.iris.read_iris(  #only reads the metadata and returns it as an OrderedDictionary
            filename=filepath,
            load_data=False,
            rawdata=False,
            debug=False,
        )

        prod_cfg = meta_odict['product_hdr']['product_configuration']
        filename = os.path.basename(filepath)
        files_meta_odict = OrderedDict()

        key = f"{filename}"

        files_meta_odict[key] = {
            # File info
            'filename':     filename,
                
            # Sweep info
            'sweeps':       OrderedDict(),
        }

        for sweep_num, sweep_data in meta_odict['data'].items():
            elev  = sweep_data['sweep_data']['elevation'][0]
            nrays = sweep_data['sweep_data']['DB_DBZ'].shape[0]
            nbins = sweep_data['sweep_data']['DB_DBZ'].shape[1]
            
            files_meta_odict[key]["sweeps"][sweep_num] = OrderedDict({  
                # Sweep info
                'elevation_angle':    round(float(elev), 4),
                'nrays':              int(nrays),
                'nbins':              int(nbins),
                
                # Timestamp
                'timestamp':          prod_cfg['sweep_ingest_time'].isoformat(),
            })
        
        return files_meta_odict
    
    except Exception as e:
        print(f"ERROR {filepath}: {e}")
        return None
    
if __name__ == '__main__':
    dirpath     = sys.argv[1]
    output_file = sys.argv[2]

    all_records = OrderedDict()

    for f in sorted(os.listdir(dirpath)):
        if 'RAW' not in f.upper():
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

    with open(output_file + '.json', 'w') as f:
        json.dump(all_records, f, indent=2)

    print(f"Written: {output_file}.json  ({len(all_records)} files)")