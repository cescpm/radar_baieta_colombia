import wradlib as wrl
import os
import sys

# Get directory from command line argument
# Usage: python process_radar.py /path/to/files
data_dir = sys.argv[1] if len(sys.argv) > 1 else '.'

# Get all files matching pattern
files = sorted([
    f for f in os.listdir(data_dir)
    if 'RAW' in f.upper()
])

print(f"Found {len(files)} files in {data_dir}")

for filename in files:
    filepath = os.path.join(data_dir, filename)
    try:
        data = wrl.io.read_iris(filepath)
        timestamp = data['product_hdr']['product_end']['ingest_time']
        
        print(f"\n{'='*50}")
        print(f"File: {filename}  |  Time: {timestamp}")
        
        for sweep_num, sweep_data in data['data'].items():
            elev = sweep_data['sweep_data']['elevation'][0]
            print(f"  Sweep {sweep_num}: {elev:.2f}°")
            
    except Exception as e:
        print(f"  ERROR {filename}: {e}")
