import os
import argparse
import json
import pickle
import tempfile
from hashlib import sha1
from collections import OrderedDict
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import wradlib as wrl

# ----------------------------------------------------------------------------------------

def extract_metadata_from_s3_object(bucket_name, s3_key):
    """
    Descarga temporalmente un archivo RAW desde S3 y extrae estrictamente la metadata.
    Cada proceso inicializa su propio cliente S3 de forma segura.
    """
    # Inicialización del cliente S3 dentro del proceso para evitar problemas de pickling
    s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))

    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        try:
            s3_client.download_file(bucket_name, s3_key, temp_file.name)
            
            if os.path.getsize(temp_file.name) == 0:
                return None

            # Extraer metadata (CPU-bound: se ejecuta en paralelo real gracias a multiprocessing)
            meta_odict = wrl.io.iris.read_iris(
                filename=temp_file.name,
                load_data=False,
                rawdata=False,
                debug=False,
            )

            prod_cfg = meta_odict['product_hdr']['product_configuration']
            filename = os.path.basename(s3_key)
            file_meta_odict = OrderedDict()
            hash1 = sha1(pickle.dumps(meta_odict)).hexdigest()

            file_meta_odict[filename] = {
                'filepath':     f"s3://{bucket_name}/{s3_key}", 
                'hash':         hash1,
                'sweeps':       OrderedDict(),
            }

            for sweep_num, sweep_data in meta_odict['data'].items():
                elev  = sweep_data['sweep_data']['elevation'][0]
                nrays = sweep_data['sweep_data']['DB_DBZ'].shape[0]
                nbins = sweep_data['sweep_data']['DB_DBZ'].shape[1]
                hdrs  = sweep_data['ingest_data_hdrs']

                available_fields = list(hdrs.keys())
                
                file_meta_odict[filename]["sweeps"][sweep_num] = OrderedDict({  
                    'elevation_angle':    round(float(elev), 4),
                    'nrays':              int(nrays),
                    'nbins':              int(nbins),   
                    'timestamp':          prod_cfg['sweep_ingest_time'].isoformat(),
                    'fields':             available_fields,
                })
            
            return filename, file_meta_odict

        except Exception as e:
            print(f"ERROR procesando {s3_key}: {e}")
            return None


def main():
    parser = argparse.ArgumentParser(description="Extrae metadata de radares (archivos RAW) desde AWS S3.")
    parser.add_argument("-d", "--date", required=True, help="Fecha a procesar en formato YYYY/MM/DD (ej. 2025/05/01)")
    parser.add_argument("-o", "--output", required=True, help="Ruta y nombre del archivo JSON de salida (sin extensión)")
    parser.add_argument("-b", "--bucket", default="s3-radaresideam", help="Nombre del bucket de S3")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Número de procesos en paralelo (se recomiendan 4 para tus 4 núcleos)")
    args = parser.parse_args()
    
    date_path = args.date
    output_file = args.output
    bucket_name = args.bucket
    max_workers = args.workers
    
    target_radar = "Barrancabermeja"
    prefix_base = f"l2_data/{date_path}/{target_radar}/"

    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    print(f"Buscando archivos exclusivamente para [{target_radar}] en: s3://{bucket_name}/{prefix_base}")
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix_base)
    
    # Descubrimiento rápido de archivos (I/O ligero)
    s3_keys_to_process = []
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            s3_key = obj['Key']
            filename = os.path.basename(s3_key)
            
            if '.RAW' not in filename.upper() or obj['Size'] == 0:
                continue
                
            s3_keys_to_process.append(s3_key)

    print(f"Se encontraron {len(s3_keys_to_process)} archivos válidos. Procesando en paralelo con {max_workers} procesos...")

    radar_files_data = OrderedDict()

    # Procesamiento con Procesos en lugar de Hilos
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(extract_metadata_from_s3_object, bucket_name, key): key 
            for key in s3_keys_to_process
        }
        
        for future in as_completed(futures):
            result = future.result()
            if result:
                filename, file_meta_odict = result
                radar_files_data.update(file_meta_odict)
                print(f"Procesado: {filename}")

    # Organizar y guardar resultados
    if radar_files_data:
        sorted_files = OrderedDict(
            sorted(radar_files_data.items(),
                   key=lambda x: next(iter(x[1]['sweeps'].values()))['timestamp'])
        )
        radar_records = {target_radar: sorted_files}
    else:
        radar_records = {target_radar: OrderedDict()}

    output_json_path = f"{output_file}.json"
    
    output_dir = os.path.dirname(output_json_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_json_path, 'w') as file:
        json.dump(radar_records, file, indent=2)

    print(f"\n¡Proceso finalizado!")
    print(f"Archivo guardado: {output_json_path} ({len(radar_records[target_radar])} archivos procesados)")


if __name__ == '__main__':
    main()