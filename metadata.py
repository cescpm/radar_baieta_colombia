import os
import argparse
import json
import pickle
import tempfile
from hashlib import sha1
from collections import OrderedDict
from datetime import datetime
import boto3
from botocore import UNSIGNED
from botocore.config import Config
import wradlib as wrl

# ----------------------------------------------------------------------------------------

def extract_metadata_from_s3_object(s3_client, bucket_name, s3_key):
    """
    Descarga temporalmente un archivo RAW desde S3 y extrae estrictamente la metadata
    necesaria para la futura creación de Pseudo-PVOLs.
    """
    with tempfile.NamedTemporaryFile(delete=True) as temp_file:
        try:
            s3_client.download_file(bucket_name, s3_key, temp_file.name)
            
            if os.path.getsize(temp_file.name) == 0:
                return None

            # Extraer metadata (solo funciona con formato IRIS RAW)
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

            # Mantenemos las claves exactas de tu script original
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
            
            return file_meta_odict

        except Exception as e:
            print(f"ERROR procesando {s3_key}: {e}")
            return None


def main():
    # Uso de argparse para facilitar la iteración de múltiples días desde bash
    parser = argparse.ArgumentParser(description="Extrae metadata de radares (archivos RAW) desde AWS S3.")
    parser.add_argument("-d", "--date", required=True, help="Fecha a procesar en formato YYYY/MM/DD (ej. 2025/05/01)")
    parser.add_argument("-o", "--output", required=True, help="Ruta y nombre del archivo JSON de salida (sin extensión)")
    parser.add_argument("-b", "--bucket", default="s3-radaresideam", help="Nombre del bucket de S3")
    args = parser.parse_args()
    
    date_path = args.date
    output_file = args.output
    bucket_name = args.bucket
    
    prefix_base = f"l2_data/{date_path}/"

    # Conexión a S3 (ajustar signature_version si el bucket es privado)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    print(f"Buscando archivos en: s3://{bucket_name}/{prefix_base}")
    
    paginator = s3.get_paginator('list_objects_v2')
    radar_records = OrderedDict()

    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix_base)
    
    for page in pages:
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            s3_key = obj['Key']
            filename = os.path.basename(s3_key)
            
            # FILTRO ESTRICTO: Solo archivos .RAW (Evitamos .nc y .nc.gz explícitamente)
            if '.RAW' not in filename.upper() or obj['Size'] == 0:
                continue
                
            relative_path = s3_key.replace(prefix_base, "")
            path_parts = relative_path.split('/')
            
            # Aseguramos que está dentro de una subcarpeta de radar
            if len(path_parts) < 2:
                continue 
                
            radar_name = path_parts[0]
            
            print(f"Procesando: Radar [{radar_name}] -> {filename}")
            
            meta_result = extract_metadata_from_s3_object(s3, bucket_name, s3_key)
            
            if meta_result:
                if radar_name not in radar_records:
                    radar_records[radar_name] = OrderedDict()
                radar_records[radar_name].update(meta_result)

    # Ordenar los archivos de cada radar por su timestamp inicial (ideal para formar PVOLs)
    for radar, files in radar_records.items():
        radar_records[radar] = OrderedDict(
            sorted(files.items(),
                   key=lambda x: next(iter(x[1]['sweeps'].values()))['timestamp'])
        )

    output_json_path = f"{output_file}.json"
    
    # Crear directorio de salida si no existe (basado en tu script original)
    output_dir = os.path.dirname(output_json_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)

    with open(output_json_path, 'w') as file:
        json.dump(radar_records, file, indent=2)

    print(f"\n¡Proceso finalizado! Metadata estructurada para creación de PVOLs.")
    print(f"Archivo guardado: {output_json_path}  ({sum(len(v) for v in radar_records.values())} archivos totales procesados)")

if __name__ == '__main__':
    main()