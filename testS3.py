import boto3
import pandas as pd
from credential_S3 import key_ACCESS, key_SECRET

#Declarando fuciones para carga y descarga

def descargar_archivo(s3, bucket_name, file_name, local_file_name):
    """
    Descarga un archivo desde un bucket de Amazon S3.

    Args:
    - s3: Cliente de S3.
    - bucket_name: Nombre del bucket en S3.
    - file_name: Nombre del archivo en el bucket.
    - local_file_name: Nombre del archivo local donde se guardará el archivo descargado.
    """
    try:
        # Descargar el archivo desde S3
        s3.download_file(bucket_name, file_name, local_file_name)
        print(f"Archivo '{file_name}' descargado como '{local_file_name}' exitosamente.")
    except Exception as e:
        print(f"Error al descargar el archivo: {str(e)}")


def subir_archivo(s3, bucket_name, file_name, local_file_path):
    """
    Sube un archivo al bucket de Amazon S3.

    Args:
    - s3: Cliente de S3.
    - bucket_name: Nombre del bucket S3.
    - file_name: Nombre del archivo en el bucket.
    - local_file_path: Ruta local del archivo que se va a subir.
    """
    try:
        # Subir el archivo a S3
        s3.upload_file(local_file_path, bucket_name, file_name)
        print(f"Archivo '{local_file_path}' subido como '{file_name}' exitosamente.")
    except Exception as e:
        print(f"Error al subir el archivo: {str(e)}")


#Ejecucion
#Instancia s3
s3 = boto3.client(
    's3',
    region_name='us-east-2',
    aws_access_key_id=key_ACCESS,
    aws_secret_access_key=key_SECRET
)

# Descargar un archivo
#descargar_archivo(s3, "udesa-s3", "advertiser_ids.csv", "advertiser_ids_local.csv")

# Subir un archivo
subir_archivo(s3, "udesa-s3", "advertiserBUCKET_ids_local.csv", "advertiser_ids_local.csv")







