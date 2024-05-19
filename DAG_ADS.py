import pandas as pd
import random
import string
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
import boto3
from io import StringIO
import os
import psycopg2

def filtrar_product_views(**context):
    # Conecto con el cliente de S3 (que a su vez tiene permisos de acceso vía IAM)
    s3 = boto3.client('s3')
    bucket = 'udesa-s3' 
    product_views_key = 'data/product_views.csv' 
    advertiser_ids_key = 'data/advertiser_ids.csv'  
    
    obj_1 = s3.get_object(Bucket=bucket, Key=product_views_key)
    product_views = pd.read_csv(obj_1['Body'])

    # Genero una fecha de ejecución vinculada al "contexto" de Airflow. Esto me va a servir para que luego en el catch-up se creen las fechas bien
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    # Filtro para quedarme solo con los datos que se corresponden al día anterior a la fecha del "contexto" del pipeline.
    # Antes tengo que homogeneizar la forma en la que está escrita la fecha en el archivo original del S3 para que sea compatible con el de datetime de la variable "yesterday"
    product_views['date'] = pd.to_datetime(product_views['date']) 
    product_views['date'] = product_views['date'].dt.strftime('%Y-%m-%d')
    product_views_yesterday = product_views[product_views['date'] == yesterday]
    
    obj_2 = s3.get_object(Bucket=bucket, Key=advertiser_ids_key)
    advertiser_ids = pd.read_csv(obj_2['Body'])
    active_advertiser_ids = advertiser_ids['advertiser_id']
    
    #Me quedo solo con los datos asociados a advertisers que estén activos
    product_views_filtered = product_views_yesterday[product_views_yesterday['advertiser_id'].isin(active_advertiser_ids)]
    
    # Guardo los datos filtrados en un csv en el bucket
    csv_buffer = StringIO()
    product_views_filtered.to_csv(csv_buffer, index=False)
    s3_key = f'data/product_views_filtered_{yesterday}.csv' # genero un nombre del archivo que guarde en el S3 un archivo con los datos asociado a la fecha de ejecución 
    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())


def filtrar_ads_views(**context):
    #La lógica del código en esta función es análoga a la de la anterior, pero con la base de ads
    s3 = boto3.client('s3')
    bucket = 'udesa-s3' 
    ads_views_key = 'data/ads_views.csv' 
    advertiser_ids_key = 'data/advertiser_ids.csv'  
    
    obj_1 = s3.get_object(Bucket=bucket, Key=ads_views_key)
    ads_views = pd.read_csv(obj_1['Body'])
    
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    ads_views['date'] = pd.to_datetime(ads_views['date']) 
    ads_views['date'] = ads_views['date'].dt.strftime('%Y-%m-%d')
    ads_views_yesterday = ads_views[ads_views['date'] == yesterday]
    
    obj_2 = s3.get_object(Bucket=bucket, Key=advertiser_ids_key)
    advertiser_ids = pd.read_csv(obj_2['Body'])
    active_advertiser_ids = advertiser_ids['advertiser_id']
    
    ads_views_filtered = ads_views_yesterday[ads_views_yesterday['advertiser_id'].isin(active_advertiser_ids)]

    csv_buffer = StringIO()
    ads_views_filtered.to_csv(csv_buffer, index=False)
    s3_key = f'data/ads_views_filtered_{yesterday}.csv'
    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())

def top_product(**context):
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    # Cargo el archivo filtered_product_views desde el S3
    s3 = boto3.client('s3')
    bucket = 'udesa-s3'
    filtered_product_views_key = f'data/product_views_filtered_{yesterday}.csv'  # Ruta del archivo filtrado en S3
    
    obj_1 = s3.get_object(Bucket=bucket, Key=filtered_product_views_key)
    product_views = pd.read_csv(obj_1['Body'])

    # Calculo los 20 product_id más frecuentes por advertiser_id
    top_product_ids = product_views.groupby(['advertiser_id', 'product_id']).size() \
                                   .groupby(level=0, group_keys=False) \
                                   .nlargest(20) \
                                   .reset_index(name='count')
    
    # Agrego la fecha del día anterior como columna
    top_product_ids['date'] = yesterday
    
    # Guardo el archivo top product en el S3
    csv_buffer = StringIO()
    top_product_ids.to_csv(csv_buffer, index=False)
    s3_key = f'data/top_product_ids_{yesterday}.csv'
    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())

def top_CTR(**context):
    #Mucho de lo que hago en esta función es análogo a lo que hago en la anterior
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    s3 = boto3.client('s3')
    bucket = 'udesa-s3'
    filtered_ads_views_key = f'data/ads_views_filtered_{yesterday}.csv'
    
    obj_1 = s3.get_object(Bucket=bucket, Key=filtered_ads_views_key)
    ads_views = pd.read_csv(obj_1['Body'])

    #Creo 2 variables para contar la cantidad de impresiones y de clicks
    ads_views['cant_impression'] = ads_views.groupby(['advertiser_id', 'product_id'])['type'].transform(lambda x: (x == 'impression').sum())
    ads_views['cant_click'] = ads_views.groupby(['advertiser_id', 'product_id'])['type'].transform(lambda x: (x == 'click').sum())
    
    #Calculo el ratio click-impression. Agrego una condición de que si impression es 0, reemplace el denominador por 1 (para que no explote el cociente).
    ads_views['ctr'] = ads_views['cant_click'] / ads_views['cant_impression'].where(ads_views['cant_impression'] != 0, other=1)

    # Elimino los duplicados en las combinaciones de advertiser_id y product_id
    unique_ads_views = ads_views.drop_duplicates(subset=['advertiser_id', 'product_id'])

    # Selecciono los 20 productos con el CTR más alto para cada advertiser_id
    top_ctr_ids = unique_ads_views.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'ctr')).reset_index(drop=True)

    top_ctr_ids['date'] = yesterday

    csv_buffer = StringIO()
    top_ctr_ids.to_csv(csv_buffer, index=False)
    s3_key = f'data/top_ctr_ids_{yesterday}.csv'
    s3.put_object(Bucket=bucket, Key=s3_key, Body=csv_buffer.getvalue())

def DBWriting(**context):
    execution_date = context['execution_date']
    yesterday = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')
    s3 = boto3.client('s3')
    bucket = 'udesa-s3'
    top_product_ids_key = f'data/top_product_ids_{yesterday}.csv'
    top_ctr_ids_key = f'data/top_ctr_ids_{yesterday}.csv'
    
    obj_1 = s3.get_object(Bucket=bucket, Key=top_product_ids_key)
    obj_2 = s3.get_object(Bucket=bucket, Key=top_ctr_ids_key)
    
    top_product_ids = pd.read_csv(obj_1['Body'])
    top_ctr_ids = pd.read_csv(obj_2['Body'])

    top_product_df = top_product_ids.reset_index()#.rename(columns={'advertiser_id': 'advertiser_id','product_id':'product_id', 0: 'count', 'date':'date'})
    top_ctr_df = top_ctr_ids.reset_index()#.rename(columns={'advertiser_id': 'advertiser_id','product_id':'product_id', 0: 'CTR', 'date':'date'})
    
    # Establezco la conexión con la base de datos RDS a través de variables de entorno ocultas que creamos en el EC2.
    conn = psycopg2.connect(
        dbname=os.getenv('RDS_DB_NAME'),
        user=os.getenv('RDS_USERNAME'),
        password=os.getenv('RDS_PASSWORD'),
        host=os.getenv('RDS_HOST'),
        port=os.getenv('RDS_PORT')
    )
    cur = conn.cursor()

    # Inserto los datos de top_product_df en la tabla top_product_df que creamos en el RDS
    for index, row in top_product_df.iterrows():
        cur.execute("INSERT INTO top_product_df (advertiser_id, product_id, date, count) VALUES (%s, %s, %s, %s)",
                    (row['advertiser_id'], row['product_id'], row['date'], row['count']))

    # Inserto los datos de top_ctr_df en la tabla top_ctr_df que creamos en el RDS
    for index, row in top_ctr_df.iterrows():
        cur.execute("INSERT INTO top_ctr_df (advertiser_id, product_id, date, ctr) VALUES (%s, %s, %s, %s)",
                    (row['advertiser_id'], row['product_id'], row['date'], row['ctr']))

    # Confirmo y cierro la conexión con la base
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='AdTech_DAG',
    schedule='0 0 * * *',
    start_date=datetime(2024, 5, 1), #por cómo está construído, arranca el 2 para levantar los datos del 1
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    
    filtrar_product_views = PythonOperator(
        task_id='filtro_productos',
        python_callable=filtrar_product_views,
        provide_context=True, #agrego el provide_context para que tome de cada función las variables de contexto de execution_date
    )

    filtrar_ads_views = PythonOperator(
        task_id='filtro_ads',
        python_callable=filtrar_ads_views,
        provide_context=True,
    )
    
    TopProduct = PythonOperator(
        task_id='Modelo_TopProduct',
        python_callable=top_product,
        provide_context=True,
    )    
    
    TopCTR = PythonOperator(
        task_id='Modelo_CTR',
        python_callable=top_CTR,
        provide_context=True,
    )    
    
    DBWriting = PythonOperator(
        task_id='DB_Writing',
        python_callable=DBWriting,
        provide_context=True,
    ) 

    filtrar_product_views >> TopProduct
    filtrar_ads_views >> TopCTR
    [TopProduct, TopCTR] >> DBWriting
