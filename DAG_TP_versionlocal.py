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

def filtrar_product_views():
    product_views = pd.read_csv("/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/product_views")
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    product_views_yesterday = product_views[product_views['date'] == yesterday]
    
    advertiser_ids = pd.read_csv("/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/advertiser_ids")
    active_advertiser_ids = advertiser_ids['advertiser_id']
    product_views_filtered = product_views_yesterday[product_views_yesterday['advertiser_id'].isin(active_advertiser_ids)]

    product_views_filtered.to_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/filtered_product_views.csv', index=False)

def filtrar_ads_views():
    ads_views = pd.read_csv("/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/ads_views")
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    ads_views_yesterday = ads_views[ads_views['date'] == yesterday]
    advertiser_ids = pd.read_csv("/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/advertiser_ids")
    active_advertiser_ids = advertiser_ids['advertiser_id']
    ads_views_filtered = ads_views_yesterday[ads_views_yesterday['advertiser_id'].isin(active_advertiser_ids)]
    ads_views_filtered.to_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/filtered_ads_views.csv', index=False)

def top_product():
    # Calculo la fecha del día anterior
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Cargo el archivo de product_views del día anterior
    #archivo = f'product_views_{yesterday}.csv'
    product_views = pd.read_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/filtered_product_views')

    # Calculo los 20 product_id más frecuentes por advertiser_id
    top_product_ids = product_views.groupby(['advertiser_id', 'product_id']).size() \
                                   .groupby(level=0, group_keys=False) \
                                   .nlargest(20) \
                                   .reset_index(name='count')
    # Agrego la fecha del día
    top_product_ids['date'] = yesterday
    
    top_product_ids.to_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/top_product_ids.csv', index=False)

def top_CTR():
    # Calculo la fecha del día anterior
    yesterday = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    # Cargo el archivo de ads_views del día anterior
    ads_views = pd.read_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/filtered_ads_views')

    # Filtro por impresiones y clics
    clicks = ads_views[ads_views['type'] == 'click'].groupby(['advertiser_id', 'product_id']).size()
    impressions = ads_views[ads_views['type'] == 'impression'].groupby(['advertiser_id', 'product_id']).size()

    # Calculo el CTR
    ctr = (clicks / impressions).fillna(0).reset_index(name='CTR')

    # Ordeno por CTR y seleccionar los 20 primeros o menos
    top_ctr_ids = ctr.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'CTR')).reset_index(drop=True)
    # Agrego la fecha del día
    top_ctr_ids['date'] = yesterday

    top_ctr_ids.to_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/top_ctr_ids.csv', index=False)

def DBWriting():
    top_product_ids = pd.read_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/top_product_ids')
    top_ctr_ids = pd.read_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/top_ctr_ids')
    top_product_df = top_product_ids.reset_index()#.rename(columns={'advertiser_id': 'advertiser_id','product_id':'product_id', 0: 'count', 'date':'date'})
    top_ctr_df = top_ctr_ids.reset_index()#.rename(columns={'advertiser_id': 'advertiser_id','product_id':'product_id', 0: 'CTR', 'date':'date'})
    
    if os.path.exists('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/TopProduct.csv'):
        top_product_df.to_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/TopProduct.csv', mode='a', header=False, index=False)
    else:
        top_product_df.to_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/TopProduct.csv', index=False)

    if os.path.exists('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/TopCTR'):
        top_ctr_df.to_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/TopCTR.csv', mode='a', header=False, index=False)
    else:
        top_ctr_df.to_csv('/wsl.localhost/Ubuntu-20.04/home/matias_gutman/airflow/dags/TopCTR.csv', index=False)
    
    #Guardo los resultados en un nuevo archivo 'ads_views_date.csv' donde 'date' hace referencia a la fecha del día anterior
    #top_product_df.to_csv(f'TopProduct.csv', mode='a', header=False, index=False)
    #top_ctr_df.to_csv(f'TopCTR.csv', mode='a', header=False, index=False)

with DAG(
    dag_id='AdTech',
    schedule='0 0 * * *',
    start_date=datetime(2024, 5, 2), #por cómo está construído, arranca el 2 para levantar los datos del 1
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
) as dag:
    
    filtrar_product_views = PythonOperator(
        task_id='filtro_productos',
        python_callable=filtrar_product_views,
    )

    filtrar_ads_views = PythonOperator(
        task_id='filtro_ads',
        python_callable=filtrar_ads_views,
    )
    
    TopProduct = PythonOperator(
        task_id='Modelo_TopProduct',
        python_callable=top_product,
    )    
    
    TopCTR = PythonOperator(
        task_id='Modelo_CTR',
        python_callable=top_CTR,
    )    
    
    DBWriting = PythonOperator(
        task_id='DB_Writing',
        python_callable=DBWriting,
    ) 

    filtrar_product_views >> TopProduct
    filtrar_ads_views >> TopCTR
    [TopProduct, TopCTR] >> DBWriting
