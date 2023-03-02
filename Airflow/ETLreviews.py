import logging
import csv
import pandas as pd
from datetime import timedelta, datetime
import re
import numpy as np
import string
from nltk.corpus import stopwords
import nltk
nltk.download('stopwords')
from functionreviews import functionreviews
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="proyectohenry-378715"
GS_PATH = "data/"
BUCKET_NAME = 'datapf-bucket-1'
STAGING_DATASET = "datawarehouse"
DATASET = "data_dataset"
LOCATION = "us"

default_args = {
    'owner': 'Cesar Quinayas',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    #start_date':  days_ago(2),
    'start_date':  datetime(2023,2,27),
    'retry_delay': timedelta(minutes=5),
}



def ETL():
    reviews_hoteles = pd.read_csv("gs://datapf-bucket-1/data/reviews_hoteles.csv",index_col=0)
    transformData(reviews_hoteles)

def transformData(reviews_hotels):
    # transformaciones tabla reviews_hotel
    reviews_hotels["time"]= pd.to_datetime(reviews_hotels["time"],unit='ms').dt.strftime('%Y-%m-%d %H:%M:%S')
    reviews_hotels["response"] = reviews_hotels["resp"].apply(lambda x: 0 if pd.isna(x) else 1 )
    reviews_hotels["state_from_json_url"] = reviews_hotels["url_origen"].apply(lambda x: x.split("/")[5].split("-")[1])
    reviews_hotels.drop_duplicates(subset=['name','time','text','rating','gmap_id','pics', 'resp'],inplace=True)
    reviews_hotels.reset_index(inplace=True,drop=True)
    reviews_hotels['keywords'] = reviews_hotels["text"].apply(lambda x: functionreviews.message_cleaning(x) if type(x) == str else '')
    reviews_hotels.to_csv("gs://datapf-bucket-1/etl/reviews_hoteles_etl.csv",index=False)

with DAG('ReviewsWarehouse', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipline',
        dag = dag
        )


    load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
        )  
    
    # ETL: PythonOperator
    etl_storage = PythonOperator(
        task_id='etl_storage',
        python_callable=ETL,
        dag=dag
    )
    
    reviewswarehouse = GCSToBigQueryOperator(
        task_id = 'reviewswarehouse',
        bucket = BUCKET_NAME,
        source_objects = ['etl/reviews_hoteles_etl.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_reviews',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'user_id', 'type': 'float64', 'mode': 'NULLABLE'},
        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'time', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'rating', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'text', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'pics', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'resp', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'gmap_id', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'url_origen', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'etl_timestamp', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'response', 'type': 'int64', 'mode': 'NULLABLE'},
        {'name': 'state_from_json_url', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'keywords', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )  
    

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
start_pipeline >> load_staging_dataset>> etl_storage>>reviewswarehouse>> finish_pipeline