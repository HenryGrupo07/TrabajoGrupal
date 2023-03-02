import logging
import csv
import pandas as pd
from datetime import timedelta, datetime
import re
import reverse_geocoder as rg
import numpy as np
from funcion import function
#from  function import dinamizar_lista_a_columna, get_state, us_state_to_abbrev, abbrev_to_us_state,get_descrip_state 
#from function import formatear_columnas, only_dict, reduccion_categoria, list_to_lower

from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook 
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.file_to_gcs import FileToGoogleCloudStorageOperator
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

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
    'start_date':  datetime(2023,2,25),
    'retry_delay': timedelta(minutes=5),
}

def ETL():
    sitios_hotels = pd.read_csv("gs://datapf-bucket-1/data/sitios_hoteles.csv",index_col=0)
    
    transformData(sitios_hotels)

def transformData(sitios_hotels):
    
    # transformaciones tabla sitios_hotel
    #### tiempo ejecucion 3min ####
    #pasamos a mayus la dir
    sitios_hotels["address"] = sitios_hotels["address"].str.upper()
    #corremos la funcion de regular expression que sirve para encontrar_AA_#####, que seria a abreviatura y el codigo postal.
    sitios_hotels["state_xx"] = sitios_hotels["address"].apply(lambda x: function.get_state(x))
    #pasamos el diccionario para saber a que estado se refiere el proyecto.
    sitios_hotels["state_name"] = sitios_hotels["state_xx"].str.strip().map(function.abbrev_to_us_state)
    #para aquellos que sí encontró, vamos a sacar la ciudad.
    sitios_hotels["city_name"] = sitios_hotels.apply(lambda row: np.nan if pd.isnull(row.state_xx) else (row.address.split(",")[2].strip() if len(row.address.split(","))==4 else np.nan),axis=1)
    #para aquellos que no encontramos, vamos a correr la funcion para hallar segun lat y long
    ##primero creamos la columnas de tuplas lat,long
    sitios_hotels["coordenadas"] = list(zip(sitios_hotels["latitude"],sitios_hotels["longitude"]))
    sitios_hotels["reverse_coor"] = sitios_hotels.apply(lambda row: rg.search(row.coordenadas) if pd.isnull(row.state_name) else np.nan, axis=1)
    sitios_hotels["state_name"] = sitios_hotels.apply(lambda row: row.reverse_coor[0]["admin1"] if pd.notnull(row.reverse_coor) else row.state_name, axis=1)
    sitios_hotels["state_name"] = sitios_hotels["state_name"].str.upper()
    sitios_hotels["state_name"] = sitios_hotels["state_name"].str.replace("DISTRICT OF COLUMBIA","WASHINGTON, D.C.")
    sitios_hotels["city_name"] = sitios_hotels.apply(lambda row: row.reverse_coor[0]["admin2"] if pd.notnull(row.reverse_coor) else row.city_name, axis=1)
    sitios_hotels["city_name"] = sitios_hotels["city_name"].str.upper()
    sitios_hotels['state_name'] = sitios_hotels.state_name.str.title()
    sitios_hotels['city_name'] = sitios_hotels.city_name.str.title()
    
    #abrir columna MISC
    df_misc = sitios_hotels.dropna(subset="MISC")
    df_misc_dinamizado = pd.json_normalize(df_misc["MISC"].apply(function.only_dict).tolist())
    sitios_hotels = function.formatear_columnas(df_misc_dinamizado,sitios_hotels)

    #clasificar los hoteles
    sitios_hotels["name"] = sitios_hotels.name.apply(lambda x: "Sin nombre" if pd.isnull(x) else x)
    sitios_hotels["category"] = sitios_hotels["category"].apply(function.only_dict).tolist()
    sitios_hotels["category"] = sitios_hotels["category"].apply(function.list_to_lower)
    sitios_hotels["cat_name"] = sitios_hotels.name.apply(lambda x: function.reduccion_categoria(x.lower()) if pd.notnull(x) else x)
    sitios_hotels["cat_name"] = sitios_hotels.apply(lambda row: function.reduccion_categoria(str(row.category)) if row.cat_name == "Otros" else row.cat_name, axis=1)
    sitios_hotels.drop(sitios_hotels[sitios_hotels["cat_name"]=="Dinner"].index,inplace=True)
    sitios_hotels.reset_index(drop=True,inplace=True)
    
    #abrir columnas creadas por MISC
    for i in df_misc_dinamizado.columns:
        function.dinamizar_lista_a_columna(sitios_hotels,i)
    sitios_hotels.drop(df_misc_dinamizado.columns,axis=1,inplace=True)
    
    #Agrupacion de columnas por servicios importantes
    sitios_num_col = len(sitios_hotels.columns)
    sitios_hotels["t_wheelchair"] = \
    sitios_hotels["Wheelchair accessible elevator"] + \
    sitios_hotels["Wheelchair accessible entrance"] + \
    sitios_hotels["Wheelchair accessible parking lot"] + \
    sitios_hotels["Wheelchair accessible restroom"] + \
    sitios_hotels["Wheelchair accessible seating"] + \
    sitios_hotels["Wheelchair-accessible car park"] + \
    sitios_hotels["Wheelchair-accessible entrance"] + \
    sitios_hotels["Wheelchair-accessible lift"] + \
    sitios_hotels["Wheelchair-accessible seating"] + \
    sitios_hotels["Wheelchair-accessible toilet"]

    sitios_hotels["t_restaurant"] = \
    sitios_hotels["All you can eat"] + \
    sitios_hotels["Braille menu"] + \
    sitios_hotels["Catering"] + \
    sitios_hotels["Comfort food"] + \
    sitios_hotels["Dessert"] + \
    sitios_hotels["Dine-in"] + \
    sitios_hotels["Dinner"] + \
    sitios_hotels["Dinner reservations recommended"] + \
    sitios_hotels["Drive-through"] + \
    sitios_hotels["Fast service"] + \
    sitios_hotels["Food"] + \
    sitios_hotels["Great coffee"] + \
    sitios_hotels["Great dessert"] + \
    sitios_hotels["Great tea selection"] + \
    sitios_hotels["Halal food"] + \
    sitios_hotels["Happy hour food"] + \
    sitios_hotels["Happy-hour food"] + \
    sitios_hotels["Healthy options"] + \
    sitios_hotels["In-store pickup"] + \
    sitios_hotels["In-store shopping"] + \
    sitios_hotels["Kids' menu"] + \
    sitios_hotels["Late-night food"] + \
    sitios_hotels["Lunch"] + \
    sitios_hotels["Organic dishes"] + \
    sitios_hotels["Outside food allowed"] + \
    sitios_hotels["Quick bite"] + \
    sitios_hotels["Restaurant"] + \
    sitios_hotels["Small plates"] + \
    sitios_hotels["Solo dining"] + \
    sitios_hotels["Vegetarian options"] 

    sitios_hotels["t_bar"] = \
    sitios_hotels["Alcohol"] + \
    sitios_hotels["Bar games"] + \
    sitios_hotels["Bar on site"] + \
    sitios_hotels["Bar onsite"] + \
    sitios_hotels["Beer"] + \
    sitios_hotels["Cocktails"] + \
    sitios_hotels["Food at bar"] + \
    sitios_hotels["Great bar food"] + \
    sitios_hotels["Great beer selection"] + \
    sitios_hotels["Great cocktails"] + \
    sitios_hotels["Great wine list"] + \
    sitios_hotels["Happy hour drinks"] + \
    sitios_hotels["Happy-hour drinks"] + \
    sitios_hotels["Hard liquor"] + \
    sitios_hotels["Salad bar"] + \
    sitios_hotels["Spirits"] + \
    sitios_hotels["Wine"]

    sitios_hotels["t_child_care"] = \
    sitios_hotels["Child care"] + \
    sitios_hotels["Good for kids"] + \
    sitios_hotels["High chairs"]

    sitios_hotels["t_familiar_group"] = \
    sitios_hotels["Family friendly"] + \
    sitios_hotels["Family-friendly"] + \
    sitios_hotels["Groups"]

    sitios_hotels["t_all_payment_method"] = \
    sitios_hotels["Checks"] + \
    sitios_hotels["Cheques"] + \
    sitios_hotels["Credit cards"] + \
    sitios_hotels["Debit cards"] + \
    sitios_hotels["Membership required"] + \
    sitios_hotels["NFC mobile payments"]

    sitios_hotels["t_lgtbq_friendly"] = \
    sitios_hotels["Gender-neutral restroom"] + \
    sitios_hotels["Gender-neutral toilets"] + \
    sitios_hotels["LGBTQ friendly"] + \
    sitios_hotels["LGBTQ-friendly"] + \
    sitios_hotels["Transgender safespace"]

    sitios_hotels["t_outdoor_seating"] = \
    sitios_hotels["Outdoor seating"] + \
    sitios_hotels["Rooftop seating"] + \
    sitios_hotels["Seating"] + \
    sitios_hotels["Stadium seating"]

    sitios_hotels["t_breakfast"] = \
    sitios_hotels["Breakfast"] + \
    sitios_hotels["Coffee"]

    sitios_hotels["t_delivery"] = \
    sitios_hotels["Delivery"] + \
    sitios_hotels["Takeaway"] + \
    sitios_hotels["Takeout"]

    sitios_hotels["t_live_performance"] = \
    sitios_hotels["Live music"] + \
    sitios_hotels["Live performances"]

    sitios_hotels["t_covid"] = \
    sitios_hotels["Mask required"] + \
    sitios_hotels["No-contact delivery"] + \
    sitios_hotels["Staff get temperature checks"] + \
    sitios_hotels["Staff required to disinfect surfaces between visits"] + \
    sitios_hotels["Staff wear masks"] + \
    sitios_hotels["Temperature check required"]

    sitios_hotels["t_fireplace"] = \
    sitios_hotels["Fireplace"]

    cat_cols = ['t_wheelchair', 't_restaurant', 't_bar','t_child_care', 't_familiar_group', 't_all_payment_method','t_lgtbq_friendly', 't_outdoor_seating', 't_breakfast', 't_delivery','t_live_performance', 't_covid', 't_fireplace']
    for i in cat_cols:
        sitios_hotels[i] = sitios_hotels[i].apply(lambda x: 1 if x>0 else 0)
    #dropeamos todas  las columnas que no vamos a usar para nada
    sitios_hotels.drop(sitios_hotels.iloc[:,23:sitios_num_col].columns,inplace=True,axis=1)
    sitios_hotels.drop_duplicates(subset=['name','address','latitude','longitude'],inplace=True)
    sitios_hotels.reset_index(inplace=True,drop=True)
    sitios_hotels.rename(columns={"name":"name_hotel"},inplace=True)
    
    sitios_hotels.to_csv("gs://datapf-bucket-1/etl/sitios_hotels_etl.csv",index=False)
    

with DAG('SitiosWarehouse', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
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
    
    sitioswarehouse = GCSToBigQueryOperator(
        task_id = 'sitioswarehouse',
        bucket = BUCKET_NAME,
        source_objects = ['etl/sitios_hotels_etl.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_sitios',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name':'name_hotel', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'address', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'gmap_id', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'description', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'latitude', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'longitude', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'category', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'avg_rating', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'num_of_reviews', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'price', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'hours', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'MISC', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'state', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'relative_results', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'url', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'url_origen', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'etl_timestamp', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'state_xx', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'state_name', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'city_name', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'coordenadas', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'reverse_coor', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'cat_name', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_wheelchair', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_restaurant', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_bar', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_child_care', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_familiar', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_all_payment_method', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_lgtbq_friendly', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_outdoor_seating', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_breakfast', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_delivery', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_live_performance', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_covid', 'type':'string', 'mode': 'NULLABLE'},
        {'name':'t_fireplace', 'type':'string', 'mode': 'NULLABLE'},
            ]
        )  
    

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
start_pipeline >> load_staging_dataset>> etl_storage>>sitioswarehouse>> finish_pipeline