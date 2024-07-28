from airflow.decorators  import dag 
from airflow.decorators import task
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator

BUCKECT_NAME = "prj_alt_school_ecommerce"   
schemas = {
    "customers" : ["customers"],
 "orders" : ["orders","order_items","order_payments","order_reviews"],
 "products" : ["products","product_category"],
 "sellers" : ["sellers"],
"geo_location" : ["geolocation"]
}

dag_owner = 'ikenna_altschool'

default_args = {'owner': dag_owner,
        'depends_on_past': False
        }

@dag(dag_id='ecommerce_postgres_to_biquery_pipeline',
        default_args=default_args,
        description='A simple piple that moves that from a production postgres db to bigquery tables.',
        start_date=datetime(2024,6,25),
        schedule_interval= None,
        catchup=False,
        tags=['ecommerce']
    )

def ecommerce_pipeline():

    db_to_gcs = PostgresToGCSOperator(
                 task_id=f'extract_customers_to_GCS',
                    postgres_conn_id='prod_db',
                    sql=f'SELECT * FROM customers.customers;',
                    bucket=BUCKECT_NAME,
                    filename=f'data/customers_data.parquet',
                    export_format='parquet',
                    gzip=False
             )

    #test the connection works
    PostgresOperator_task = PostgresOperator(
    task_id='PostgresOperator_task',
    postgres_conn_id='prod_db',
    sql = """select * from  customers.customers"""
    )
    """ 
    postgres_to_gcs_tasks = []
    for schema, tables in schemas.items():
        for table in tables:
            task = PostgresToGCSOperator(
                 task_id=f'extract_{table}_to_GCS',
                    postgres_conn_id='prod_db',
                    sql=f'SELECT * FROM {schema}.{table};',
                    bucket=BUCKECT_NAME,
                    filename=f'data/{table}_data.parquet',
                    export_format='parquet',
                    gzip=False
             )
        postgres_to_gcs_tasks.append(task)
    

    transfer_customers_postgres_to_biquery = ""
    
    postgres_to_gcs_tasks
    """
    db_to_gcs
ecommerce_pipeline()
