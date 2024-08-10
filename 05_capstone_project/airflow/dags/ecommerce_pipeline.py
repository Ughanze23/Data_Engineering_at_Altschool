from airflow.decorators  import dag 
from airflow.decorators import task
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.slack.notifications.slack import send_slack_notification

BUCKECT_NAME = "prj_alt_school_ecommerce"   
PROJECT_ID="alt-school-423017"
DATASET="raw_ecommerce"

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
        tags=['ecommerce'],
        on_success_callback=[
        send_slack_notification(
            slack_conn_id='slack',
            text="The DAG {{ dag.dag_id }} succeeded",
            channel="#data-engineering",
            username="Airflow",
        )
    ],
    on_failure_callback=[
        send_slack_notification(
            slack_conn_id='slack',
            text="The DAG {{ dag.dag_id }} failed",
            channel="#data-engineering",
            username="Airflow",
        )
    ],

    )

def ecommerce_pipeline():

    #load postgres tables to gcs
    postgres_to_gcs_tasks = []
    for schema, tables in schemas.items():
        for table in tables:
            if table != 'order_reviews':
                task = PostgresToGCSOperator(
                    task_id=f'extract_{table}_to_GCS',
                    postgres_conn_id='prod_db',
                    sql=f'SELECT * FROM {schema}.{table};',
                    bucket=BUCKECT_NAME,
                    filename=f'data/{table}_data.csv',
                    export_format='csv',
                    gzip=False,
                    )
            else :
                task = PostgresToGCSOperator(
                task_id=f'extract_{table}_to_GCS',
                postgres_conn_id='prod_db',
                sql=f'SELECT * FROM {schema}.{table};',
                bucket=BUCKECT_NAME,
                filename=f'data/{table}_data.json',
                export_format='json',
                    )
        postgres_to_gcs_tasks.append(task)
    
    #write gcs data to bigquery tables
    gcs_to_bq_tasks = []
    for tables in schemas.values():
        for table in tables :
            if table != 'order_reviews':
                task = GCSToBigQueryOperator(
                    task_id=f'load_{table}_gcs_data_to_bigquery',
                    bucket=BUCKECT_NAME,
                    source_objects=f'data/{table}_data.csv',
                    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{table}',
                    source_format='CSV',
                    write_disposition='WRITE_TRUNCATE',
                    skip_leading_rows=1,
                    quote_character='"'
                    )
            else:
                task =  GCSToBigQueryOperator(
                    task_id=f'load_{table}_gcs_data_to_bigquery',
                    bucket=BUCKECT_NAME,
                    source_objects=f'data/{table}_data.json',
                    destination_project_dataset_table=f'{PROJECT_ID}.{DATASET}.{table}',
                    source_format="NEWLINE_DELIMITED_JSON",
                    write_disposition="WRITE_TRUNCATE",
                    autodetect=True,
                    )
        gcs_to_bq_tasks.append(task)
    
    for pg_task in range(len(postgres_to_gcs_tasks)):
        postgres_to_gcs_tasks[pg_task] >> gcs_to_bq_tasks[pg_task]
    
ecommerce_pipeline()
