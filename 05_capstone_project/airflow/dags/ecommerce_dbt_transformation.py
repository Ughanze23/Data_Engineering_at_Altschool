from airflow.decorators  import dag 
from airflow.decorators import task
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash import BashOperator
import os


#The path to the dbt project
#DBT_PROJECT_PATH = f"{os.environ['AIRFLOW_HOME']}/dbt/"
# The path where Cosmos will find the dbt executable
# in the virtual environment created in the Dockerfile
#DBT_EXECUTABLE_PATH = f"/opt/airflow/dbt_venv/bin/dbt"
dag_owner = 'ikenna_altschool'

default_args = {'owner': dag_owner,
        'depends_on_past': False
        }


@dag(dag_id='ecommerce_data_transformations',
        default_args=default_args,
        description='transform data in ecommerce bigquery tables.',
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

def ecommerce_dbt():

    # Sensor to wait for the completion of ecommerce_postgres_to_biquery_pipeline DAG
    wait_for_ecommerce_pipeline = ExternalTaskSensor(
        task_id='wait_for_ecommerce_postgres_to_biquery_pipeline',
        external_dag_id='ecommerce_postgres_to_biquery_pipeline',  # The ID of the DAG to wait for
        external_task_id=None,  # Wait for the entire DAG (set this to a specific task ID if needed)
        allowed_states=['success'],  # Wait for the DAG to succeed
        failed_states=['failed'],  # Treat failed/skipped states as failure
        mode='poke',  # Use 'poke' mode to check regularly, or use 'reschedule' mode to reduce load
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt"
    )
    dbt_run
ecommerce_dbt()

