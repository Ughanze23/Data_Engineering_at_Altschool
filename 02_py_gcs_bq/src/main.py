import config as c
from gcs_manager import GcsManager
from bq_manager import BqManager
from dotenv import load_dotenv
import os
import get_request_api as api
import json

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACCOUNT_AUTH_FILE")


if __name__ == "__main__":

    # create gcs and bq clients
    gcs_client = GcsManager(project_id=c.PROJECT_ID)
    bq_client = BqManager(project_id=c.PROJECT_ID)

    # create bucket
    gcs_client.create_bucket(bucket_name=c.BUCKECT_NAME)

    # create dataset
    bq_client.create_dataset(dataset_id=c.DATA_SET_NAME, location="EU")

    # create tables
    bq_client.create_table(
        schema_file_path=c.SCHEMA_FILE_PATH[0],
        dataset_id=c.DATA_SET_NAME,
        table_id=c.TABLE_NAME[0],
    )
    bq_client.create_table(
        schema_file_path=c.SCHEMA_FILE_PATH[1],
        dataset_id=c.DATA_SET_NAME,
        table_id=c.TABLE_NAME[1],
    )

    # load csv data from system into bigquery table
    # fetch table schema
    with open(c.SCHEMA_FILE_PATH[0]) as schema_file:
        schema = json.load(schema_file)

    # fetch bigquery job config
    job_config = bq_client.fetch_job_config(file_format="csv", schema=schema)

    # read file as bytes
    with open(c.BQ_FILE_UPLOAD_PATH, "rb") as file:
        # upload data to biquery
        table = f"{c.DATA_SET_NAME}.{c.TABLE_NAME[0]}"  # table name
        bq_client.upload_data_to_table_from_file(
            file_obj=file, destination_table=table, job_config=job_config
        )

    # upload data from API into >> GCS then load into >> Bigquery table
    # fetch data from API
    gcs_data = api.get_request_jsonl(url=f"{c.BASE_URL}/{c.TARGET_ENDPOINT}")

    # upload json data into gcs
    gcs_uri = gcs_client.upload_file_from_filestream(
        file_obj=gcs_data,
        bucket_name=c.BUCKECT_NAME,
        destination_blob_name=c.GCS_FILE_NAME[1],
    )

    # load data from gcs into bigquery
    # fetch table schema
    with open(c.SCHEMA_FILE_PATH[1]) as schema_file:
        schema = json.load(schema_file)

    # fetch bigquery job config
    job_config = bq_client.fetch_job_config(file_format="json", schema=schema)

    # load data into bigquery
    table = f"{c.DATA_SET_NAME}.{c.TABLE_NAME[1]}"  # table name
    bq_client.upload_data_to_table_from_uri(
        gcs_uri=gcs_uri, destination_table=table, job_config=job_config
    )
