from google.cloud import bigquery
from google.cloud import exceptions
import logging
import json
from os import PathLike
import re
from typing import Literal
from typing import IO

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")


class BqManager:
    """Interact with Google bigquery resource"""

    def __init__(self, project_id) -> None:
        self.project_id = project_id
        self.client = self._create_bq_client()

    def _create_bq_client(self) -> bigquery.Client:
        """Creates a Google Cloud Bigquery client object

        Returns:
        Object : returns a Bigquery client object
        """
        try:
            bq_client = bigquery.Client(project=self.project_id)
            logging.info(
                f"Successfully connected to Bigquery resource on project: {self.project_id}"
            )
            return bq_client

        except Exception as e:
            logging.error(f"Error connecting to Bigquery: {e}")
            raise e

    def create_dataset(self, dataset_id: str, location: str = "US") -> None:
        """Create a dataset in Bigquery.

        Args:
        dataset_id: dataset name in bigquery.
        location: what location the dataset should be created in(defaults to US).
        """
        try:
            dataset_ref = self.client.dataset(dataset_id)
            dataset = self.client.create_dataset(dataset_ref)

            logging.info(f"Dataset: {dataset_id} created succesfully")

        except exceptions.Conflict:
            logging.info(f"Dataset: {dataset_id} already exists")

        except Exception as e:
            logging.error(f"Could not create dataset: {e}")
            raise e

    def delete_dataset(self, dataset_id: str) -> None:
        """Delete bigquery dataset"""
        # Use the not_found_ok parameter to not receive an error if the dataset has already been deleted.
        try:
            self.client.delete_dataset(
                dataset_id, delete_contents=True, not_found_ok=True
            )
            logging.info(f"Deleted dataset '{dataset_id}'.")

        except Exception as e:
            logging.error(f"Could not delete dataset: {e}")
            raise e

    def create_table(
        self, schema_file_path: PathLike, dataset_id: str, table_id: str
    ) -> None:
        """Create table in Bigquery

        Args:
        schema_file_path: file path of bigquery table json schema.
        dataset_id: dataset name in bigquery.
        table_id: table name in bigquery.

        """
        try:
            with open(schema_file_path, "r") as schema_file:
                schema = json.load(schema_file)

            # creat table reference
            table_ref = self.client.dataset(dataset_id).table(table_id)
            table = bigquery.Table(table_ref, schema=schema)

            # create table
            table = self.client.create_table(table)

            logging.info(f"Table: {table_ref} created.")

        except exceptions.Conflict:
            logging.info(f"Table: {table_id} already exists")

        except Exception as e:
            logging.error(f"Could not create table: {table_id} - {e}")
            raise e

    def delete_table(self, table_id: str) -> None:
        """Delete table in bigquery"""

        # If the table does not exist, delete_table raises
        # google.api_core.exceptions.NotFound unless not_found_ok is True.
        try:
            self.client.delete_table(table_id, not_found_ok=True)
            logging.info(f"Deleted table '{table_id}'.")
        except exceptions as e:
            logging.error(f"Could not delete table {e}")
            raise e

    def fetch_job_config(
        self,
        file_format: Literal["csv", "json", "avro", "parquet"],
        schema,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
    ) -> bigquery.LoadJobConfig:
        # write_disposition='WRITE_TRUNCATE',WRITE_APPEND
        config_dict = {
            "json": bigquery.LoadJobConfig(
                autodetect=False,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=schema,
                create_disposition=create_disposition,
                write_disposition=write_disposition,
            ),
            "avro": bigquery.LoadJobConfig(
                autodetect=False, 
                source_format=bigquery.SourceFormat.AVRO,
                schema=schema,
                use_avro_logical_types=True,
                create_disposition=create_disposition,
                write_disposition=write_disposition,
            ),
            "csv": bigquery.LoadJobConfig(
                autodetect=True,
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                field_delimiter=",",
                quote_character="",
                schema=schema,
                create_disposition=create_disposition,
                write_disposition=write_disposition,
            ),
            "parquet": bigquery.LoadJobConfig(
                autodetect=False,
                source_format=bigquery.SourceFormat.PARQUET,
                schema=schema,
                create_disposition=create_disposition,
                write_disposition=write_disposition,
            ),
        }
        return config_dict[file_format]

    def upload_data_to_table_from_uri(
        self, gcs_uri: str, destination_table: str, job_config: bigquery.LoadJobConfig
    ) -> None:
        """Upload data to bigquery table using GCS URI"""

        gcs_uri_regex = r"^gs://([a-z0-9-._]+)/(.+)$"
        if not re.match(gcs_uri_regex, gcs_uri):
            raise ValueError("Invalid GCS URI format")
        table_name_regex = r"^([a-z0-9_-]+)\.([a-z0-9_-]+)$"
        if not re.match(table_name_regex, destination_table):
            raise ValueError("Invalid BigQuery table name format (dataset.table_name)")
        try:
            load_job = self.client.load_table_from_uri(
                source_uris=gcs_uri,
                destination=destination_table,
                job_config=job_config,
                retry=0,
            )
            load_job.result()
            logging.info("Successfully loaded data into table..")

            # get table
            table = self.client.get_table(destination_table)
            logging.info(
                f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {destination_table}"
            )

        except Exception as e:
            logging.error(f"Could not upload data into table: {e}")
            raise e

    def upload_data_to_table_from_file(
        self,
        file_obj: IO[bytes],
        destination_table: str,
        job_config: bigquery.LoadJobConfig,
    ) -> None:
        """Upload data to bigquery table from file object"""

        table_name_regex = r"^([a-z0-9_-]+)\.([a-z0-9_-]+)$"
        if not re.match(table_name_regex, destination_table):
            raise ValueError("Invalid BigQuery table name format (dataset.table_name)")
        try:
            load_job = self.client.load_table_from_file(
                file_obj=file_obj, destination=destination_table, job_config=job_config
            )
            load_job.result()
            logging.info("Successfully loaded data into table..")

            # get table
            table = self.client.get_table(destination_table)
            logging.info(
                f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {destination_table}"
            )

        except Exception as e:
            logging.error(f"Could not upload data into table: {e}")
            raise e
