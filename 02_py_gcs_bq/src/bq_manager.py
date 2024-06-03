from google.cloud import bigquery
import logging
import json

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")


class BqManager:
    """Interact with Google bigquery resource"""
    
    def __init__(self,project_id) -> None:
        self.project_id = project_id
        self.client = self._create_bq_client()
    
    def _create_bq_client(self) -> bigquery.Client:
        """Creates a Google Cloud Bigquery client object

        Returns:
        Object : returns a Bigquery client object
        """
        try:
            bq_client = bigquery.Client(project=self.project_id)
            logging.info(f"Successfully connected to Bigquery resource on project: {self.project_id}")
            return bq_client

        except Exception as e:
            logging.error(f"Error connecting to Bigquery: {e}")
            raise e

    
    def create_dataset(self,dataset_id:str,location:str="US") -> None:
        """Create a dataset in Bigquery.

        Args:
        dataset_id: dataset name in bigquery.
        location: what location the dataset should be created in(defaults to US).
        """
        try :
            dataset_ref = self.client.dataset(dataset_id)
            dataset = self.client.create_dataset(dataset_ref)

            logging.info(f"Dataset: {dataset_id} created succesfully")

        except Exception as e:
            logging.error(f"Could not create dataset: {e}")

    def create_table(self,file_path:str,dataset_id:str,table_id:str) -> None:
        """Create table in Bigquery 
        
        Args:
        file_path: file path of bigquery table json schema.
        dataset_id: dataset name in bigquery.
        table_id: table name in bigquery.

        """

        with open(file_path) as schema_file:
            schema = json.load(schema_file)

        #creat table reference
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref,schema=schema)

        #create table
        table = self.client.create_table(table)

        logging.info(f"{table.table_id} table created.")

    #upload data to table

    #drop table