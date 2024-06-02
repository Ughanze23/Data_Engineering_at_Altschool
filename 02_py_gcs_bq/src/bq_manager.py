from google.cloud import bigquery
import logging


logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")


class BqManager:
    """Interact with Google bigquery resource"""
    
    def __init__(self,project_id) -> None:
        self.project_id = project_id
        self.client = self._create_bq_client(self)
    
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