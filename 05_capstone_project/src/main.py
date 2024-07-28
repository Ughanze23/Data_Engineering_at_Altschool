import config as c
from gcs_manager import GcsManager
from bq_manager import BqManager
from dotenv import load_dotenv
import os

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACCOUNT_AUTH_FILE")


if __name__ == "__main__":

    # create gcs and bq clients
    gcs_client = GcsManager(project_id=c.PROJECT_ID)
    bq_client = BqManager(project_id=c.PROJECT_ID)

    # create tables
    for prefix in c.DATA_SET_PREFIX:
        # create dataset
        bq_client.create_dataset(
            dataset_id=f"{prefix}_{c.DATA_SET_NAME}", location="EU"
        )

        for table in c.TABLE_NAME:
            bq_client.create_table(
                schema_file_path=f"../schemas/{table}_schema.json",
                dataset_id=f"{prefix}_{c.DATA_SET_NAME}",
                table_id=f"{table}",
            )
