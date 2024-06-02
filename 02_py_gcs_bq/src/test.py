import config as c
from gcs_manager import GcsManager
from dotenv import load_dotenv
import os
from get_request_api import get_request

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACCOUNT_AUTH_FILE")

client = GcsManager(project_id=c.PROJECT_ID)

client.upload_file(
    bucket_name="etl_basics_staging",
    file_path="/workspaces/AltschoolfinalsemesterPortfolio/02_py_gcs_bq/data/usa_names.csv",
    destination_blob_name="usa_names.csv",
)

json_data = get_request(url=c.URL)

client.upload_file_from_filestream(
    bucket_name="etl_basics_staging", destination_blob_name="games.json", file_obj=json_data
)

# client.get_all_bucket_files(bucket_name="etl_basics_staging")

# client.delete_file(bucket_name="etl_basics_staging",blob_name="usa_names.csv")
