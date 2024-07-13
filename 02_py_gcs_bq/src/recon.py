import config as c
from gcs_manager import GcsManager
from bq_manager import BqManager
from dotenv import load_dotenv
import os
import get_request_api as api
import json

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACCOUNT_AUTH_FILE")


#create gcs client
project_id = 'alt-school-423017'
bucket ="etl_basics2"
gcs_client = GcsManager(project_id=project_id)



def get_gcs_txt_files(file_name)->str: 
    """get file from gcp and return a string """
    file = gcs_client.download_file_to_stream(bucket_name=bucket,source_blob_name=file_name)
    file = file.getvalue().decode("utf-8")
    return file


nips_zip_folders = get_gcs_txt_files('nips_zipfolders.txt') 
inflow_gcs = get_gcs_txt_files('inflow.txt')
outflow_gcs = get_gcs_txt_files('outflow.txt')

print(type(nips_zip_folders.split(",")))





 
    


