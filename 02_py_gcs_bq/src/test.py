import config as c
from gcs_manager import GcsManager
from bq_manager import BqManager
from dotenv import load_dotenv
import os
import get_request_api as api
import json

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACCOUNT_AUTH_FILE")



"""
gcs_client.create_bucket(bucket_name="etl_basics_staging")

#gcs_client.delete_bucket(bucket_name="etl_basics_staging2")

gcs_client.upload_file(
    bucket_name="etl_basics_staging2",
    file_path="/workspaces/AltschoolfinalsemesterPortfolio/02_py_gcs_bq/data/usa_names.csv",
    destination_blob_name="usa_names.csv",
)

json_data = get_request(url=c.URL)
json_data = get_request(url=f"{c.BASE_URL}/{c.TARGET_ENDPOINT}")

gcs_client.upload_file_from_filestream(
    bucket_name="etl_basics_staging", destination_blob_name="games.json", file_obj=json_data
)

# client.get_all_bucket_files(bucket_name="etl_basics_staging")

# client.delete_file(bucket_name="etl_basics_staging",blob_name="usa_names.csv")

"""

bq_client = BqManager(project_id=c.PROJECT_ID)


#bq_client.create_dataset(dataset_id="etl_basics_staging")


#bq_client.create_table(schema_file_path=c.SCHEMA_FILE_PATH,dataset_id="etl_basics_staging",table_id="usa_names")
#bq_client.create_table(schema_file_path="/workspaces/AltschoolfinalsemesterPortfolio/02_py_gcs_bq/schemas/games_schema.json",dataset_id="etl_basics_staging",table_id="games")

#json_data = api.get_request_jsonl(url=f'{c.BASE_URL}/{c.TARGET_ENDPOINT}')

#gcs_client = GcsManager(project_id=c.PROJECT_ID)

#gcs_uri = gcs_client.upload_file_from_filestream(
 #   bucket_name="etl_basics_staging", destination_blob_name="games.json", file_obj=json_data
#)

gcs_uri="gs://etl_basics_staging/games.json"

#upload csv data to table.
with open(c.SCHEMA_FILE_PATH,'r') as schemaFile :
    schema = json.load(schemaFile)

job_config =bq_client.fetch_job_config(file_format='json',schema=schema)

bq_client.upload_data_to_table_from_uri(gcs_uri=gcs_uri,destination_table="etl_basics_staging.games",job_config=job_config)




#with open("/workspaces/AltschoolfinalsemesterPortfolio/02_py_gcs_bq/data/usa_names.csv", "rb") as source_file:
#    bq_client.upload_data_to_table_from_file(file_obj=source_file,destination_table='etl_basics_staging.usa_names',job_config=job_config)





#infer table schema from incoming data


#flow
#extract data from api => create  bucket => load data into bucket => create dataset => create table => load gcs data into data.

#add exceptions for when bucket does not exist. file does not exist, data set does not exist and table does not exist