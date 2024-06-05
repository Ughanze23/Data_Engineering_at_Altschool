DATA_SET_NAME = "etl_basics"  # bigquery dataset name
PROJECT_ID = "alt-school-423017"  # GC project id
BUCKECT_NAME = ""  # GCS bucket name
TABLE_NAME = ""  # bigquery table_name
GCS_UPLOAD_FILE_PATH = ""  # file path to upload a blob to GCS
GCS_FILE_NAME = ""  # destination blob name in GCS
DESTINATION_FILE_NAME = ""  # downloaded file from gcs file path
BASE_URL = (
    "https://api.sampleapis.com/playstation"  # URL provided to get request api script
)
TARGET_ENDPOINT = "games"  # goes with the baseURL
SCHEMA_FILE_PATH = ""  # file path of bigquery table json schema
