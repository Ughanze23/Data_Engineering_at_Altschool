DATA_SET_NAME = "etl_basics"  # bigquery dataset name
PROJECT_ID = "alt-school-423017"  # GC project id
BUCKECT_NAME = "etl_basics"  # GCS bucket name
TABLE_NAME = ["usa_names", "games"]  # bigquery table_name
GCS_UPLOAD_FILE_PATH = "/workspaces/AltschoolfinalsemesterPortfolio/02_py_gcs_bq/data/usa_names.csv"  # file path to upload a blob to GCS
GCS_FILE_NAME = ["usa_name.csv", "games.json"]  # destination blob name in GCS
DESTINATION_FILE_NAME = ""  # downloaded file from gcs file path
BASE_URL = (
    "https://api.sampleapis.com/playstation"  # URL provided to get request api script
)
TARGET_ENDPOINT = "games"  # goes with the baseURL
SCHEMA_FILE_PATH = [
    "/workspaces/AltschoolfinalsemesterPortfolio/02_py_gcs_bq/schemas/usa_names_schema.json",
    "/workspaces/AltschoolfinalsemesterPortfolio/02_py_gcs_bq/schemas/games_schema.json",
]  # file path of bigquery table json schema

BQ_FILE_UPLOAD_PATH = (
    "/workspaces/AltschoolfinalsemesterPortfolio/02_py_gcs_bq/data/usa_names.csv"
)

# Dictionary to store all table configurations
table_configs = {
    "usa_names": {
        "TABLE_NAME": "usa_names",
        "GCS_FILE_NAME": "usa_name.csv",
        "SCHEMA_FILE_PATH": "../schemas/usa_names_schema.json",
    },
    "games": {
        "TABLE_NAME": "games",
        "GCS_FILE_NAME": "games.json",
        "SCHEMA_FILE_PATH": "../schemas/games_schema.json",
    },
}
