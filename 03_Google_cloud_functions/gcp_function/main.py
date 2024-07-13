from bq_manager import BqManager

def gcs_trigger(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    bucket = event["bucket"]
    name = event["name"]
    gcs_uri = f"gs://{bucket}/{name}"


    bq_client = BqManager(project_id = 'alt-school-423017')
    job_config = bq_client.fetch_job_config(file_format="csv",schema=None)

    # load data into bigquery
    table = f'alt_ecommerce.customer_orders'  # table name
    bq_client.upload_data_to_table_from_uri(
            gcs_uri=gcs_uri, destination_table=table, job_config=job_config
        )

