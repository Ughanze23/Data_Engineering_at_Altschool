import random
import io
import csv
import uuid
from gcs_manager import GcsManager
from dotenv import load_dotenv
import os
import config as c

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACCOUNT_AUTH_FILE")


def generate_orders(num_orders: int = 1000) -> list:
    """Simulate the generation of ecommerce orders data

    Args:
    num_orders: number of orders to generate

    Returns:
    A list
    """
    orders = []

    for order_id in range(1, num_orders + 1):
        customer_id = random.randint(1000, 9999)
        product_id = random.randint(1, 100)
        quantity = random.randint(1, 10)
        total_amount = round(random.uniform(10, 100), 2)

        order = [order_id, customer_id, product_id, quantity, total_amount]

        orders.append(order)

    return orders


def save_as_csv(data: list) -> str:
    """Convert a list of lists to a Csv string
    Args:
    data : list of lists

    Returns:
    Csv string
    """
    csv_string = io.StringIO()
    csv_writer = csv.writer(csv_string)
    csv_writer.writerow(
        ["OrderID", "CustomerID", "ProductID", "Quantity", "TotalAmount"]
    )
    csv_writer.writerows(data)
    return csv_string.getvalue()


if __name__ == "__main__":
    # create gcs client

    gcs_client = GcsManager(project_id=c.PROJECT_ID)

    # create gcs bucket
    gcs_client.create_bucket(bucket_name=c.BUCKECT_NAME)

    orders_data = generate_orders()
    orders_csv = save_as_csv(orders_data)

    
    
    unique_id = str(uuid.uuid4())[:8]  # generate random 8 - character UUID
    file_name = f"customer_orders_{unique_id}.csv"
    
    # upload files to bucket
    gcs_client.upload_file_from_filestream(
        bucket_name=c.BUCKECT_NAME,
        file_obj=orders_csv,
        destination_blob_name=file_name,
        content_type="text/csv",
    ) 
