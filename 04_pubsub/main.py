from dotenv import load_dotenv
from google.cloud import pubsub_v1
import os
from faker import Faker
import json
from google.api_core.exceptions import GoogleAPIError
import time

PROJECT_ID = "alt-school-423017"
TOPIC_ID = "alt_school"
no_of_records = 1500

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("SERVICE_ACCOUNT_AUTH_FILE")
fake = Faker()

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project=PROJECT_ID, topic=TOPIC_ID)


def generate_fake_user_data() -> dict:
    """Generate customer data
    Returns:
    Dict:customer information.
    """
    return {
        "name": fake.name(),
        "address": fake.address(),
        "email": fake.email(),
        "phone_number": fake.phone_number(),
        "birthdate": fake.date_of_birth().isoformat(),
        "created_at": fake.date_time_this_year().isoformat(),
    }


def publish_message(data) -> None:
    """Publish data to pubsub topic"""
    try:
        # Convert data into JSON string
        data_str = json.dumps(data)
        # Convert string data into bytestring
        data_bytes = data_str.encode("utf-8")

        # Publish the message
        future = publisher.publish(topic_path, data_bytes)
        print(f"Published message ID: {future.result()}")

    except (GoogleAPIError, json.JSONDecodeError, Exception) as e:
        print(f"Failed to publish message: {e}")


if __name__ == "__main__":
    for i in range(no_of_records):
        user_data = generate_fake_user_data()
        publish_message(user_data)
        time.sleep(5)
