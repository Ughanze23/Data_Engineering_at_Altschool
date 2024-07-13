from google.cloud import storage
from google.cloud import exceptions
import logging
from io import StringIO, BytesIO
from os import PathLike

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class GcsManager:
    """Manage Google Cloud Storage buckets."""

    def __init__(self, project_id: str) -> None:

        self.project_id = project_id
        self.client = self._create_gcs_client()

    def _create_gcs_client(self) -> storage.Client:
        """Creates a Google Cloud Storage client object

        Returns:
        Object : returns a storage client object
        """
        try:
            gcs_client = storage.Client(project=self.project_id)
            logging.info(
                f"Successfully connected to GCS resource on project: {self.project_id}"
            )
            return gcs_client

        except Exception as e:
            logging.error(f"Error connecting to GCS: {e}")
            raise e

    def get_all_buckets(self) -> list:
        """Fetches and returns a list of bucket names from Google Cloud Storage.

        Returns:
        list: Returns all Bucket names
        """
        try:
            buckets = self.client.list_buckets()
            # print("Buckets:")
            # for bucket in buckets:
            #    print(bucket.name)
            # print("Listed all storage buckets.")
            bucket_names = [
                bucket.name for bucket in buckets
            ]  # List comprehension to extract names
            logging.info(f"{len(bucket_names)} bucket(s) exists in this project")
            return bucket_names
        except Exception as e:
            logging.error(f"Could not fetch buckets: {e}")

    def create_bucket(
        self,
        bucket_name: str,
        location: str = "EUROPE-WEST2",
        storage_class: str = "STANDARD",
    ) -> None:
        """create a new gcs bucket.

        Args:
        bucket_name: The name of the GCS bucket
        location: Location of bucket in GC (defaults to europe-west2)
        storage_class: Storage class of bucket in GC (defaults to STANDARD)
        """
        try:
            bucket = storage.Bucket(self.client, name=bucket_name)
            # Set the storage class
            bucket.storage_class = (
                storage_class  # Or other classes like "NEARLINE", "COLDLINE", "ARCHIVE"
            )
            bucket = self.client.create_bucket(bucket_name, location=location)
            logging.info(f"Bucket: {bucket_name} created. gs://{bucket.name}")

        except exceptions.Conflict:
            logging.info(f"Bucket with name: {bucket_name} already exists.")

        except Exception as e:
            logging.error(f"Could not create bucket: {e}")
            raise e

    def delete_bucket(self, bucket_name: str) -> None:
        """delete a gcs bucket.

        Args:
        bucket_name: The name of the GCS bucket
        """
        try:
            bucket = self.client.get_bucket(bucket_name)
            bucket.delete()
            logging.info(f"Bucket {bucket_name} deleted")

        except exceptions.NotFound:
            logging.info(f"Bucket: {bucket_name} does not exist.")

        except Exception as e:
            logging.error(f"Could not delete bucket: {e}")
            raise e

    def get_all_bucket_files(self, bucket_name: str) -> list:
        """return all blobs in a bucket.

        Args:
        bucket_name: The name of the GCS bucket

        Returns:
        List: The names of all blobs in a bucket
        """
        try:
            blobs = self.client.list_blobs(bucket_name)
            blob_names = [blob.name for blob in blobs]
            logging.info(f"{len(blob_names)} file(s) found in bucket: {bucket_name} ")
            return blob_names
        except Exception as e:
            logging.error(f"Could not return blobs in bucket: {e}")

    def upload_file(
        self, bucket_name: str, file_path: PathLike, destination_blob_name: str
    ) -> None:
        """Uploads a blob to the bucket.

        Args:
        bucket_name: The name of the GCS bucket
        file_path: File location in local system
        destination_blob_name: The name the file will be called in GCS bucket

        Returns : GCS file URI
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            if blob.exists():
                logging.info(f"blob with name: {destination_blob_name} already exists.")
                return f"gs://{bucket_name}/{destination_blob_name}"
            else:
                blob.upload_from_filename(filename=file_path)
                logging.info(
                    f"{destination_blob_name} uploaded successfully into bucket - {bucket_name}"
                )
                return f"gs://{bucket_name}/{destination_blob_name}"

        except Exception as e:
            logging.error(f"Could not upload file to bucket: {e}")
            raise e

    def upload_file_from_filestream(
        self,
        bucket_name: str,
        file_obj: str,
        destination_blob_name: str,
        content_type="application/json",
        encoding: str = "utf-8",
    ) -> str:
        """Uploads a blob to a bucket using stringIO.

        Args:
        bucket_name: The name of the GCS bucket to upload to.
        file_obj: The string data to upload.
        destination_blob_name: The name of the file in gcs.
        content_type: the type of file defaults to 'application/json'
        encoding: The encoding of the string data (defaults to 'utf-8').

         Returns : GCS file URI
        """
        try:
            # Create an in-memory StringIO object from the string data
            data_stream = StringIO(file_obj)
            data_stream.seek(0)
            # Encode the data to bytes using the specified encoding
            data_bytes = data_stream.getvalue().encode(encoding)

            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            if blob.exists():
                logging.info(
                    f"File {destination_blob_name} already exists in {bucket_name}."
                )
                return f"gs://{bucket_name}/{destination_blob_name}"
            else:
                blob.upload_from_string(data=data_bytes, content_type=content_type)
                logging.info(
                    f"{destination_blob_name} uploaded successfully into bucket - {bucket_name}"
                )
                return f"gs://{bucket_name}/{destination_blob_name}"

        except Exception as e:
            logging.error(f"Could not upload file to bucket: {e}")
            raise e

    def download_file(
        self, bucket_name: str, source_blob_name: str, destination_file_name: PathLike
    ) -> None:
        """Downloads a blob from the bucket.

        Args:
        bucket_name: The name of the GCS bucket
        source_blob_name: The name of the file in the GCS bucket
        destination_file_name: The destination file name/file path
        """

        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            blob.download_to_filename(destination_file_name)
            logging.info(
                f"Downloaded storage object {source_blob_name} from bucket {bucket_name} to local file {destination_file_name}."
            )
        except exceptions.NotFound:
            logging.info(
                f"Could not find blob: {source_blob_name} in gcs bucket: {bucket_name}"
            )
        except Exception as e:
            logging.error(f"Could not download blob: {e}")
            raise e

    def download_file_to_stream(self, bucket_name: str, source_blob_name: str):
        """Downloads a blob to a stream or other file-like object
        Args:
        bucket_name: The name of the GCS bucket

        Returns
        BytesIO: An in-memory bytes buffer with the blob's content.
        """
        try:
            # Initialize an in-memory bytes buffer
            file_obj = BytesIO()
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(source_blob_name)
            blob.download_to_file(file_obj)
            # Seek to the beginning of the stream
            file_obj.seek(0)

            logging.info(f"Downloaded blob {source_blob_name} to file-like object.")
            return file_obj

        except exceptions as e:
            logging.error(f"Error downloading blob:{source_blob_name} - {e}")
            return None

    def delete_file(self, bucket_name: str, blob_name: str) -> None:
        """Delete blob from a bucket.

        Args:
        bucket_name: The bucket name in GCS
        blob_name: THe name of the file in GCS bucket
        """
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            blob.delete()
            logging.info(f"Blob {blob_name} deleted")

        except exceptions.NotFound:
            logging.info(
                f"Could not find blob: {blob_name} in gcs bucket: {bucket_name}"
            )

        except Exception as e:
            logging.error(f"Could not delete blob: {e}")
            raise e
