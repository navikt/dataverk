import json
from enum import Enum
from io import BytesIO
from collections.abc import Mapping
from google.cloud import storage
from google.cloud import exceptions as gcloud_exceptions
from google.oauth2 import service_account
from pathlib import Path
from urllib.parse import unquote
from dataverk.connectors.storage.utils import blob_metadata
from dataverk.exceptions import dataverk_exceptions
from dataverk.connectors.storage.bucket_storage_base import BucketStorageBase


class BlobMetadataFormat(str, Enum):
    MARKDOWN: str = "markdown"
    OBJECT: str = "object"


class GoogleStorageConnector(BucketStorageBase):
    """Google Storage connector"""

    def __init__(self, bucket_name: str, settings: Mapping):
        super().__init__()
        self.bucket = self._get_bucket_connection(bucket_name, settings)

    def write(
        self, data: str, destination_blob_name: str, fmt: str, metadata: dict = None
    ):
        """Write string to a bucket."""
        try:
            name = f"{destination_blob_name}.{fmt}"
            blob = self.bucket.blob(unquote(name))
            blob.upload_from_string(data)
        except gcloud_exceptions.GoogleCloudError as error:
            self.log.error(f"Error writing file {name} to google storage: {error}")
            raise gcloud_exceptions.GoogleCloudError(error)
        else:
            self.log.info(
                f"{destination_blob_name} successfully written to {blob.public_url}"
            )
            return f"{blob.public_url}"

    def read(self, blob_name: str, **kwargs):
        """Downloads a blob from the bucket."""
        blob = self.bucket.blob(blob_name)
        string_buffer = BytesIO()
        try:
            blob.download_to_file(string_buffer)
        except gcloud_exceptions.GoogleCloudError as error:
            self.log.error(f"Unable to read blob {blob_name}: {error}")
            raise gcloud_exceptions.GoogleCloudError(error)
        else:
            self.log.info(f"Blob {blob_name} read to string to string")
            return string_buffer.getvalue().decode()

    def get_blob_metadata(self, blob_name: str, metadata_format: str = "markdown"):
        """Prints out a blob's metadata."""
        blob = self.bucket.get_blob(blob_name)

        if metadata_format == BlobMetadataFormat.MARKDOWN.value:
            return blob_metadata.as_markdown(blob)
        elif metadata_format == BlobMetadataFormat.OBJECT.value:
            return blob_metadata.as_json(blob)
        else:
            raise NotImplementedError(
                f"Blob metadata format {metadata_format} is not supported. "
                f"Supported formats are: {[name.value for name in BlobMetadataFormat]}"
            )

    def _get_bucket_connection(self, bucket_name, settings):
        try:
            storage_client = storage.Client(
                credentials=self._get_gcp_credentials(settings)
            )
            return self._get_bucket(storage_client, bucket_name)
        except gcloud_exceptions.GoogleCloudError as error:
            self.log.error(f"Unable to get bucket {bucket_name}: {error}")
            raise gcloud_exceptions.GoogleCloudError(error)

    def _get_gcp_credentials(self, settings):
        try:
            info = settings["bucket_storage"]["gcs"]["credentials"]
        except KeyError:
            return None
        else:
            info = self.parse_gcp_credentials(info)
            scope = "https://www.googleapis.com/auth/cloud-platform"
            credentials = service_account.Credentials.from_service_account_info(
                info, scopes=(scope,)
            )
            return credentials

    def _get_bucket(self, storage_client, bucket_name):
        try:
            return storage_client.get_bucket(bucket_name)
        except gcloud_exceptions.NotFound:
            raise dataverk_exceptions.StorageBucketDoesNotExist(bucket_name)

    def _set_blob_metadata(self, blob, fmt: str, metadata: Mapping):
        if fmt.lower().endswith("gz"):
            blob.content_encoding = f"gzip"
        blob.cache_control = "no-cache"
        blob.metadata = metadata

    @staticmethod
    def parse_gcp_credentials(info):
        if isinstance(info, str):
            if GoogleStorageConnector.is_json_file(info):
                with open(Path(info)) as file:
                    return json.load(file)
            else:
                return json.loads(info)
        else:
            return info

    @staticmethod
    def is_json_file(info: str):
        return info.endswith(".json")
