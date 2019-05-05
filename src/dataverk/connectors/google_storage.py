from collections import Mapping

import google.cloud.storage as storage
from io import BytesIO
from datetime import timedelta
from google.cloud import exceptions
from google.oauth2 import service_account
from dataverk.connectors.abc.bucket_storage_base import BucketStorageConnector


class GoogleStorageConnector(BucketStorageConnector):
    """Google Storage connector"""

    def __init__(self, bucket_name: str, settings: Mapping={}):
        """Init"""

        super().__init__()

        try:
            gcp_project = self._gcp_project_name(settings)
            gcp_credentials = self._gcp_credentials(settings)

            storage_client = storage.Client(project=gcp_project,
                                            credentials=gcp_credentials)

            self.bucket = self._get_bucket(storage_client, bucket_name)

            # Reload fetches the current ACL from Cloud Storage.
            self.bucket.acl.reload()
        except Exception as ex:
            print(ex)

    def write(self, source_string: str, destination_blob_name: str, fmt: str, metadata: dict = {}):
        """Write string to a bucket."""
        try:

            name = f'{destination_blob_name}.{fmt}'
            blob = self.bucket.blob(name)
            """
            try:
                blob.delete()
            except:
                pass
            """

            # blob.content_type = f'application/{fmt}'
            # blob.content_language = 'en-US'
            # blob.content_encoding='utf-8'
            blob.cache_control = 'no-cache'
            blob.content_type = 'text/plain'
            blob.metadata = metadata
            blob.upload_from_string(source_string)
            blob.make_public()

            self.log(f'{self.__class__}: String (format: {fmt}) written to {blob.public_url}')
            return f'{blob.public_url}'

        except Exception as ex:
            print(ex)
            self.log(f'{self.__class__}: Error writing file {name} to google storage')

    def read(self, blob_name: str):
        """Downloads a blob from the bucket."""
        blob = self.bucket.blob(blob_name)
        self.log(f'Blob {blob_name} read to string to string')
        string_buffer = BytesIO()
        blob.download_to_file(string_buffer)
        return string_buffer.getvalue()

    def upload_blob(self, source_file_name: str, destination_blob_name: str):
        """Uploads a file to the bucket."""
        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)
        blob.make_public()  # offentlig tilgjengelig for nedlasting
        self.log(f'{self.__class__}: File {source_file_name} uploded to {destination_blob_name}')

    def delete_blob(self, blob_name: str):
        """Deletes a blob from the bucket."""
        blob = self.bucket.blob(blob_name)
        blob.delete()
        self.log(f'{self.__class__}: Blob{blob_name} in bucket {blob.bucket.name} deleted')

    def download_blob(self, blob_name: str, destination_file_name: str):
        """Downloads a blob from the bucket."""
        blob = self.bucket.blob(blob_name)
        blob.download_to_filename(destination_file_name)
        self.log(f'{self.__class__}: Blob{blob_name} downloaded to {destination_file_name}')

    def get_blob_metadata(self, blob_name: str, format: str = 'markdown'):
        """Prints out a blob's metadata."""
        blob = self.bucket.get_blob(blob_name)

        if format == 'markdown':
            items = []

            items.append(f'### Dataset:\n\n')

            items.append(f'Encrypted: _{self.encrypted}_\n\n')

            items.append(f'### Blob:\n\n')
            items.append(f'Blob: _{blob.name}_\n\n')
            items.append(f'Bucket: _{blob.bucket.name}_\n\n')
            items.append(f'Storage class: _{blob.storage_class}_\n\n')
            items.append(f'ID: _{blob.id}_\n\n')
            items.append(f'Size: _{blob.size}_\n\n')
            items.append(f'Updated: _{blob.updated}_\n\n')
            items.append(f'Generation: _{blob.generation}_\n\n')
            items.append(f'Metageneration: _{blob.metageneration}_\n\n')
            items.append(f'Etag: _{blob.etag}_\n\n')
            items.append(f'Owner: _{blob.owner}_\n\n')
            items.append(f'Component count: _{blob.component_count}_\n\n')
            items.append(f'Crc32c: _{blob.crc32c}_\n\n')
            items.append(f'md5_hash: _{blob.md5_hash}_\n\n')
            items.append(f'Cache-control: _{blob.cache_control}_\n\n')
            items.append(f'Content-type: _{blob.content_type}_\n\n')
            items.append(f'Content-disposition: _{blob.content_disposition}_\n\n')
            items.append(f'Content-encoding: _{blob.content_encoding}_\n\n')
            items.append(f'Content-language: _{blob.content_language}_\n\n')
            # items.append(f'Metadata: _{blob.metadata}_\n\n')

            items.append(f'### Metadata:\n\n')

            if blob.metadata is not None:
                for key, value in blob.metadata.items():
                    items.append(f'{key}: _{value}_')
                    items.append(f'\n\n')

            return ''.join(items)

        if format == 'object':
            return blob

    def _gcp_project_name(self, settings):
        try:
            return settings["bucket_storage_connections"]["google_cloud"]["client"]
        except KeyError:
            return None

    def _gcp_credentials(self, settings):
        try:
            return settings["bucket_storage_connections"]["google_cloud"]["credentials"]
        except KeyError:
            return None

    def _make_blob_public(self, blob_name):
        """Makes a blob publicly accessible."""
        blob = self.bucket.blob(blob_name)
        blob.make_public()
        self.log(f'{self.__class__}: Making blob{blob_name} public')
        return blob.public_url

    def _get_signed_url(self, blob_name, ttl=1):
        """Generates a signed URL for a blob.
        Note that this method requires a service account key file.
        """
        blob = self.bucket.blob(blob_name)

        url = blob.generate_signed_url(
            # This URL is valid for 1 hour
            expiration=timedelta(hours=ttl),
            # Allow GET requests using this URL.
            method='GET')

        return url

    def _get_bucket(self, storage_client, bucket_name):
        try:
            bucket = storage_client.get_bucket(bucket_name)
            return bucket
        except exceptions.NotFound:
            try:
                bucket = storage_client.bucket(bucket_name)
                bucket.location = "europe-north1"
                bucket.storage_class = "REGIONAL"
                bucket.create()
                self.log(f'{self.__class__}: Bucket {bucket_name} created in Google Cloud Storage')
                return bucket
            except:
                # TODO custom errors?
                raise IOError('GCS bucket not available and could not be created')