from io import BytesIO
from google.cloud import exceptions
from google.cloud.storage import Client


class GCSStreamConnector:
    """Google Storage connector"""

    def __init__(self, bucket_name: str, storage_client: Client, encrypted=True):
        """Init"""
        super(self.__class__, self).__init__(settings={}, encrypted=encrypted)
        try:
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

            #self.log(f'{self.__class__}: String (format: {fmt}) written to {blob.public_url}')
            return f'{blob.public_url}'

        except Exception as ex:
            print(ex)
            #self.log(f'{self.__class__}: Error writing file {name} to google storage')

    def read(self, blob_name: str):
        """Downloads a blob from the bucket."""
        blob = self.bucket.blob(blob_name)
        #self.log(f'Blob {blob_name} read to string to string')
        string_buffer = BytesIO()
        blob.download_to_file(string_buffer)
        return string_buffer.getvalue()

    @staticmethod
    def _get_bucket(storage_client, bucket_name):
        try:
            bucket = storage_client.get_bucket(bucket_name)
            return bucket
        except exceptions.NotFound:
            raise IOError('GCS bucket not available')
