import boto3
from dataverk.connectors.bucket_storage_base import BucketStorageConnector
from collections.abc import Mapping
from ssl import create_default_context


# AWS S3
class AWSS3Connector(BucketStorageConnector):
    """Amazon S3 Storage compatible connection"""

    # Init
    def __init__(self, bucket_name: str, settings: Mapping, encrypted=True):

        super(self.__class__, self).__init__(settings=settings, encrypted=encrypted)

        ssl_context = create_default_context()

        self.s3 = boto3.resource(
            service_name='s3',
            aws_access_key_id=settings["bucket_storage_connections"]["aws_s3"]["access_key"],
            aws_secret_access_key=settings["bucket_storage_connections"]["aws_s3"]["secret_key"],
            verify=ssl_context.load_default_certs(),
            endpoint_url=settings["bucket_storage_connections"]["aws_s3"]["host"]
        )

        if not self.s3.Bucket(bucket_name) in self.s3.buckets.all():
            self._create_bucket(bucket_name)

        self.bucket = self.s3.Bucket(bucket_name)

    def write(self, source_string: str, destination_blob_name: str, fmt: str="", metadata: dict={}):
        obj = self.s3.Object(self.bucket.name, destination_blob_name)
        obj.put(Body=source_string)

    def read(self, blob_name: str):
        obj = self.s3.Object(self.bucket.name, blob_name)
        self.log(f'{self.__class__}: Object {blob_name} read to string')
        return obj.get()['Body'].read().decode('utf-8')

    def upload_blob(self, source_file_name: str, destination_blob_name: str):
        self.bucket.upload_file(source_file_name, destination_blob_name)
        self.log(f'{self.__class__}: File {source_file_name} uploaded to {destination_blob_name}')

    def delete_blob(self, blob_name: str):
        obj = self.s3.Object(self.bucket.name, blob_name)
        obj.delete()
        self.log(f'{self.__class__}: Object {blob_name} deleted from bucket {self.bucket.name}')

    def download_blob(self, blob_name: str, destination_path: str):
        self.bucket.download_file(blob_name, destination_path)
        self.log(f'{self.__class__}: File {blob_name} downloaded to {destination_path}')

    def get_blob_metadata(self, blob_name: str, format: str='markdown'):
        pass

    def list_bucket_objects(self):
        for obj in self.bucket.objects.all():
            print(obj)

    def _create_bucket(self, bucket_name):
        self.s3.create_bucket(Bucket=bucket_name)
        self.log(f'{self.__class__}: Bucket {bucket_name} created')

