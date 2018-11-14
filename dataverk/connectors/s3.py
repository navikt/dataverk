from dataverk.connectors import BaseConnector
import boto3
from dataverk.utils.settings_store import SettingsStore

# AWS S3
class AWSS3Connector(BaseConnector):
    """Amazon S3 Storage compatible connection"""

    # Init
    def __init__(self, settings: SettingsStore, encrypted=True):

        super(self.__class__, self).__init__(encrypted=encrypted)

        bucket_name = settings["bucket_storage_connections"]["AWS_S3"]["bucket"]

        self.s3 = boto3.resource(
            service_name='s3',
            aws_access_key_id=settings["bucket_storage_connections"]["AWS_S3"]["access_key"],
            aws_secret_access_key=settings["bucket_storage_connections"]["AWS_S3"]["secret_key"],
            verify=False,
            endpoint_url=settings["bucket_storage_connections"]["AWS_S3"]["host"]
        )

        if not self.s3.Bucket(bucket_name) in self.s3.buckets.all():
            self.create_bucket(bucket_name)

        self.bucket = self.s3.Bucket(bucket_name)

    def create_bucket(self, bucket_name):
        self.s3.create_bucket(Bucket=bucket_name)
        self.log(f'{self.__class__}: Bucket {bucket_name} created')

    def delete_bucket(self, bucket_name):
        bucket = self.s3.Bucket(bucket_name)
        bucket.objects.all().delete()
        bucket.delete()
        self.log(f'{self.__class__}: Bucket {bucket_name} deleted')

    def list_bucket_objects(self):
        for obj in self.bucket.objects.all():
            print(obj)

    def put_object(self, object_key, body):
        obj = self.s3.Object(self.bucket.name, object_key)
        obj.put(Body=body)

    def get_object(self, object_key):
        obj = self.s3.Object(self.bucket.name, object_key)
        self.log(f'{self.__class__}: Object {object_key} read to string')
        return obj.get()['Body'].read().decode('utf-8')

    def upload_from_file(self, file_path, object_key):
        self.bucket.upload_file(file_path, object_key)
        self.log(f'{self.__class__}: File {file_path} uploaded to {object_key}')

    def download_to_file(self, object_key, destination_path):
        self.bucket.download_file(object_key, destination_path)
        self.log(f'{self.__class__}: File {object_key} downloaded to {destination_path}')

    def delete_object(self, object_key):
        obj = self.s3.Object(self.bucket.name, object_key)
        obj.delete()
        self.log(f'{self.__class__}: Object {object_key} deleted from bucket {self.bucket.name}')

