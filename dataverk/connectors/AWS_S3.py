import requests
from dataverk.connectors.bucket_storage_base import BucketStorageConnector
from collections.abc import Mapping


class S3Connector(BucketStorageConnector):

    def __init__(self, bucket_name: str, settings: Mapping, encrypted=True):
        super().__init__(settings=settings, encrypted=encrypted)
        self.s3_api_url = settings["bucket_storage_connections"]["dataverk_s3"]["host"]
        self.bucket_name = bucket_name

    def write(self, source_string: str, destination_blob_name: str, fmt: str=".csv", metadata: dict={}):
        res = requests.put(url=f'{self.s3_api_url}/{self.bucket_name}/{destination_blob_name}',
                           data=source_string,
                           headers={'content-type': 'text/plain'})

        if res.status_code != 200:
            res.raise_for_status()
        self.log(f'{self.__class__}: Object {destination_blob_name} written to bucket {self.bucket_name}')

    def read(self, blob_name: str):
        res = requests.get(url=f'{self.s3_api_url}/{self.bucket_name}/{blob_name}')

        if res.status_code != 200:
            res.raise_for_status()

        self.log(f'{self.__class__}: Object {blob_name} read to string')
        return res.text

    def upload_blob(self, source_file_name: str, destination_blob_name: str):
        with open(source_file_name, 'r') as source_data:
            res = requests.put(url=f'{self.s3_api_url}/{self.bucket_name}/{destination_blob_name}',
                               data=source_data.read().encode("utf-8"),
                               headers={'content-type': 'text/plain; charset=utf-8'})

            if res.status_code != 200:
                res.raise_for_status()

        self.log(f'{self.__class__}: File {source_file_name} uploaded to {destination_blob_name}')

    def delete_blob(self, blob_name: str):
        pass

    def download_blob(self, blob_name: str, destination_file_name: str):
        with open(destination_file_name, 'w') as dest_file:
            res = requests.get(url=f'{self.s3_api_url}/{self.bucket_name}/{blob_name}')

            if res.status_code != 200:
                res.raise_for_status()

            dest_file.write(res.text)

        self.log(f'{self.__class__}: File {blob_name} downloaded to {destination_path}')


    def get_blob_metadata(self, blob_name: str, format: str='markdown'):
        pass