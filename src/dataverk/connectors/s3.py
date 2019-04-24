import requests
from dataverk.connectors import BaseConnector


class S3Connector(BaseConnector):

    def __init__(self, bucket_name: str, s3_endpoint: str):
        super().__init__()
        self.s3_api_url = s3_endpoint
        self.bucket_name = bucket_name

    def write(self, source_string: str, destination_blob_name: str, fmt: str="csv", metadata: dict={}):
        res = requests.put(url=f'{self.s3_api_url}/{self.bucket_name}/{destination_blob_name}.{fmt}',
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
