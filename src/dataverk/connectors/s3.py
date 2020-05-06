import requests
from dataverk.connectors.abc.base import BaseConnector


class S3Connector(BaseConnector):

    def __init__(self, bucket_name: str, s3_endpoint: str):
        super().__init__()
        self.s3_api_url = s3_endpoint
        self.bucket_name = bucket_name

    def write(self, data, destination_blob_name: str, fmt: str="csv"):
        res = requests.put(url=f"{self.s3_api_url}/{self.bucket_name}/{destination_blob_name}.{fmt}",
                           data=data)
        res.raise_for_status()
        self.log.info(f"Object {destination_blob_name} written to bucket {self.bucket_name}")

    def read(self, blob_name: str):
        res = requests.get(url=f"{self.s3_api_url}/{self.bucket_name}/{blob_name}")

        res.raise_for_status()
        self.log.info(f"Object {blob_name} read to string")
        return res.text
