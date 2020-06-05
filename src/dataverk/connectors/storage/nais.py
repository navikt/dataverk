import requests
from dataverk.connectors.storage.bucket_storage_base import BucketStorageBase


class NaisS3Connector(BucketStorageBase):
    """
    Connector for NAIS S3 API
    """

    def __init__(self, bucket_name: str, s3_endpoint: str):
        super().__init__()
        self._bucket_name = bucket_name
        self._s3_api_url = s3_endpoint

    def write(self, data, destination_blob_name: str, fmt: str = "csv", **kwargs):
        try:
            res = requests.put(
                url=f"{self._s3_api_url}/{self._bucket_name}/{destination_blob_name}.{fmt}",
                data=data,
            )
            res.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.error(
                f"Unable to write object {destination_blob_name} to bucket {self._bucket_name}: {str(err)}"
            )
            raise requests.exceptions.HTTPError(err)
        except requests.exceptions.RequestException as err:
            self.log.error(f"Connection error {self._s3_api_url}: {str(err)}")
            raise requests.exceptions.RequestException(err)
        else:
            self.log.info(
                f"Object {destination_blob_name} written to bucket {self._bucket_name}"
            )

    def read(self, blob_name: str, **kwargs):
        try:
            res = requests.get(
                url=f"{self._s3_api_url}/{self._bucket_name}/{blob_name}"
            )
            res.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.error(
                f"Unable to read object {blob_name} from bucket {self._bucket_name}: {str(err)}"
            )
            raise requests.exceptions.HTTPError(err)
        except requests.exceptions.RequestException as err:
            self.log.error(f"Connection error {self._s3_api_url}: {str(err)}")
            raise requests.exceptions.RequestException(err)
        else:
            self.log.info(
                f"Object {blob_name} successfully read from bucket {self._bucket_name}"
            )
            return res.text
