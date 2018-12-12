from dataverk.connectors.bucket_storage_base import BucketStorageConnector, BucketType
from dataverk.connectors.google_storage import GoogleStorageConnector
from dataverk.connectors.azure_blob_storage import AzureStorageConnector
from dataverk.connectors.s3 import AWSS3Connector
from collections.abc import Mapping


def get_storage_connector(bucket_type: BucketType, bucket_name: str, settings: Mapping, encrypted: bool=True) -> BucketStorageConnector:
    if bucket_type == BucketType.AWS_S3:
        return AWSS3Connector(bucket_name=bucket_name, settings=settings, encrypted=encrypted)
    elif bucket_type == BucketType.GCS:
        return GoogleStorageConnector(bucket_name=bucket_name, settings=settings, encrypted=encrypted)
    elif bucket_type == BucketType.AZURE:
        return AzureStorageConnector(bucket_name=bucket_name, settings=settings, encrypted=encrypted)
