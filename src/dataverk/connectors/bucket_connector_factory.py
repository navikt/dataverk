from dataverk.connectors.abc.bucket_storage_base import BucketStorageConnector
from dataverk.connectors.google_storage import GoogleStorageConnector
from dataverk.connectors.azure_blob_storage import AzureStorageConnector
from collections.abc import Mapping
from enum import Enum
from dataverk.connectors.s3 import S3Connector


class BucketType(Enum):
    AWS_S3 = "aws_s3"
    DATAVERK_S3 = "dataverk_s3"
    GCS = "google_cloud"
    AZURE = "azure"
    GITHUB = "github"


def get_storage_connector(bucket_type: BucketType, bucket_name: str, settings: Mapping, encrypted: bool=True) -> BucketStorageConnector:
    if bucket_type == BucketType.DATAVERK_S3:
        return S3Connector(bucket_name=bucket_name, s3_endpoint=settings["bucket_storage_connections"]["dataverk_s3"]["host"])
    elif bucket_type == BucketType.GCS:
        return GoogleStorageConnector(bucket_name=bucket_name, settings=settings)
    elif bucket_type == BucketType.AZURE:
        return AzureStorageConnector(bucket_name=bucket_name, settings=settings, encrypted=encrypted)
    elif bucket_type == BucketType.GITHUB:
        return None
    else:
        raise ValueError(f'Bucket type {bucket_type} is not supported')
