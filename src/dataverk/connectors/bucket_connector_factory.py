from dataverk.connectors.abc.bucket_storage_base import BucketStorageConnector
from dataverk.connectors.google_storage import GoogleStorageConnector
from collections.abc import Mapping
from enum import Enum
from dataverk.connectors.s3 import S3Connector
from os import environ

from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet


class BucketType(str, Enum):
    NAIS: str = "nais"
    GCS: str = "gs"
    LOCAL: str = "local"


def get_storage_connector(bucket_type: BucketType, bucket_name: str, settings: Mapping, encrypted: bool=True) -> BucketStorageConnector:
    if bucket_type == BucketType.NAIS:
        try:
            environ["DATAVERK_BUCKET_ENDPOINT"]
        except KeyError as missing_env:
            raise EnvironmentVariableNotSet(missing_env)
        return S3Connector(bucket_name=bucket_name, s3_endpoint=environ["DATAVERK_BUCKET_ENDPOINT"])
    elif bucket_type == BucketType.GCS:
        return GoogleStorageConnector(bucket_name=bucket_name, settings=settings)
    elif bucket_type == BucketType.LOCAL:
        return None
    else:
        raise ValueError(f'Bucket type {bucket_type} is not supported')
