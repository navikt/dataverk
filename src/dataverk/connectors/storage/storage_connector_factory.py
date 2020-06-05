import os
from dataverk.connectors.storage.bucket_storage_base import BucketStorageBase
from dataverk.connectors.storage.file_storage import FileStorageConnector
from dataverk.connectors.storage.google_storage import GoogleStorageConnector
from collections.abc import Mapping
from enum import Enum
from dataverk.connectors.storage.nais import NaisS3Connector


class StorageType(str, Enum):
    NAIS: str = "nais"
    GCS: str = "gcs"
    LOCAL: str = "local"


def get_storage_connector(
    storage_type: StorageType, settings: Mapping, bucket_name: str = None
) -> BucketStorageBase:
    if storage_type == StorageType.NAIS:
        return NaisS3Connector(bucket_name=bucket_name, s3_endpoint=os.environ["DATAVERK_BUCKET_ENDPOINT"])
    elif storage_type == StorageType.GCS:
        return GoogleStorageConnector(bucket_name=bucket_name, settings=settings)
    elif storage_type == StorageType.LOCAL:
        return FileStorageConnector(settings)
    else:
        raise NotImplementedError(
            f"""Bucket type {storage_type} is not supported.
            Supported types are {[name.value for name in StorageType]}"""
        )
