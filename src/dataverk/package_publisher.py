import json
import os

from typing import Mapping
from dataverk.abc.base import DataverkBase
from dataverk.connectors.storage.bucket_storage_base import BucketStorageBase
from dataverk.connectors.storage.storage_connector_factory import (
    get_storage_connector,
    StorageType,
)


class PackagePublisher(DataverkBase):
    def __init__(
        self, settings_store: Mapping, env_store: Mapping, dp
    ):
        super().__init__()
        self._settings_store = settings_store
        self._env_store = env_store
        self._bucket = os.getenv("DATAVERK_BUCKET", dp.bucket)
        self._datapackage_metadata = dp.datapackage_metadata
        self._resources = dp.resources

    def publish(self) -> None:
        """ Publishes all resources in datapackage

        :return: None
        """
        bucket_type = self._datapackage_metadata.get("store")
        datapackage_id = self._datapackage_metadata.get("id")
        storage_connector = get_storage_connector(
            storage_type=StorageType(bucket_type),
            bucket_name=self._bucket,
            settings=self._settings_store,
        )

        self.log.info(
            f"Publishing datapackage {self._datapackage_metadata.get('title')} "
            f"to bucket {self._bucket}"
        )

        PackagePublisher._upload_datapackage_metadata(
            storage_connector=storage_connector,
            datapackage_id=datapackage_id,
            datapackage_metadata=self._datapackage_metadata,
        )
        PackagePublisher._upload_datapackage_resources(
            storage_connector=storage_connector,
            datapackage_id=datapackage_id,
            datapackage_metadata=self._datapackage_metadata,
            resources=self._resources,
        )

    @staticmethod
    def _upload_datapackage_metadata(
        storage_connector: BucketStorageBase,
        datapackage_id: str,
        datapackage_metadata: Mapping,
    ) -> None:
        storage_connector.write(
            data=json.dumps(datapackage_metadata),
            destination_blob_name=datapackage_id + "/datapackage",
            metadata=datapackage_metadata,
            fmt="json",
        )

    @staticmethod
    def _upload_datapackage_resources(
        storage_connector: BucketStorageBase,
        datapackage_id: str,
        datapackage_metadata: Mapping,
        resources: dict,
    ) -> None:
        for filename, resource in resources.items():
            storage_connector.write(
                data=resource.get("data"),
                destination_blob_name=f"{datapackage_id}/{filename}",
                metadata=datapackage_metadata,
                fmt=resource.get("format"),
            )
