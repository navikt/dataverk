import io
import json
import gzip
from collections import Mapping

from dataverk.abc.base import DataverkBase
from dataverk.connectors.storage.bucket_storage_base import BucketStorageBase
from dataverk.connectors.storage.storage_connector_factory import (
    get_storage_connector,
    StorageType,
)


class PackagePublisher(DataverkBase):
    def __init__(
        self, settings_store: Mapping, env_store: Mapping, datapackage_metadata: Mapping
    ):
        super().__init__()
        self._settings_store = settings_store
        self._env_store = env_store
        self._datapackage_metadata = datapackage_metadata

    def publish(self, resources):
        """ - Iterates through all bucket storage connections in the settings.json file and publishes the datapackage
            - Updates ES index with metadata for the datapackage

        :return: None
        """
        bucket_type = self._datapackage_metadata.get("store")

        self.log.info(f"Publishing datapackage {self._datapackage_metadata.get('title')} "
                      f"to bucket {self._datapackage_metadata.get('bucket')}")

        self.upload_to_storage_bucket(
            datapackage_metadata=self._datapackage_metadata,
            storage_connector=get_storage_connector(
                storage_type=StorageType(bucket_type),
                bucket_name=self._datapackage_metadata.get("bucket"),
                settings=self._settings_store
            ),
            datapackage_key_prefix=self._datapackage_key_prefix(
                self._datapackage_metadata.get("id")
            ),
            resources=resources
        )

    @staticmethod
    def upload_to_storage_bucket(
        datapackage_metadata,
        resources,
        storage_connector: BucketStorageBase,
        datapackage_key_prefix: str,
    ) -> None:
        """ Publish data to bucket storage.

        :param resources: datapackage data to be published
        :param datapackage_metadata: metadata associated with the datapackage
        :param storage_connector: BucketStorageConnector object: the connection object for chosen bucket storage.
        :param datapackage_key_prefix: str: prefix for datapackage key
        :return: None
        """
        storage_connector.write(
            json.dumps(datapackage_metadata),
            datapackage_key_prefix + "datapackage",
            "json", metadata=datapackage_metadata
        )
        for filename, resource in resources.items():
            storage_connector.write(data=resource.get("data"),
                                    destination_blob_name=f"{datapackage_key_prefix}resources/{filename}",
                                    metadata=datapackage_metadata,
                                    fmt=resource.get("format"))

    @staticmethod
    def _datapackage_key_prefix(base: str):
        return base + "/"
