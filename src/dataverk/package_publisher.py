import json
from collections import Mapping
from dataverk.connectors.abc.bucket_storage_base import BucketStorageConnector
from dataverk.connectors.bucket_connector_factory import (
    get_storage_connector,
    BucketType,
)


class PackagePublisher:
    def __init__(
        self, settings_store: Mapping, env_store: Mapping, datapackage_metadata: Mapping
    ):
        self._settings_store = settings_store
        self._env_store = env_store
        self._datapackage_metadata = datapackage_metadata

    def publish(self, resources, csv_sep):
        """ - Iterates through all bucket storage connections in the settings.json file and publishes the datapackage
            - Updates ES index with metadata for the datapackage

        :return: None
        """

        for bucket_type in self._settings_store["bucket_storage_connections"]:
            if self._is_publish_set(bucket_type=bucket_type):
                self.upload_to_storage_bucket(
                    datapackage_metadata=self._datapackage_metadata,
                    conn=get_storage_connector(
                        bucket_type=BucketType(bucket_type),
                        bucket_name=self._datapackage_metadata.get("bucket"),
                        settings=self._settings_store,
                        encrypted=False,
                    ),
                    datapackage_key_prefix=self._datapackage_key_prefix(
                        self._datapackage_metadata.get("name")
                    ),
                    resources=resources,
                    csv_sep=csv_sep
                )

    @staticmethod
    def upload_to_storage_bucket(
        datapackage_metadata,
        resources,
        csv_sep: str,
        conn: BucketStorageConnector,
        datapackage_key_prefix: str,
    ) -> None:
        """ Publish data to bucket storage.

        :param csv_sep: csv separator
        :param resources: datapackage data to be published
        :param datapackage_metadata: metadata assosciated with the datapackage
        :param conn: BucketStorageConnector object: the connection object for chosen bucket storage.
                     If no bucket storage connector is configured (conn=None) no resources shall be published to bucket storage
        :param datapackage_key_prefix: str: prefix for datapackage key
        :return: None
        """
        if conn is not None:
            conn.write(
                json.dumps(datapackage_metadata),
                datapackage_key_prefix + "datapackage",
                "json",
            )
            for filename, df in resources.items():
                csv_string = df.to_csv(sep=csv_sep, encoding="utf-8")
                conn.write(
                    csv_string, f"{datapackage_key_prefix}resources/{filename}", "csv"
                )

    def _is_publish_set(self, bucket_type: str):
        return (
            self._settings_store["bucket_storage_connections"][bucket_type][
                "publish"
            ].lower()
            == "true"
        )

    @staticmethod
    def _datapackage_key_prefix(datapackage_name: str):
        return datapackage_name + "/"
