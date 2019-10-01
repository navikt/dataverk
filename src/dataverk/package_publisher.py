import io
import json
import gzip
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

    def publish(self, resources):
        """ - Iterates through all bucket storage connections in the settings.json file and publishes the datapackage
            - Updates ES index with metadata for the datapackage

        :return: None
        """
        bucket_type = self._datapackage_metadata.get("store")

        print(self._datapackage_metadata["store"])

        self.upload_to_storage_bucket(
            datapackage_metadata=self._datapackage_metadata,
            conn=get_storage_connector(
                bucket_type=BucketType(bucket_type),
                bucket_name=self._datapackage_metadata.get("bucket"),
                settings=self._settings_store,
                encrypted=False,
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
        conn: BucketStorageConnector,
        datapackage_key_prefix: str,
    ) -> None:
        """ Publish data to bucket storage.

        :param resources: datapackage data to be published
        :param datapackage_metadata: metadata associated with the datapackage
        :param conn: BucketStorageConnector object: the connection object for chosen bucket storage.
                     If no bucket storage connector is configured (conn=None) no resources shall be published to bucket storage
        :param datapackage_key_prefix: str: prefix for datapackage key
        :return: None
        """
        if conn is not None:
            conn.write(
                json.dumps(datapackage_metadata),
                datapackage_key_prefix + "datapackage",
                "json", datapackage_metadata
            )
            for filename, item in resources.items():
                df = item['df']
                sep = item['dsv_separator']
                fmt = item["format"]
                compressed = item["compressed"]

                data_buff = io.StringIO()
                df.to_csv(data_buff, sep=sep, index=False)

                if compressed:
                    compressed_data = PackagePublisher._compress_content(data_buff)
                    PackagePublisher._upload(conn=conn,
                                             data=compressed_data,
                                             destination_blob_name=f"{datapackage_key_prefix}resources/{filename}",
                                             metadata=datapackage_metadata,
                                             fmt=f"{fmt}.gz")
                else:
                    PackagePublisher._upload(conn=conn,
                                             data=data_buff.getvalue(),
                                             destination_blob_name=f"{datapackage_key_prefix}resources/{filename}",
                                             metadata=datapackage_metadata,
                                             fmt=fmt)

    @staticmethod
    def _compress_content(data_buff):
        gz_buff = io.BytesIO()
        with gzip.GzipFile(fileobj=gz_buff, mode='w') as zipped_f:
            zipped_f.write(bytes(data_buff.getvalue(), encoding="utf-8"))
        return gz_buff.getvalue()

    @staticmethod
    def _upload(conn, data, destination_blob_name, metadata, fmt):
        conn.write(data=data,
                   destination_blob_name=destination_blob_name,
                   metadata=metadata,
                   fmt=fmt)

    @staticmethod
    def _datapackage_key_prefix(base: str):
        return base + "/"
