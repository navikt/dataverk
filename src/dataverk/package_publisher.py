import json
from collections import Mapping
from dataverk.connectors.abc.bucket_storage_base import BucketStorageConnector
from dataverk.connectors.bucket_connector_factory import get_storage_connector, BucketType


class PackagePublisher:

    def __init__(self, settings_store: Mapping, env_store: Mapping, datapackage_metadata: Mapping):
        self._settings_store = settings_store
        self._env_store = env_store
        self.datapackage_json = datapackage_metadata

    def _datapackage_key_prefix(self, datapackage_name: str):
        return datapackage_name + '/'

    def _is_publish_set(self, bucket_type: str):
        return self._settings_store["bucket_storage_connections"][bucket_type]["publish"].lower() == "true"

    def publish(self, resources):
        ''' - Iterates through all bucket storage connections in the settings.json file and publishes the datapackage
            - Updates ES index with metadata for the datapackage

        :return: None
        '''

        for bucket_type in self._settings_store["bucket_storage_connections"]:
            if self._is_publish_set(bucket_type=bucket_type):
                self.upload_to_storage_bucket(datapackage_metadata=self.datapackage_json,
                                              conn=get_storage_connector(bucket_type=BucketType(bucket_type),
                                                                         bucket_name=self.datapackage_json.get("bucket_name"),
                                                                         settings=self._settings_store,
                                                                         encrypted=False),
                                              datapackage_key_prefix=self._datapackage_key_prefix(
                                                  self.datapackage_json.get("name")),
                                              resources=resources)

    def upload_to_storage_bucket(self, datapackage_metadata, resources, conn: BucketStorageConnector, datapackage_key_prefix: str) -> None:
        ''' Publish data to bucket storage.

        :param dir_path: str: path to directory where generated resources ar located locally
        :param conn: BucketStorageConnector object: the connection object for chosen bucket storage.
                     If no bucket storage connector is configured (conn=None) no resources shall be published to bucket storage
        :param datapackage_key_prefix: str: prefix for datapackage key
        :return: None
        '''
        if conn is not None:
            conn.write(json.dumps(datapackage_metadata), datapackage_key_prefix + 'datapackage', "json")
            for filename, df in resources.items():
                csv_string = df.to_csv(sep=",", encoding="utf-8")
                conn.write(csv_string, f'{datapackage_key_prefix}resources/{filename}', 'csv')





