from collections import Mapping

import urllib3
from dataverk.connectors.elasticsearch import ElasticsearchConnector
from dataverk.connectors.bucket_connector_factory import get_storage_connector, BucketType
from dataverk.utils import publish_data
from pathlib import Path
from datetime import datetime


class PublishDataPackage:

    def __init__(self, settings_store: Mapping, env_store: Mapping,
                 es_index: ElasticsearchConnector, datapackage_metadata: Mapping):
        self._settings_store = settings_store
        self._env_store = env_store
        self._es_index = es_index
        self.datapackage_json = datapackage_metadata

    def _datapackage_key_prefix(self, datapackage_name: str):
        return datapackage_name + '/'

    def _update_es_index(self):
        try:
            id = self.datapackage_json["id"]
            js = {
                'name': self.datapackage_json.get('id', ''),
                'title': self.datapackage_json.get('title', ''),
                'updated': datetime.now(),
                'keywords': self.datapackage_json.get('keywords', []),
                'accessRights': self.datapackage_json.get('accessRights', ''),
                'description': self.datapackage_json.get('description', ''),
                'publisher': self.datapackage_json.get('publisher', ''),
                'geo': self.datapackage_json.get('geo', []),
                'provenance': self.datapackage_json.get('provenance', ''),
                'uri': f'{self.datapackage_json.get("path", "")}/datapackage.json'
            }
            self._es_index.write(id, js)
        except urllib3.exceptions.LocationValueError as err:
            print(f'write to elastic search failed, host_uri could not be resolved')
            raise urllib3.exceptions.LocationValueError(err)

    def _is_publish_set(self, bucket_type: str):
        return self._settings_store["bucket_storage_connections"][bucket_type]["publish"].lower() == "true"

    def publish(self):
        ''' - Iterates through all bucket storage connections in the settings.json file and publishes the datapackage
            - Updates ES index with metadata for the datapackage

        :return: None
        '''
        print(f'Publishing package {self._settings_store["package_name"]}')

        for bucket_type in self._settings_store["bucket_storage_connections"]:
            if self._is_publish_set(bucket_type=bucket_type):
                publish_data.upload_to_storage_bucket(dir_path=str(Path()),
                                                      conn=get_storage_connector(bucket_type=BucketType(bucket_type),
                                                                                 bucket_name=self.datapackage_json.get("bucket_name"),
                                                                                 settings=self._settings_store,
                                                                                 encrypted=False),
                                                      datapackage_key_prefix=self._datapackage_key_prefix(
                                                          self._settings_store["package_name"]))
                self._update_es_index()

        print(f'Package {self._settings_store["package_name"]} successfully published')



