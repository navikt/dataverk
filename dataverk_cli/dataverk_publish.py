import json
import urllib3
from dataverk.connectors.elasticsearch import ElasticsearchConnector
from dataverk.connectors.bucket_connector_factory import get_storage_connector, BucketType
from dataverk.utils.resource_discoverer import search_for_files
from dataverk.context.env_store import EnvStore
from dataverk.context import settings
from dataverk.utils import publish_data
from pathlib import Path
from datetime import datetime


class PublishDataPackage:

    def __init__(self, resource_files: dict=None, search_start_path: str='.'):

        if resource_files is not None:
            self.resource_files = resource_files
        else:
            self.resource_files = search_for_files(start_path=Path(search_start_path),
                                                   file_names=('settings.json', '.env'), levels=4)

        try:
            self.env_store = EnvStore(Path(self.resource_files[".env"]))
        except KeyError:
            self.env_store = None

        self.package_settings = settings.settings_store_factory(settings_file_path=Path(self.resource_files["settings.json"]),
                                                                env_store=self.env_store)

        self.datapackage_json = self.read_datapackage_json()

    def read_datapackage_json(self):
        try:
            with self._package_top_dir().joinpath('datapackage.json').open(mode='r') as datapackage_file:
                return json.load(datapackage_file)
        except OSError:
            raise OSError(f'No datapackage.json file found in datapackage')

    def _package_top_dir(self) -> Path:
        return Path(".").absolute()

    def _datapackage_key_prefix(self, datapackage_name: str):
        return datapackage_name + '/'

    def _update_es_index(self):
        try:
            es = ElasticsearchConnector(settings=self.package_settings)
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
            es.write(id, js)
        except urllib3.exceptions.LocationValueError as err:
            print(f'write to elastic search failed, host_uri could not be resolved')
            raise urllib3.exceptions.LocationValueError(err)

    def _is_publish_set(self, bucket_type: str):
        return self.package_settings["bucket_storage_connections"][bucket_type]["publish"].lower() == "true"

    def publish(self):
        ''' - Iterates through all bucket storage connections in the settings.json file and publishes the datapackage
            - Updates ES index with metadata for the datapackage

        :return: None
        '''
        print(f'Publishing package {self.package_settings["package_name"]}')

        for bucket_type in self.package_settings["bucket_storage_connections"]:
            if self._is_publish_set(bucket_type=bucket_type):
                publish_data.upload_to_storage_bucket(dir_path=str(self._package_top_dir()),
                                                      conn=get_storage_connector(bucket_type=BucketType(bucket_type),
                                                                                 bucket_name=self.datapackage_json.get("bucket_name"),
                                                                                 settings=self.package_settings,
                                                                                 encrypted=False),
                                                      datapackage_key_prefix=self._datapackage_key_prefix(
                                                          self.package_settings["package_name"]))
                self._update_es_index()

        print(f'Package {self.package_settings["package_name"]} successfully published')


def publish_datapackage():
    datapackage = PublishDataPackage()
    datapackage.publish()
