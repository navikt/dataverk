from collections import Mapping
from uuid import uuid4

import urllib3
from dataverk.connectors.elasticsearch import ElasticsearchConnector
from datetime import datetime


class ElasticSearchUpdater:

    def __init__(self, es_index: ElasticsearchConnector, datapackage_metadata: Mapping):
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

    def publish(self):
        ''' - Iterates through all bucket storage connections in the settings.json file and publishes the datapackage
            - Updates ES index with metadata for the datapackage

        :return: None
        '''
        self._update_es_index()

    def _package_id(self):
        # TODO implement with file
        # self._package_id = str(uuid4())
        pass



