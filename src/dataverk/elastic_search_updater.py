from collections import Mapping

import urllib3
from dataverk.connectors.elasticsearch import ElasticsearchConnector
from datetime import datetime


class ElasticSearchUpdater:

    def __init__(self, es_index: ElasticsearchConnector, datapackage_metadata: Mapping):
        self._es_index = es_index
        self.datapackage_json = datapackage_metadata


    def publish(self):
        ''' - Iterates through all bucket storage connections in the settings.json file and publishes the datapackage
            - Updates ES index with metadata for the datapackage

        :return: None
        '''

        try:
            id = self.datapackage_json["id"]
            js = {
                'id': self.datapackage_json["id"],
                'title': self.datapackage_json.get('title', 'missing title'),
                'updated': datetime.now().isoformat(),
                'keywords': self.datapackage_json.get('keywords', []),
                'accessRights': self.datapackage_json.get('accessRights', ''),
                'description': self.datapackage_json.get('description', ''),
                'publisher': self.datapackage_json.get('publisher', ''),
                'author': self.datapackage_json.get('author', ''),
                'package': self.datapackage_json.get('package', ''),
                'geo': self.datapackage_json.get('geo', []),
                'provenance': self.datapackage_json.get('provenance', ''),
                'uri': f'{self.datapackage_json.get("path", "")}/datapackage.json'
            }
            self._es_index.write(id, js)
        except urllib3.exceptions.LocationValueError as err:
            print(f'write to elastic search failed, host_uri could not be resolved')
            raise urllib3.exceptions.LocationValueError(err)





