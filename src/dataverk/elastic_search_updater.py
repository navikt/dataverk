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
            title = self.datapackage_json.get('title', 'title missing')
            desc = self.datapackage_json.get('description', 'description missing')
            js = {
                "id": id,
                "type": self.datapackage_json.get("type", ""),
                "suggest": title + ' ' + desc,
                "description": desc,
                "title": title,
                "format": self.datapackage_json.get("format", ""),
                "category": self.datapackage_json.get("category", ""),
                "provenance": self.datapackage_json.get("provenance", ""),
                "master": self.datapackage_json.get("master", ""),
                "purpose": self.datapackage_json.get("purpose", ""),
                "legalbasis": self.datapackage_json.get("legalbasis", ""),
                "pii": self.datapackage_json.get("pii", ""),
                "issued": self.datapackage_json.get("issued", datetime.now().isoformat()),
                "modified": datetime.now().isoformat(),
                "modified_by": self.datapackage_json.get("modified_by", ""),
                "created": self.datapackage_json.get("created", datetime.now().isoformat()),
                "created_by": self.datapackage_json.get("created_by", ""),
                "policy": self.datapackage_json.get("policy", [{"legal_basis": "", "purpose": ""}]),
                "distribution": self.datapackage_json.get("distribution", [{"id": "", "format": "", "url": ""}]),
                "keywords": self.datapackage_json.get("keywords", []),
                "theme": self.datapackage_json.get("theme", [""]),
                "accessRights": self.datapackage_json.get("accessRights", []),
                "publisher": self.datapackage_json.get("publisher", []),
                "spatial": self.datapackage_json.get("spatial", []),
                "geo": self.datapackage_json.get("geo", []),
                "url": f"{self.datapackage_json.get('path', '')}/datapackage.json",
                "repo": self.datapackage_json.get("repo", ""),
                "ispartof": self.datapackage_json.get("ispartof", []),
                "haspart": self.datapackage_json.get("haspart", []),
            }

            self._es_index.write(id, js)
        except urllib3.exceptions.LocationValueError as err:
            print(f'write to elastic search failed, host_uri could not be resolved')
            raise urllib3.exceptions.LocationValueError(err)





