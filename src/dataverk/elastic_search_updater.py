from collections import Mapping
import urllib3
import json
from dataverk.connectors.elasticsearch import ElasticsearchConnector
from datetime import datetime



class ElasticSearchUpdater:

    def __init__(self, es_index: ElasticsearchConnector, datapackage_metadata: Mapping):
        self._es_index = es_index
        self.datapackage_json = datapackage_metadata

    def _flatten_json(self,y):
        out = {}

        def flatten(x, name=''):
            if type(x) is dict:
                for a in x:
                    flatten(x[a], name + a + '_')
            else:
                out[name[:-1]] = x

        flatten(y)
        return out


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
                "license": self.datapackage_json.get("license", {'name': 'CC BY 4.0', 'url': 'http://creativecommons.org/licenses/by/4.0/deed.no'}),
                "language": self.datapackage_json.get("language", "Norsk"),
                "periodicity": self.datapackage_json.get("periodicity", "NA"),
                "temporal": self.datapackage_json.get("temporal", ""),
                "category": self.datapackage_json.get("category", ""),
                "provenance": self.datapackage_json.get("provenance", ""),
                "issued": self.datapackage_json.get("issued", datetime.now().isoformat()),
                "modified": self.datapackage_json.get("modified", datetime.now().isoformat()),
                "distribution": self.datapackage_json.get("distribution", [{"id": "", "format": "", "url": ""}]),
                "keywords": self.datapackage_json.get("keywords", []),
                "theme": self.datapackage_json.get("theme", [""]),
                "accessRights": self.datapackage_json.get("accessRights", ['Internal']),
                "publisher": self.datapackage_json.get("publisher", {'name': 'Arbeids- og velferdsetaten (NAV)', 'publisher_url': 'https://www.nav.no'}),
                "contactpoint": self.datapackage_json.get("contactpoint", []),
                "spatial": self.datapackage_json.get("spatial", []),
                "url": f"{self.datapackage_json.get('path', '')}/datapackage.json",
                "source": self.datapackage_json.get("source", ""),
                "repo": self.datapackage_json.get("repo", ""),
                "ispartof": self.datapackage_json.get("ispartof", []),
                "haspart": self.datapackage_json.get("haspart", []),
            }

            resources = self.datapackage_json.get('resources', [])

            resource_names = []
            resource_descriptions = []
            for resource in resources:
                if resource.get('name'):
                    resource_names.append(resource['name'])
                if resource.get('description'):
                    resource_descriptions.append(resource['description'])

            js['resource_names'] = resource_names
            js['resource_descriptions'] = resource_descriptions

            js_flat = self._flatten_json(js)

            res = self._es_index.write(id, js_flat)
            print(res)
        except urllib3.exceptions.LocationValueError as err:
            print(f'write to elastic search failed, host_uri could not be resolved')
            raise urllib3.exceptions.LocationValueError(err)





