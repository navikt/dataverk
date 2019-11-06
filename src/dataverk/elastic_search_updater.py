from collections import Mapping
import urllib3
import json
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
            dp = self.datapackage_json
            id = dp["id"]
            title = dp.get('title', 'title missing')
            desc = dp.get('description', 'description missing')
            js = {
                "id": id,
                "versionInfo": dp.get("versionInfo", "0.0.1"),
                "versionNotes": dp.get("versionNotes", []),
                "type": dp.get("type", ""),
                "format": dp.get("format", ""),
                "suggest": title + ' ' + desc,
                "description": desc,
                "title": title,
                "license": dp.get("license", {'name': 'CC BY 4.0', 'url': 'http://creativecommons.org/licenses/by/4.0/deed.no'}),
                "language": dp.get("language", "Norsk"),
                "accrualPeriodicity": dp.get("accrualPeriodicity", ""),
                "temporal": dp.get("temporal", ""),
                "category": dp.get("category", ""),
                "status": dp.get("status", ""),
                "rights": dp.get("rights", []),
                "byteSize": dp.get("byteSize", []),
                "provenance": dp.get("provenance", ""),
                "issued": dp.get("issued", datetime.now().isoformat()),
                "modified": dp.get("modified", datetime.now().isoformat()),
                "distribution": dp.get("distribution", []),
                "keyword": dp.get("keywords", []),
                "term": dp.get("term", []),
                "theme": dp.get("theme", []),
                "accessRights": dp.get("accessRights", ['non-public']),
                "accessRightsComment": dp.get("accessRightsComment", ""),
                "publisher": dp.get("publisher", {'name': 'Arbeids- og velferdsetaten (NAV)', 'publisher_url': 'https://www.nav.no'}),
                "creator": dp.get("creator", {'name': 'NAV kunnskapsavdelingen', 'email': 'statistikk@nav.no'}),
                "contactPoint": dp.get("contactPoint", []),
                "spatial": dp.get("spatial", []),
                "url": f"{dp.get('path', '')}/datapackage.json",
                "source": dp.get("source", []),
                "sample": dp.get("sample", []),
                "repo": dp.get("repo", ""),
                "notebook": dp.get("notebook", ""),
                "code": dp.get("code", ""),
            }

            resources = dp.get('resources', [])

            resource_names = []
            resource_descriptions = []
            for resource in resources:
                if resource.get('name'):
                    resource_names.append(resource['name'])
                if resource.get('description'):
                    resource_descriptions.append(resource['description'])

            js['resource_names'] = resource_names
            js['resource_descriptions'] = resource_descriptions

            res = self._es_index.write(id, js)
            print(res.text)
        except urllib3.exceptions.LocationValueError as err:
            print(f'write to elastic search failed, host_uri could not be resolved')
            raise urllib3.exceptions.LocationValueError(err)





