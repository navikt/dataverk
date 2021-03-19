import urllib3

from collections import Mapping
from dataverk.abc.base import DataverkBase
from dataverk.connectors.elasticsearch import ElasticsearchConnector
from datetime import datetime


class ElasticSearchUpdater(DataverkBase):

    def __init__(self, es_index: ElasticsearchConnector, datapackage_metadata: Mapping):
        super().__init__()
        self._es_index = es_index
        self.datapackage_json = datapackage_metadata

    def _add_resources(self, js):
        resources = self.datapackage_json.get('resources', [])

        js['resource_names'] = []
        js['resource_descriptions'] = []
        for resource in resources:
            if resource.get('name'):
                js['resource_names'].append(resource['name'])
            if resource.get('description'):
                js['resource_descriptions'].append(resource['description'])

    def _create_es_doc(self) -> tuple:

        dp_id = self.datapackage_json["id"]
        title = self.datapackage_json.get('title', 'title missing')
        desc = self.datapackage_json.get('description', 'description missing')
        js = {
            "id": dp_id,
            "versionInfo": self.datapackage_json.get("versionInfo", "0.0.1"),
            "versionNotes": self.datapackage_json.get("versionNotes", []),
            "type": self.datapackage_json.get("type", "datapackage"),
            "format": self.datapackage_json.get("format", "datapackage"),
            "suggest": title + ' ' + desc,
            "description": desc,
            "title": title,
            "license": self.datapackage_json.get("license",
                              {'name': 'CC BY 4.0', 'url': 'http://creativecommons.org/licenses/by/4.0/deed.no'}),
            "language": self.datapackage_json.get("language", "Norsk"),
            "periodicity": self.datapackage_json.get("accrualPeriodicity", ""),
            "temporal": self.datapackage_json.get("temporal", {}),
            "category": self.datapackage_json.get("category", ""),
            "status": self.datapackage_json.get("status", ""),
            "rights": self.datapackage_json.get("rights", ""),
            "byteSize": self.datapackage_json.get("byteSize", []),
            "provenance": self.datapackage_json.get("provenance", ""),
            "issued": self.datapackage_json.get("issued", datetime.now().isoformat()),
            "modified": self.datapackage_json.get("modified", datetime.now().isoformat()),
            "distribution": self.datapackage_json.get("distribution", []),
            "keyword": self.datapackage_json.get("keywords", []),
            "term": self.datapackage_json.get("term", []),
            "theme": self.datapackage_json.get("theme", []),
            "accessRights": self.datapackage_json.get("accessRights", ['non-public']),
            "accessRightsComment": self.datapackage_json.get("accessRightsComment", ""),
            "publisher": self.datapackage_json.get("publisher",
                                {'name': 'Arbeids- og velferdsetaten (NAV)', 'publisher_url': 'https://www.nav.no'}),
            "creator": self.datapackage_json.get("creator", {'name': 'NAV kunnskapsavdelingen', 'email': 'statistikk@nav.no'}),
            "contactPoint": self.datapackage_json.get("contactPoint", []),
            "spatial": self.datapackage_json.get("spatial", []),
            "url": f"{self.datapackage_json.get('path', '')}/datapackage.json",
            "uri": f"{self.datapackage_json.get('uri', '')}",
            "source": self.datapackage_json.get("source", []),
            "sample": self.datapackage_json.get("sample", []),
            "repo": self.datapackage_json.get("repo", ""),
            "notebook": self.datapackage_json.get("notebook", ""),
            "code": self.datapackage_json.get("code", ""),
        }

        self._add_resources(js)

        return dp_id, js

    def publish(self, es_api_token: str):
        """ Updates ES index with metadata for the datapackage

        :return: None
        """
        dp_id, js = self._create_es_doc()
        try:
            self._es_index.write(dp_id, js, auth_token=es_api_token)
        except urllib3.exceptions.LocationValueError as err:
            self.log.error(f"write to elastic search failed, host_uri could not be resolved")
            raise urllib3.exceptions.LocationValueError(err)
