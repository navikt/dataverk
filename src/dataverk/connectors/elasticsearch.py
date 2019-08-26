import requests
import json
from elasticsearch import Elasticsearch
from dataverk.connectors import BaseConnector
from collections.abc import Mapping
from ssl import create_default_context


class ElasticsearchConnector(BaseConnector):
    """Elasticsearch connection"""

    def __init__(self, settings: Mapping, host="elastic_host"):
        super().__init__()

        ssl_context = create_default_context()
        self.host_uri = settings["index_connections"][host]
        self.es_token = settings["index_connections"]["token"]

        if self.host_uri is None:
            raise ValueError(f'Connection settings are not available for the host: {host}')

        self.es = Elasticsearch([self.host_uri], ssl_context=ssl_context)
        self.index = settings["index_connections"]["index"]

    def write(self, dp_id, doc):
        """Add or update document"""
        try:
            res = requests.post(self.host_uri, json=doc, params={"token": self.es_token},
                                headers={"Content-Type": "application/json"})
            res.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise requests.exceptions.HTTPError(f"Unable to update ES index:\n"
                                                f"{str(err)}")
        except requests.exceptions.RequestException as err:
            raise requests.exceptions.RequestException(f"ES index connection error:\n"
                                                       f"{str(err)}")

        self.log(f'{self.__class__}: Document {dp_id} of type {doc} indexed to elastic index: {self.index}.')
        return res

    def get(self, id):
        """Retrieve document by id from elastic index"""

        try:
            self.log(f'Get document {id} from, elastic {self.index}')
            doc = self.es.get(index=self.index, id=id)
            return doc
        except:
            self.log(f'{self.__class__}: Unable to retrieve document: {id} by id from elastic index: {self.index}.')

    def search(self, query):
        """Search elastic index"""

        try:
            self.log(f'Search elastic {self.index} with query {query}')
            hits = self.es.search(index=self.index, body=query)
            return hits
        except:
            self.log(
                f'{self.__class__}: Unable to retrieve document with query: {query} from elastic index: {self.index}.')
