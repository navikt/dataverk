import requests
from elasticsearch import Elasticsearch
from dataverk.connectors.abc.base import BaseConnector
from collections.abc import Mapping
from ssl import create_default_context


class ElasticsearchConnector(BaseConnector):
    """Elasticsearch connection"""

    def __init__(self, settings: Mapping, host="elastic_host"):
        super().__init__()

        ssl_context = create_default_context()
        self.host_uri = settings["index_connections"][host]

        if self.host_uri is None:
            raise ValueError(f'Connection settings are not available for the host: {host}')

        self.es = Elasticsearch([self.host_uri], ssl_context=ssl_context)
        self.index = settings["index_connections"]["index"]

    def write(self, dp_id, doc):
        """Add or update document"""
        try:
            res = requests.post(self.host_uri, json=doc,
                                headers={"Content-Type": "application/json"})
            res.raise_for_status()
        except requests.exceptions.HTTPError as err:
            raise requests.exceptions.HTTPError(f"""Unable to update ES index:
                                                    {str(err)}""")
        except requests.exceptions.RequestException as err:
            raise requests.exceptions.RequestException(f"ES index connection error:\n"
                                                       f"{str(err)}")

        self.log.info(f'{self.__class__}: Document {dp_id} of type {self.index} indexed to elastic index: {self.index}.')
        return res
