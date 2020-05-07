import requests

from dataverk.connectors.abc.base import DataverkBase
from collections.abc import Mapping


class ElasticsearchConnector(DataverkBase):
    """Elasticsearch connector"""

    def __init__(self, settings: Mapping, host="elastic_host"):
        super().__init__()

        self._es_address = self._es_address(settings, host)
        self.index = settings["index_connections"]["index"]

    def write(self, dp_id: str, doc: Mapping):
        """ Add or update document in es index

        :param dp_id: str: document id
        :param doc: dict: document
        :return:
        """
        try:
            res = requests.post(self._es_address, json=doc, headers={"Content-Type": "application/json"})
            res.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.error(f"Unable to update ES index: {str(err)}""")
            raise requests.exceptions.HTTPError(err)
        except requests.exceptions.RequestException as err:
            self.log.error(f"ES index connection error: {str(err)}")
            raise requests.exceptions.RequestException(err)
        else:
            self.log.info(f"Document {dp_id} successfully written to elastic index {self.index}.")
            return res

    def _es_address(self, settings: Mapping, host):
        try:
            address = settings["index_connections"][host]
        except KeyError as err:
            self.log.error(f"No ES index specified: {err}")
            raise ValueError(f'Connection settings are not available for the host: {host}')
        else:
            return address
