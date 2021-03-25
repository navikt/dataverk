import requests
import os

from dataverk.abc.base import DataverkBase
from collections.abc import Mapping

from dataverk.exceptions import dataverk_exceptions
from dataverk.exceptions.dataverk_exceptions import ElasticSearchApiError


class ElasticsearchConnector(DataverkBase):
    """Elasticsearch connector"""

    def __init__(self, settings: Mapping, host="elastic_host"):
        super().__init__()

        self._es_address = self._get_es_address(settings, host)

    def write(self, dp_id: str, doc: Mapping, auth_token: str = None):
        """ Add or update document in es index

        :param dp_id: str: document id
        :param doc: dict: document
        :param auth_token: authorization token for es api
        :return:
        """
        headers = {"Content-Type": "application/json"}
        if auth_token:
            headers["Authorization"] = f"bearer {auth_token}"

        try:
            res = requests.post(self._es_address, json=doc, headers=headers)
            res.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self.log.error(f"Unable to update ES index: {str(err)}""")
            self.log.error(f"{res.text}")
            raise ElasticSearchApiError(str(err), res.text)
        except requests.exceptions.RequestException as err:
            self.log.error(f"ES index connection error: {str(err)}")
            raise requests.exceptions.RequestException(err)
        else:
            self.log.info(f"Document {dp_id} successfully written to elastic index.")
            return res

    def _get_es_address(self, settings: Mapping, host):
        try:
            address = os.environ["DATAVERK_ES_HOST"]
        except KeyError:
            address = self._get_es_address_from_settings(settings, host)

        return address

    def _get_es_address_from_settings(self, settings: Mapping, host):
        try:
            address = settings["index_connections"][host]
        except KeyError as err:
            self.log.warning(f"No ES index specified: {err}")
            raise dataverk_exceptions.IncompleteSettingsObject(f"{err}")
        else:
            return address
