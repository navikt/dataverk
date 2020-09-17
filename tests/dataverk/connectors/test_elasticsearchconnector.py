from unittest import TestCase

import requests
import os


from dataverk.connectors.elasticsearch import ElasticsearchConnector
from dataverk.exceptions.dataverk_exceptions import IncompleteSettingsObject

INDEX = "https://my.es.index.com"

HOST = "elastic_host"

SETTINGS = {
    "index_connections": {
        "elastic_host": INDEX,
        "index": "myindex",
        "token": "token"
    }
}

ENVIRON_KEY = "DATAVERK_ES_HOST"


class Base(TestCase):

    def test__get_address_settings(self):
        es = ElasticsearchConnector(SETTINGS, HOST)
        address = es._get_es_address(SETTINGS, HOST)
        actual_address = INDEX
        self.assertEqual(actual_address, address)

    def test__get_address_no_settings(self):
        os.environ[ENVIRON_KEY] = INDEX
        es = ElasticsearchConnector({}, HOST)
        address = es._get_es_address({}, HOST)
        actual_address = INDEX
        self.assertEqual(actual_address, address)
        os.environ.pop(ENVIRON_KEY)

    def test__get_address_no_setting_env(self):
        es = ElasticsearchConnector({}, HOST)
        with self.assertRaises(IncompleteSettingsObject):
            address = es._get_es_address({}, HOST)


class MethodsReturnValues(Base):

    def test_write_invalid(self):
        es = ElasticsearchConnector(SETTINGS, HOST)
        with self.assertRaises(requests.exceptions.RequestException):
            es.write("id", {})




