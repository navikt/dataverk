from unittest import TestCase

import requests
import os


from dataverk.connectors.elasticsearch import ElasticsearchConnector
from dataverk.exceptions import dataverk_exceptions

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

        address = es._es_address
        actual_address = INDEX

        self.assertEqual(actual_address, address)

    def test__get_address_no_settings(self):
        os.environ[ENVIRON_KEY] = INDEX
        es = ElasticsearchConnector({}, HOST)

        address = es._es_address
        actual_address = INDEX

        self.assertEqual(actual_address, address)
        del os.environ[ENVIRON_KEY]

    def test__get_address_no_settings_no_env(self):
        with self.assertRaises(dataverk_exceptions.IncompleteSettingsObject):
            es = ElasticsearchConnector({})


class MethodsReturnValues(Base):

    def test_write_invalid(self):
        es = ElasticsearchConnector(SETTINGS, HOST)
        with self.assertRaises(requests.exceptions.RequestException):
            es.write("id", {})




