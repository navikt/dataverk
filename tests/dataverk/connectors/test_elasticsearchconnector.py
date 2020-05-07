from unittest import TestCase

import requests

from dataverk.connectors.elasticsearch import ElasticsearchConnector


SETTINGS = {
    "index_connections": {
        "elastic_host": "https://my.es.index.com",
        "index": "myindex",
        "token": "token"
    }
}


class Base(TestCase):
    def setUp(self):
        self.es = ElasticsearchConnector(SETTINGS)


class MethodsReturnValues(Base):

    def test_write_invalid(self):
        with self.assertRaises(requests.exceptions.RequestException):
            self.es.write("id", {})
