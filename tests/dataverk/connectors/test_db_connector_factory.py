import copy
import unittest

from dataverk.connectors import OracleConnector, PostgresConnector, SqliteConnector, Db2Connector
from dataverk.connectors.databases import db_connector_factory

SETTINGS = {
    "db_connection_strings": {
        "oracle": "oracle://user:password@host:8080/service",
        "postgres": "postgres://user:password@host:8080/service",
        "sqllite": "sqlite://"
    }
}


class DataverkTest(unittest.TestCase):

    def test__get_db_connector_valid(self):
        connector_types = [("oracle", OracleConnector), ("postgres", PostgresConnector),
                           ("sqllite", SqliteConnector)]

        for connector_type in connector_types:
            with self.subTest(msg="Testing sql connector type factory method", _input=connector_type):
                self.assertIsInstance(
                    db_connector_factory.get_db_connector(settings_store=SETTINGS,
                                                          source=connector_type[0]), connector_type[1])

    def test__get_db_connector_invalid(self):
        settings = copy.deepcopy(SETTINGS)
        settings["db_connection_strings"]["not_implemented_connector"] = "notimplemented://user:pass@host:1234/service"

        with self.assertRaises(NotImplementedError):
            db_connector_factory.get_db_connector(settings_store=settings, source="not_implemented_connector")
