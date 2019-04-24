import unittest

from dataverk.connectors import OracleConnector, SQLiteConnector, PostgresConnector, db_connector_factory

SETTINGS = {
    "db_connection_strings": {
        "oracle": "oracle://user:password@host:8080/service",
        "postgres": "postgres://user:password@host:8080/service"
    }
}


class DataverkTest(unittest.TestCase):

    def test__get_db_connector_valid(self):
        connector_types = [("Oracle", "oracle", OracleConnector), ("Postgres", "postgres", PostgresConnector),
                           ("Sqllite", ":memory:", SQLiteConnector)]

        for connector_type in connector_types:
            with self.subTest(msg="Testing sql connector type factory method", _input=connector_type):
                self.assertIsInstance(db_connector_factory.get_db_connector(settings_store=SETTINGS, connector=connector_type[0],
                                                                            source=connector_type[1]), connector_type[2])

    def test__get_db_connector_invalid(self):
        invalid_connector_type = "InvalidSqlConnector"

        with self.assertRaises(NotImplementedError):
            db_connector_factory.get_db_connector(settings_store=SETTINGS, connector=invalid_connector_type, source="connection")
