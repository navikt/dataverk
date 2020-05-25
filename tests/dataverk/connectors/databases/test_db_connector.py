from unittest import TestCase

from dataverk.connectors.databases.base import DBBaseConnector


settings = {
    "db_connection_strings": {
        "database": "oracle://user:***@host:1234/db"
    }
}


class TestDbConnector(TestCase):

    def setUp(self):
        self.db_con = DBBaseConnector(settings, "database")
        self.db_con.__enter__()

    def tearDown(self):
        self.db_con.__exit__(None, None, None)

    def test__get_engine(self):
        engine = self.db_con._create_engine()
        self.assertEqual(settings["db_connection_strings"]["database"], str(engine.url))
