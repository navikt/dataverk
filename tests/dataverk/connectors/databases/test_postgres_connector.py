import unittest

from unittest import mock
from dataverk.exceptions import dataverk_exceptions
from dataverk.connectors import PostgresConnector
from tests.dataverk.connectors.databases.test_resources.mock_sqlalchemy import mock_create_engine_execute

ROLE = "role"

VALID_SETTINGS = {
    "db_connection_strings": {
        "source": "postgres://user:***@host:1234/db"
    },

    "db_vault_path": {
        "source": f"/vault/path/{ROLE}"
    }
}


class TestPostgresConnector(unittest.TestCase):

    def setUp(self):
        self.conn = PostgresConnector(VALID_SETTINGS, "source")

    def test__get_role_name_valid(self):
        role = self.conn._get_role_name()
        self.assertEqual(role, ROLE)

    def test__get_role_name_raises_IncompleteSettingsObject(self):
        conn = PostgresConnector(VALID_SETTINGS, "non-existent-source")
        with self.assertRaises(dataverk_exceptions.IncompleteSettingsObject):
            role = conn._get_role_name()

    @mock.patch("sqlalchemy.engine.create_engine", side_effect=mock_create_engine_execute)
    def test__set_role(self, mock_execute):
        with PostgresConnector(VALID_SETTINGS, "source") as postgres_conn:
            postgres_conn._set_role()

    def test__set_role_raises_IncompleteSettingsObject(self):
        conn = PostgresConnector(VALID_SETTINGS, "non-existent-source")
        with self.assertRaises(dataverk_exceptions.IncompleteSettingsObject):
            conn._set_role()
