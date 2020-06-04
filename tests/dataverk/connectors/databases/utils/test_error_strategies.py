from unittest import TestCase, mock

from dataverk.exceptions import dataverk_exceptions

from dataverk.connectors import PostgresConnector
from dataverk.connectors.databases.utils.error_strategies import PostgresOperationalErrorStrategy
from tests.dataverk.connectors.databases.test_resources.mock_vault import expected_user_password, \
    mock_get_database_creds

source = "database"
settings = {
    "db_connection_strings": {
        f"{source}": "postgresql://user:password@host:5432/db"
    },
    "db_vault_path": {
        f"{source}": "postgresql/vault/path"
    }
}

incomplete_settings = {
    "db_connection_strings": {
        f"{source}": "postgresql://user:password@host:5432/db"
    }
}

expected_settings = {
    "db_connection_strings": {
        f"{source}": f"postgresql://{expected_user_password}@host:5432/db"
    },
    "db_vault_path": {
        f"{source}": "postgresql/vault/path"
    }
}


class TestHandleOperationalErrorStrategy(TestCase):

    def setUp(self):
        self.postgres_conn = PostgresConnector(settings, source)
        self.postgres_conn.__enter__()

    def tearDown(self):
        self.postgres_conn.__exit__(None, None, None)

    @mock.patch("dataverk_vault.api.get_database_creds", side_effect=mock_get_database_creds)
    def test_handle_operational_error(self, mock_vault):
        error_strategy = PostgresOperationalErrorStrategy()
        error_strategy.handle_error(self.postgres_conn)
        self.assertEqual(expected_settings, self.postgres_conn.settings)

    def test_incomplete_settings_object(self):
        self.postgres_conn.settings = incomplete_settings
        error_strategy = PostgresOperationalErrorStrategy()

        with self.assertRaises(dataverk_exceptions.IncompleteSettingsObject):
            error_strategy.handle_error(self.postgres_conn)
