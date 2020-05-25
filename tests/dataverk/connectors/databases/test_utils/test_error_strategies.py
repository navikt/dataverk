from unittest import TestCase
from dataverk.connectors import PostgresConnector
from dataverk.connectors.databases.utils.error_strategies import OperationalErrorStrategy

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

expected_user_password = "new_user:new_password"
expected_settings = {
    "db_connection_strings": {
        f"{source}": f"postgresql://{expected_user_password}@host:5432/db"
    },
    "db_vault_path": {
        f"{source}": "postgresql/vault/path"
    }
}


class MockVaultApi:

    def get_database_creds(self, vault_path: str):
        return expected_user_password


class TestHandleOperationalErrorStrategy(TestCase):

    def setUp(self):
        self.postgres_conn = PostgresConnector(settings, source)
        self.postgres_conn.__enter__()

    def tearDown(self):
        self.postgres_conn.__exit__(None, None, None)

    def test_handle_operational_error(self):
        error_strategy = OperationalErrorStrategy()
        OperationalErrorStrategy.vault_api = MockVaultApi()
        error_strategy.handle_error(self.postgres_conn)
        self.assertEqual(expected_settings, self.postgres_conn.settings)

    def test_incomplete_settings_object(self):
        self.postgres_conn.settings = incomplete_settings
        error_strategy = OperationalErrorStrategy()

        with self.assertRaises(KeyError):
            error_strategy.handle_error(self.postgres_conn)
