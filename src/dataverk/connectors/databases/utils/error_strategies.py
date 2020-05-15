from urllib3.util import url
from dataverk_vault import api as vault_api
from dataverk.connectors.databases.base import DBBaseConnector


class OperationalErrorStrategy:

    @staticmethod
    def reset_db_connection(connector: DBBaseConnector):
        connector._engine.dispose()

        try:
            vault_path = connector.settings.vault_path(connector.source)
        except KeyError as err:
            connector.log.error(f"No vault path specified in settings, unable to update credentials: {err}")
        else:
            OperationalErrorStrategy._update_credentials(connector, vault_path)
            connector._engine = connector._create_engine()

    @staticmethod
    def _update_credentials(connector: DBBaseConnector, vault_path: str):
        connector.log.warning(f"Updating db credentials")
        conn_string = connector._connection_string()
        parsed_url = url.parse_url(conn_string)
        new_credentials = vault_api.get_database_creds(vault_path)
        connector.settings["db_connection_strings"][connector.source] = conn_string.replace(parsed_url.auth,
                                                                                            new_credentials)
