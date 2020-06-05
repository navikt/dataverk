from urllib3.util import parse_url
from collections.abc import Mapping
from dataverk.connectors.databases.base import DBBaseConnector


class OracleConnector(DBBaseConnector):
    def __init__(self, settings_store: Mapping, source: str):
        super().__init__(settings_store, source)

    def _connection_string(self):
        connection_string = self.settings["db_connection_strings"][self.source]
        return OracleConnector._format_connection_string(connection_string)

    @staticmethod
    def _format_connection_string(connection_string):
        parsed_url = parse_url(connection_string)
        if "?" not in parsed_url.request_uri:
            return str(parsed_url).replace(
                parsed_url.request_uri,
                f"/?service_name={parsed_url.request_uri.split('/')[1]}",
            )
        else:
            return connection_string
