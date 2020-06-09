import dask
import pandas as pd
import dask.dataframe as dd
import cx_Oracle

from urllib3.util import parse_url
from collections.abc import Mapping
from dataverk.connectors.databases.base import DBBaseConnector


class OracleConnector(DBBaseConnector):
    def __init__(self, settings_store: Mapping, source: str):
        super().__init__(settings_store, source)

    def get_dask_df(self, query, where_values) -> dd.DataFrame:
        return self._read_sql_query_dask(query, where_values)

    def _read_sql_query_dask(self, sql, where_values):
        dload = dask.delayed(self._load_df_part)
        parts = [dload(sql, where) for where in where_values]
        return dd.from_delayed(parts)

    def _load_df_part(self, sql, where):
        partial_sql = f"{sql} {where}"
        return pd.read_sql(partial_sql, self._engine)

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
