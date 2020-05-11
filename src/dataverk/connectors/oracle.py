import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from collections.abc import Mapping
from dataverk.connectors.abc.db_base import DBBaseConnector


class OracleConnector(DBBaseConnector):

    def __init__(self, settings_store: Mapping, source=None):
        super().__init__(settings_store, source)

    def __enter__(self):
        self._engine = self._create_engine()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._engine.dispose()

    def get_pandas_df(self, query: str, verbose_output: bool=False) -> pd.DataFrame:
        start_time = time.time()
        self.log.info(f"Reading from Oracle database: {self._source}")

        try:
            df = pd.read_sql(query, self._engine)
        except SQLAlchemyError as error:
            self.log.error(str(error.__dict__["orig"]))
            return pd.DataFrame()

        end_time = time.time()
        self.log.info(f"{len(df)} records returned in {end_time - start_time} seconds.")
        if verbose_output:
            self.log(f"Query: {query}")

        return df

    def persist_pandas_df(self, table: str, df: pd.DataFrame, chunksize: int=10000, if_exists: str='append') -> None:
        start_time = time.time()
        self.log.info(f"Persisting {len(df)} records to table: {table} in Oracle database: {self._source}")

        try:
            df.to_sql(table, self._engine, if_exists=if_exists, chunksize=chunksize)
        except SQLAlchemyError as error:
            self.log.error(str(error.__dict__["orig"]))
            return

        end_time = time.time()
        self.log.info(f"Persisted {len(df)} records to table {table} in {end_time - start_time} seconds")

    def _create_engine(self) -> Engine:
        connection_string = self._connection_string()
        return create_engine(connection_string)
    
    def _connection_string(self):
        connection_string = self._settings["db_connection_strings"][self._source]
        return self._format_connection_string(connection_string)

    def _format_connection_string(self, connection_string):
        parsed_url = parse_url(connection_string)
        if "?" not in parsed_url.request_uri:
            return str(parsed_url).replace(parsed_url.request_uri, f"/?service_name={parsed_url.request_uri.split('/')[1]}")
        else:
            return connection_string
