import sqlite3
import pandas as pd
from typing import Mapping
from dataverk.connectors.abc.db_base import DBBaseConnector


class SQLiteConnector(DBBaseConnector):

    def __init__(self, settings_store: Mapping, source: str):
        super().__init__(settings_store, source)

    def __enter__(self):
        self._cnx = sqlite3.connect(self._source)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cnx.close()

    def get_pandas_df(self, query, verbose_output=False):
        """Get Pandas dataframe"""

        if verbose_output:
            self.log(f'Query: {query}')

        return pd.read_sql_query(query, self._cnx)
    
    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists='append'):
        """Persist Pandas dataframe"""
        
        # TODO try catch
        df.to_sql(table, self._cnx, if_exists=if_exists)
