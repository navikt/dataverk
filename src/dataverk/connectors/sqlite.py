import pandas as pd
import sqlite3
from dataverk.connectors.abc.db_base import DBBaseConnector


class SQLiteConnector(DBBaseConnector):

    def __init__(self, source=":memory:"):
        super().__init__()

        self.source = source
        self.cnx = sqlite3.connect(self.source)
        self.cur = self.cnx.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cnx.close()

    def get_pandas_df(self, query, verbose_output=False):
        """Get Pandas dataframe"""

        if verbose_output:
            self.log(f'Query: {query}')

        return pd.read_sql_query(query, self.cnx)
    
    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists='append'):
        """Persist Pandas dataframe"""
        
        # TODO try catch
        df.to_sql(table, self.cnx, if_exists=if_exists)
