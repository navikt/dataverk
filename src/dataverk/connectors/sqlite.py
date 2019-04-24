import pandas as pd
import sqlite3
from dataverk.connectors.abc.db_base import DBBaseConnector


class SQLiteConnector(DBBaseConnector):

    def __init__(self, source=":memory:"):
        """Init"""

        super().__init__()
        self.source = source
        self.cnx = sqlite3.connect(self.source)
        self.cur = self.cnx.cursor

    def get_pandas_df(self, query):
        """Get Pandas dataframe"""

        return pd.read_sql_query(query, self.cnx)
    
    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists='replace'):
        """Persist Pandas dataframe"""
        
        # TODO try catch
        df.to_sql(table, self.cnx, if_exists="replace")
