import pandas as pd
import sqlite3
from dataverk.connectors import SQLDbConnector

# SQLite connector
class SQLiteConnector(SQLDbConnector):
    """Common sqlite3 connection methods"""

    # Init
    def __init__(self, source=":memory:"):
        """Init"""
        super(SQLiteConnector, self).__init__()
        self.source = source
        self.cnx = sqlite3.connect(self.source)
        self.cur = self.cnx.cursor

    # Get dataframe
    def get_pandas_df(self, query):
        """Get Pandas dataframe"""
        super(SQLiteConnector, self).get_pandas_df(query)
        return pd.read_sql_query(query, self.cnx)
    
    # Persist dataframe
    def persist_pandas_df(self, table, df):
        """Persist Pandas dataframe"""
        # TODO try catch
        df.to_sql(table, self.cnx, if_exists="replace")
        return