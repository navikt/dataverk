import pandas as pd
from dataverk.connectors.oracle import OracleConnector

def get_dataframe_from_oracle(self):
    con = OracleConnector()
    # Database: datalab. 
    # Credentials and connection string must be specified in vault 
    # or otherwise (ref. settings.py)
    df = con.get_pandas_df("datalab", "select * from test_table")
    isdf = isinstance(df, pd.DataFrame)

