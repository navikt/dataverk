import pandas as pd
from dataverk.connectors.oracle import OracleConnector

def get_dataframe_from_oracle():
    con = OracleConnector()
    # Database: datalab. 
    # Credentials and connection string must be specified in vault 
    # or otherwise (ref. settings.py)
    df = con.get_pandas_df("datalab", "select * from test_table")
    return isinstance(df, pd.DataFrame)

res = get_dataframe_from_oracle()
print(res)

