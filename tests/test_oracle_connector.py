import pandas as pd
from dataverk.connectors.OracleConnector import OracleConnector

class TestOracleConnector:
    
    def test_get_datalab_test_table(self):
        con = new OracleConnector()
        # Database: datalab. 
        # Credentials and connection string must be specified in vault 
        # or otherwise (ref. settings.py)
        df = con.get_pandas_df("datalab", "select * from test_table")
        assert isInstance(df, pd.DataFrame)

