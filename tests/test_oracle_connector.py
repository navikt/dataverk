import pandas as pd
from dataverk.connectors.oracle import OracleConnector

class TestOracleConnector:
    
    def test_get_datalab_test_table(self):
        con = OracleConnector()
        # Database: datalab. 
        # Credentials and connection string must be specified in vault 
        # or otherwise (ref. settings.py)
        df = con.get_pandas_df("datalab", "select * from test_table")
        assert isinstance(df, pd.DataFrame)

