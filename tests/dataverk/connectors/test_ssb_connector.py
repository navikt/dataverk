import pandas as pd
from nada.connectors import SQLiteConnector
from nada.datasets.ssb import FylkerUtdanning, get_sysselsatte_aku


class TestSSBConnector:
    
    def test_get_fylker_utdanning_pivot(self):
        ds = FylkerUtdanning()
        df = ds.get_pandas_df(pivot=True)
        assert len(df.columns) == 9

    def test_get_fylker_utdanning(self):
        ds = FylkerUtdanning()
        df = ds.get_pandas_df(pivot=False)
        assert len(df.columns) == 4

        
    def test_get_sysselsatte_aku(self):
        df = get_sysselsatte_aku()
        print(len(df.columns))
        assert True