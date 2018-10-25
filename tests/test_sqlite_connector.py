import pandas as pd
from dataverk.connectors.sqlite import SQLiteConnector

class TestSQLiteConnector:
    
    def test_sql_lite_in_memory_roundtrip(self):

        people = [{'id': 1, 'name': 'Per'}]
        df = pd.DataFrame(people)
        cnx = SQLiteConnector()
        cnx.persist_pandas_df('people', df)
        df = cnx.get_pandas_df("select * from people")
        assert df.iloc[0]['name'] == 'Per'
