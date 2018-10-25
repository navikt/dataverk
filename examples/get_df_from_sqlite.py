import pandas as pd
from dataverk.connectors.sqlite import SQLiteConnector

# default is in-memory database
cnx = SQLiteConnector()

people = [
    {'id': 1, 'name': 'Per'},
    {'id': 2, 'name': 'Hans'},
    {'id': 3, 'name': 'Olav'}
]
df = pd.DataFrame(people)
  
def sql_lite_roundtrip(df):
    cnx.persist_pandas_df('people', df)
    df = cnx.get_pandas_df("select * from people")
    return df

res = sql_lite_roundtrip(df)
print(res)

def query_sqlite(query):
    df = cnx.get_pandas_df(query)
    return df

res = query_sqlite("select * from people where id = 3")
print(res)
    

