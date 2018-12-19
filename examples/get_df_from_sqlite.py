import pandas as pd
from dataverk.connectors.sqlite import SQLiteConnector

# default is in-memory database
con = SQLiteConnector()

people = [
    {'id': 1, 'name': 'Per'},
    {'id': 2, 'name': 'Hans'},
    {'id': 3, 'name': 'Olav'}
]
df = pd.DataFrame(people)
  
def sql_lite_roundtrip(df):
    con.persist_pandas_df('people', df)
    df = con.get_pandas_df("select * from people")
    return df

res = sql_lite_roundtrip(df)
print(res)

df = con.get_pandas_df("select * from people where id = 3")
