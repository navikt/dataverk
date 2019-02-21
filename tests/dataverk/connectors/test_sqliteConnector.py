# -*- coding: utf-8 -*-
# Import statements
# =================
from unittest import TestCase
import pandas as pd
from dataverk.connectors import SQLiteConnector


class Base(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

class MethodsReturnValues(Base):
    def test_sql_lite_in_memory_roundtrip(self):
        people = [{'id': 1, 'name': 'Per'}]
        df = pd.DataFrame(people)
        cnx = SQLiteConnector()
        cnx.persist_pandas_df('people', df)
        df = cnx.get_pandas_df("select * from people")
        self.assertTrue(df.iloc[0]['name'] == 'Per')