# -*- coding: utf-8 -*-
# Import statements
# =================
import pandas as pd
from unittest import TestCase
from dataverk.connectors.databases.base import DBBaseConnector


class MethodsReturnValues(TestCase):

    def test_sql_lite_in_memory_roundtrip(self):
        people = [{'id': 1, 'name': 'Per'}]
        df = pd.DataFrame(people)
        settings = {
            "db_connection_strings": {
                "sqlite": "sqlite://"
            }
        }
        with DBBaseConnector(settings_store=settings, source="sqlite") as con:
            con.persist_pandas_df('people', df=df)
            df = con.get_pandas_df("select * from people")
            self.assertTrue(df.iloc[0]['name'] == people[0]["name"])
