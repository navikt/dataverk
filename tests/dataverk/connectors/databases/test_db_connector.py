import unittest
import pandas as pd

from unittest import mock

from sqlalchemy.exc import SQLAlchemyError

from dataverk.connectors.databases.base import DBBaseConnector
from tests.dataverk.connectors.databases.test_resources.common import PANDAS_DATAFRAME
from tests.dataverk.connectors.databases.test_resources.mock_pandas import MockPandas

settings = {
    "db_connection_strings": {
        "database": "oracle://user:***@host:1234/db"
    }
}


class TestDbConnector(unittest.TestCase):

    def setUp(self):
        self.db_con = DBBaseConnector(settings, "database")
        self.db_con.__enter__()

    def tearDown(self):
        self.db_con.__exit__(None, None, None)

    def test__get_engine(self):
        engine = self.db_con._create_engine()
        self.assertEqual(settings["db_connection_strings"]["database"], str(engine.url))

    @mock.patch("pandas.read_sql", side_effect=MockPandas.read_sql)
    def test_get_pandas_dataframe_valid(self, mock_read_sql):
        df = self.db_con.get_pandas_df("SELECT * FROM mytable")
        self.assertTrue(df.equals(PANDAS_DATAFRAME))

    @mock.patch("pandas.read_sql", side_effect=MockPandas.read_sql)
    def test_get_pandas_dataframe_raise_sqlalchemy_error(self, mock_read_sql):
        with self.assertRaises(SQLAlchemyError):
            df = self.db_con.get_pandas_df("SELECT * FROM mytable", raise_sqlalchemy_error=True)

    @mock.patch("pandas.DataFrame.to_sql", side_effect=MockPandas.to_sql)
    def test_pandas_dataframe_to_sql_valid(self, mock_to_sql):
        self.db_con.persist_pandas_df("mytable", df=pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}))

    @mock.patch("pandas.DataFrame.to_sql", side_effect=MockPandas.to_sql)
    def test_pandas_dataframe_to_sql_raise_sqlalchemy_error(self, mock_to_sql):
        with self.assertRaises(SQLAlchemyError):
            self.db_con.persist_pandas_df("mytable", df=pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}),
                                          raise_sqlalchemy_error=True)
