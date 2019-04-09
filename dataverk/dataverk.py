import pandas as pd
from collections import Mapping, Sequence
from dataverk import DataverkContext
from dataverk.connectors import OracleConnector, SQLiteConnector, PostgresConnector, KafkaConnector


class Dataverk:

    def __init__(self, resource_path: str=".", auth_token: str=None):
        self._context = DataverkContext(resource_path, auth_token)

    @property
    def context(self):
        return self._context

    def read_sql(self, source, sql, connector='Oracle') -> pd.DataFrame:
        """ Read pandas dataframe from SQL database

        :param source: str: database source
        :param sql: str: sql query or file with sql query
        :param connector: str: Database connector (default oracle)
        :return: pd.Dataframe: Dataframe with result
        """
        conn = self._get_db_connector(settings_store=self.context.settings, connector=connector, source=source)
        query = self._get_sql_query(sql=sql)

        return conn.get_pandas_df(query=query)

    def read_kafka(self, topics: Sequence, fetch_mode: str = "from_beginning") -> pd.DataFrame:
        """ Read kafka topics and return pandas dataframe

        :param topics: Sequence of topics to subscribe to
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset), default last_committed_offset
        :return: pandas.Dataframe
        """
        consumer = KafkaConnector(settings=self.context.settings, topics=topics, fetch_mode=fetch_mode)

        return consumer.get_pandas_df()

    def to_sql(self, df, table, sink=None, schema=None, connector='Oracle', if_exists: str = 'replace'):
        """ Write records in dataframe to a SQL database table

        :param df: pd.Dataframe: Dataframe to write
        :param table: str: Table in db to write to
        :param sink:
        :param schema:
        :param connector: str: Connector type (default: Oracle)
        :param if_exists: str: Action if table already exists in database (default: replace)
        """
        conn = self._get_db_connector(settings_store=self._context.settings, connector=connector, source=sink)

        return conn.persist_pandas_df(table, schema=schema, df=df, if_exists=if_exists)

    def _get_db_connector(self, settings_store: Mapping, connector: str, source: str):
        """ Factory function returning connector type

        :param settings_store: Mapping object with project specific configurations
        :param connector: str: Connector type
        :param source: str
        :return:
        """
        if connector.lower() == 'oracle':
            return OracleConnector(settings_store=settings_store, source=source)
        elif connector.lower() == 'sqllite':
            return SQLiteConnector(source=source)
        elif connector.lower() == 'postgres':
            return PostgresConnector(settings_store=settings_store, source=source)
        else:
            raise NotImplementedError(f"{connector} is not a valid connector type. Valid types are oracle, "
                                      f"sqllite and postgres")

    def _get_sql_query(self, sql):
        if self._is_sql_file(sql):
            return self.context.get_sql_query(sql)
        else:
            return sql

    @staticmethod
    def _is_sql_file(source):
        return '.sql' in source
