import json
import pandas as pd
from pathlib import Path
from collections import Mapping, Sequence
from dataverk.connectors import OracleConnector, SQLiteConnector, PostgresConnector, KafkaConnector
from dataverk.context import EnvStore, values_importer
from dataverk.context.replacer import Replacer
from dataverk.utils import file_functions


class DataverkContext:

    def __init__(self, resource_path: str=".", auth_token: str=None):
        self._resource_path = resource_path
        self._settings_store = {}
        self._env_store = EnvStore()
        self._http_headers = self._set_http_headers(auth_token)

    def set_envs_from_file(self, local_env_file: str):
        self._env_store = EnvStore(Path(local_env_file))

    def load_settings(self):
        self._settings_store = json.loads(file_functions.get_package_resource("settings.json", self._resource_path, self._http_headers))

    def load_secrets(self):
        self._try_apply_secrets(self._settings_store, self._env_store)

    def read_sql(self, source, sql, connector='Oracle') -> pd.DataFrame:
        """ Read pandas dataframe from SQL database

        :param source: str: database source
        :param sql: str: sql query or file with sql query
        :param connector: str: Database connector (default oracle)
        :return: pd.Dataframe: Dataframe with result
        """
        conn = self._get_db_connector(settings_store=self._settings_store, connector=connector, source=source)
        query = self._get_sql_query(sql=sql)

        return conn.get_pandas_df(query=query)

    def read_kafka(self, topics: Sequence, fetch_mode: str = "from_beginning") -> pd.DataFrame:
        """ Read kafka topics and return pandas dataframe

        :param topics: Sequence of topics to subscribe to
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset), default last_committed_offset
        :return: pandas.Dataframe
        """
        consumer = KafkaConnector(settings=self._settings_store, topics=topics, fetch_mode=fetch_mode)

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
        conn = self._get_db_connector(settings_store=self._settings_store, connector=connector, source=sink)

        return conn.persist_pandas_df(table, schema=schema, df=df, if_exists=if_exists)

    @staticmethod
    def _set_http_headers(auth_token: str):
        if auth_token is not None:
            return {"Authorization": f"token {auth_token}"}
        else:
            return None

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
            return file_functions.get_package_resource(sql, self._resource_path, self._http_headers)
        else:
            return sql

    @staticmethod
    def _is_sql_file(source):
        return '.sql' in source

    def _try_apply_secrets(self, settings: Mapping, env_store: Mapping):
        return self._apply_secrets(env_store, settings)

    @staticmethod
    def _apply_secrets(env_store, settings):
        importer = values_importer.get_secrets_importer(settings, env_store)
        replacer = Replacer(importer.import_values())
        return replacer.get_filled_mapping(json.dumps(settings), json.loads)
