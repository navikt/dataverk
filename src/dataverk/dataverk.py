import math
import pandas as pd
import dask.dataframe as dd
from collections.abc import Sequence
from dataverk.context import EnvStore
from dataverk import DataverkContext
from dataverk.connectors import KafkaConnector, kafka, JSONStatConnector
from dataverk.connectors import db_connector_factory
from dataverk.elastic_search_updater import ElasticSearchUpdater
from dataverk.connectors.elasticsearch import ElasticsearchConnector
from dataverk.package_publisher import PackagePublisher


class Dataverk:

    def __init__(self, resource_path: str=".", env_file: str='.env', auth_token: str=None):
        # TODO make context builder
        env_store = EnvStore.safe_create(env_file)
        self._context = DataverkContext(env_store, resource_path, auth_token)

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
        conn = db_connector_factory.get_db_connector(settings_store=self.context.settings, connector=connector, source=source)
        query = self._get_sql_query(sql=sql)

        return conn.get_pandas_df(query=query)

    def read_sql_dask(self, source, sql, where_values, connector='Oracle') -> dd.DataFrame:
        """ Read dask dataframe from SQL database

        :param source: str: database source
        :param sql: str: sql query
        :param where_values: where values for specifying dask partitions
        :param connector: Database connector
        :return: dask dataframe with result
        """
        conn = db_connector_factory.get_db_connector(settings_store=self.context.settings, connector=connector, source=source)

        return conn.get_dask_df(query=sql, where_values=where_values)

    def read_kafka_message_fields(self, topics: Sequence, fetch_mode: str = "from_beginning") -> pd.DataFrame:
        """ Read single kafka message from topic and return list of message fields

        :param topics: Sequence of topics to subscribe to
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset), default last_committed_offset
        :return: list: fields in kafka message
        """
        consumer = kafka.get_kafka_consumer(settings=self.context.settings, topics=topics, fetch_mode=fetch_mode)
        conn = KafkaConnector(consumer=consumer, settings=self.context.settings, topics=topics, fetch_mode=fetch_mode)

        return conn.get_message_fields()

    def read_kafka(self, topics: Sequence, strategy=None, fields=None, fetch_mode: str = "from_beginning", max_mesgs: int=math.inf) -> pd.DataFrame:
        """ Read kafka topics and return pandas dataframe

        :param strategy: function or lambda passed to the kafka consumer for aggregating data on the fly
        :param fields: requested fields in kafka message
        :param max_mesgs: max number of kafka messages to read
        :param topics: Sequence of topics to subscribe to
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset), default last_committed_offset
        :return: pandas.Dataframe
        """
        consumer = kafka.get_kafka_consumer(settings=self.context.settings, topics=topics, fetch_mode=fetch_mode)
        conn = KafkaConnector(consumer=consumer, settings=self.context.settings, topics=topics, fetch_mode=fetch_mode)

        return conn.get_pandas_df(strategy=strategy, fields=fields, max_mesgs=max_mesgs)

    def read_json_stat(self, url, params=None):
        """ Read json-stat return pandas dataframe

        :param url: str: path to resource
        :param params: optional request parameters
        :return: pandas.Dataframe
        """
        conn = JSONStatConnector()
        return conn.get_pandas_df(url, params=params)

    def to_sql(self, df, table, sink=None, schema=None, connector='Oracle', if_exists: str = 'replace'):
        """ Write records in dataframe to a SQL database table

        :param df: pd.Dataframe: Dataframe to write
        :param table: str: Table in db to write to
        :param sink:
        :param schema:
        :param connector: str: Connector type (default: Oracle)
        :param if_exists: str: Action if table already exists in database (default: replace)
        """
        conn = db_connector_factory.get_db_connector(settings_store=self._context.settings, connector=connector, source=sink)

        return conn.persist_pandas_df(table, schema=schema, df=df, if_exists=if_exists)

    def publish(self, datapackage):
        resources = datapackage.resources
        metadata = datapackage.datapackage_metadata

        # Publish resources to buckets
        package_publisher = PackagePublisher(datapackage_metadata=metadata, settings_store=self._context.settings, env_store={})
        package_publisher.publish(resources=resources)

        # Publish metadata to elastic search
        es_conn = ElasticsearchConnector(self._context.settings)
        eu = ElasticSearchUpdater(es_conn, metadata)
        eu.publish()

    def _get_sql_query(self, sql):
        if self._is_sql_file(sql):
            return self.context.get_sql_query(sql)
        else:
            return sql

    @staticmethod
    def _is_sql_file(source):
        return '.sql' in source
