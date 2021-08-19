import math

import pandas as pd

from collections.abc import Sequence
from dataverk_publisher import publish_datapackage
from dataverk.abc.base import DataverkBase
from dataverk.context import EnvStore
from dataverk.dataverk_context import DataverkContext
from dataverk.connectors import KafkaConnector, kafka, JSONStatConnector
from dataverk.connectors.databases import db_connector_factory
from dataverk.utils.anonymization import anonymize_replace


class Dataverk(DataverkBase):
    def __init__(
        self, resource_path: str = ".", env_file: str = ".env", auth_token: str = None
    ):
        super().__init__()
        env_store = EnvStore.safe_create(env_file)
        self._context = DataverkContext(env_store, resource_path, auth_token)

    @property
    def context(self):
        return self._context

    def read_sql(
        self,
        source: str,
        sql: str,
        connector: str = "Oracle",
        verbose_output: bool = False,
        *args,
        **kwargs,
    ) -> pd.DataFrame:
        """ Read pandas dataframe from SQL database

        :param source: str: database source reference
        :param sql: str: sql query or file with sql query
        :param connector: str: Database connector (default oracle)
        :param verbose_output: bool: flag for verbose output option
        :return: pd.Dataframe: Dataframe with result
        """
        conn = db_connector_factory.get_db_connector(
            settings_store=self.context.settings, source=source
        )
        query = self._get_sql_query(sql=sql)

        with conn:
            return conn.get_pandas_df(
                query=query, verbose_output=verbose_output, *args, **kwargs
            )

    def execute_sql(
        self,
        source: str,
        sql: str,
        connector: str = "Oracle",
        verbose_output: bool = False,
        *args,
        **kwargs,
    ) -> None:
        """ Execute a sql statement, procedure or function

        :param source: str: database source reference
        :param sql: str: sql query or file with sql query
        :param connector: str: Database connector (default oracle)
        :param verbose_output: bool: flag for verbose output option
        :return: None
        """

        conn = db_connector_factory.get_db_connector(
            settings_store=self.context.settings, source=source
        )
        query = self._get_sql_query(sql=sql)

        with conn:
            conn.execute_sql(
                query=query, verbose_output=verbose_output, *args, **kwargs
            )

    def read_kafka_message_fields(
        self, topics: Sequence, fetch_mode: str = "from_beginning"
    ) -> pd.DataFrame:
        """ Read single kafka message from topic and return list of message fields

        :param topics: Sequence of topics to subscribe to
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset), default last_committed_offset
        :return: list: fields in kafka message
        """
        consumer = kafka.get_kafka_consumer(
            settings=self.context.settings, topics=topics, fetch_mode=fetch_mode
        )
        conn = KafkaConnector(
            consumer=consumer,
            settings=self.context.settings,
            topics=topics,
            fetch_mode=fetch_mode,
        )

        return conn.get_message_fields()

    def read_kafka(
        self,
        topics: Sequence,
        strategy=None,
        fields=None,
        fetch_mode: str = "from_beginning",
        max_mesgs: int = math.inf,
    ) -> pd.DataFrame:
        """ Read kafka topics and return pandas dataframe

        :param strategy: function or lambda passed to the kafka consumer for aggregating data on the fly
        :param fields: requested fields in kafka message
        :param max_mesgs: max number of kafka messages to read
        :param topics: Sequence of topics to subscribe to
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset), default last_committed_offset
        :return: pandas.Dataframe
        """
        consumer = kafka.get_kafka_consumer(
            settings=self.context.settings, topics=topics, fetch_mode=fetch_mode
        )
        conn = KafkaConnector(
            consumer=consumer,
            settings=self.context.settings,
            topics=topics,
            fetch_mode=fetch_mode,
        )

        return conn.get_pandas_df(strategy=strategy, fields=fields, max_mesgs=max_mesgs)

    def read_json_stat(self, url, params=None):
        """ Read json-stat return pandas dataframe

        :param url: str: path to resource
        :param params: optional request parameters
        :return: pandas.Dataframe
        """
        conn = JSONStatConnector()
        return conn.get_pandas_df(url, params=params)

    def anonymize(
        self,
        df,
        eval_column,
        anonymize_columns=None,
        evaluator=lambda x: x < 4,
        replace_by="*",
        anonymize_eval=True,
    ):
        """ Replace values in columns when condition in evaluator is satisfied

        :param df: pandas DataFrame
        :param eval_column: name of column to evaluate for anonymization
        :param anonymize_columns: optional, column name or list of column(s) to anonymize if value in eval_column satisfies the
        condition given in evaluator, default=None
        :param evaluator: lambda function, condition for anonymization based on values in eval_column, default=lambda x: x < 4
        :param replace_by: value or list or dict of values to replace by. List or dict passed must have same length as the number
        of columns to anonymize. Elements in list passed should in addition have the same order as columns in

        a) anonymize_columns + eval_columns if anonymize_eval=True and eval_column is _not_ given in anonymize_columns
        b) anonymize_columns                if anonymize_eval=True and eval_column is given in anonymize_columns
                                            or anonymize_eval=False
        c) eval_column                      if anonymize_eval=True and anonymize_columns is None or anonymize_columns=[]

        The order of values to replace by in dictionary does not matter.

        :param anonymize_eval, bool, whether to anonymize eval_column, default=True

        :return: anonymized pandas DataFrame
        """
        return anonymize_replace(
            df=df,
            eval_column=eval_column,
            anonymize_columns=anonymize_columns,
            evaluator=evaluator,
            replace_by=replace_by,
            anonymize_eval=anonymize_eval,
        )

    def to_sql(
        self,
        df: pd.DataFrame,
        table: str,
        sink: str,
        connector: str = "Oracle",
        if_exists: str = "append",
        *args,
        **kwargs,
    ):
        """ Write records in dataframe to a SQL database table

        :param df: pd.Dataframe: Dataframe to write
        :param table: str: Table in db to write to
        :param sink: str: target database name reference
        :param connector: str: Connector type (default: Oracle)
        :param if_exists: str: Action if table already exists in database (default: replace)
        """
        conn = db_connector_factory.get_db_connector(
            settings_store=self._context.settings, source=sink
        )

        with conn:
            return conn.persist_pandas_df(
                table, df=df, if_exists=if_exists, *args, **kwargs
            )

    @staticmethod
    def publish(datapackage):
        """ Publish datapackage to data catalog

        :param datapackage: Datapackage or deetly.datapackage.Datapackage
        """
        publish_datapackage(datapackage)

    def _get_sql_query(self, sql):
        if self._is_sql_file(sql):
            return self.context.get_sql_query(sql)
        else:
            return sql

    @staticmethod
    def _is_sql_file(source):
        return ".sql" in source
