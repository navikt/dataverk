import pandas as pd
from .connectors import OracleConnector, SQLiteConnector, DVKafkaConsumer
from .utils import notebook2script
from dataverk.context import singleton_settings_store_factory
from pathlib import Path
from collections import Sequence


def write_notebook():
    notebook2script()


def read_sql(source, sql, connector='Oracle'):
    """
    Read pandas dataframe from SQL database 
    """
    settings_store = singleton_settings_store_factory()

    if connector == 'Oracle':
        conn = OracleConnector(source=source, settings=settings_store)

        if _is_sql_file(sql):
            query = _read_sql_file(sql_file=sql)
            return conn.get_pandas_df(query)
        else:
            return conn.get_pandas_df(sql) 

    if connector == 'SQLite':
        conn = SQLiteConnector(source=source)

        if _is_sql_file(sql):
            query = _read_sql_file(sql_file=sql)
            return conn.get_pandas_df(query)
        else:     
            return conn.get_pandas_df(sql)         


def read_kafka(topics: Sequence, fetch_mode: str="last_commited_offset") -> pd.DataFrame:
    """ Read kafka topics and return pandas dataframe

    :param topics: Sequence of topics to subscribe to
    :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset), default last_committed_offset
    :return: pandas.Dataframe
    """
    settings_store = singleton_settings_store_factory()
    consumer = DVKafkaConsumer(settings=settings_store, topics=topics, fetch_mode=fetch_mode)

    return consumer.get_pandas_df()


def to_sql(df, table, sink=None, schema=None, connector='Oracle'):
    """Write records in dataframe to a SQL database table"""
    settings_store = singleton_settings_store_factory()

    if connector == 'Oracle':
        conn = OracleConnector(settings=settings_store, source=sink)
        return conn.persist_pandas_df(table, schema, df, if_exists=if_exists)

    # TODO: handle also not in-memory db
    if connector == 'SQLite':
        conn = SQLiteConnector(source=sink)
        return conn.persist_pandas_df(table, df)


def _is_sql_file(source):
    return '.sql' in source


def _read_sql_file(sql_file):
    path = _current_dir()
    with path.joinpath(sql_file).open('r') as f:
        return f.read()


def _current_dir() -> Path:
    return Path(".").absolute()
