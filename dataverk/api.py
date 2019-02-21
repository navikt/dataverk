import pandas as pd
from .connectors import OracleConnector, SQLiteConnector, KafkaConnector, BaseConnector, PostgresConnector
from .utils import notebook2script
from dataverk.context import singleton_settings_store_factory
from pathlib import Path
from collections import Sequence, Mapping


def write_notebook():
    """ Write the jupyter notebook to script
    """
    notebook2script()


def read_sql(source, sql, connector='Oracle'):
    """ Read pandas dataframe from SQL database

    :param source: str: database source
    :param sql: str: sql query or file with sql query
    :param connector: str: Database connector (default oracle)
    :return: pd.Dataframe: Dataframe with result
    """
    settings_store = singleton_settings_store_factory()
    conn = _get_db_connector(settings_store=settings_store, connector=connector, source=source)
    query = _get_sql_query(sql=sql)

    return conn.get_pandas_df(query=query)


def read_kafka(topics: Sequence, fetch_mode: str="from_beginning") -> pd.DataFrame:
    """ Read kafka topics and return pandas dataframe

    :param topics: Sequence of topics to subscribe to
    :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset), default last_committed_offset
    :return: pandas.Dataframe
    """
    settings_store = singleton_settings_store_factory()
    consumer = KafkaConnector(settings=settings_store, topics=topics, fetch_mode=fetch_mode)

    return consumer.get_pandas_df()


def to_sql(df, table, sink=None, schema=None, connector='Oracle', if_exists: str='replace'):
    """ Write records in dataframe to a SQL database table

    :param df: pd.Dataframe: Dataframe to write
    :param table: str: Table in db to write to
    :param sink:
    :param schema:
    :param connector: str: Connector type (default: Oracle)
    :param if_exists: str: Action if table already exists in database (default: replace)
    """
    settings_store = singleton_settings_store_factory()
    conn = _get_db_connector(settings_store=settings_store, connector=connector, source=sink)

    return conn.persist_pandas_df(table, schema=schema, df=df, if_exists=if_exists)


def _get_db_connector(settings_store: Mapping, connector: str, source: str) -> BaseConnector:
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


def _get_sql_query(sql):
    if _is_sql_file(sql):
        return _read_sql_file(sql_file=sql)
    else:
        return sql


def _is_sql_file(source):
    return '.sql' in source


def _read_sql_file(sql_file):
    path = _current_dir()
    with path.joinpath(sql_file).open('r') as f:
        return f.read()


def _current_dir() -> Path:
    return Path(".").absolute()
