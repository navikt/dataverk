from dataverk.connectors import OracleConnector, SQLiteConnector
from dataverk.utils import notebook2script
from dataverk.context import singleton_settings_store_factory
from pathlib import Path


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


def to_sql(df, table, if_exists: str='replace', sink=None, schema=None, connector='Oracle'):
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
