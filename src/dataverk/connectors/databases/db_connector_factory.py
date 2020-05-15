from collections import Mapping
from enum import Enum
from urllib3.util import parse_url
from dataverk.connectors.databases.base import DBBaseConnector
from dataverk.connectors import OracleConnector, PostgresConnector, Db2Connector, SqliteConnector


class DbType(Enum):
    ORACLE = "oracle"
    POSTGRES = "postgres"
    DB2 = "ibm_db"
    SQLITE = "sqlite"


def get_db_connector(settings_store: Mapping, source: str) -> DBBaseConnector:
    """ Factory function returning connector type

    :param settings_store: Mapping object with project specific configurations
    :param source: str
    :return: database connector
    """
    try:
        connection_string = parse_url(settings_store["db_connection_strings"][source])
    except KeyError:
        raise ValueError(f'Database connection string not found in settings file. '
                         f'Unable to establish connection to database: {source}')

    if DbType.ORACLE.value in connection_string.scheme.lower():
        return OracleConnector(settings_store=settings_store, source=source)
    elif DbType.POSTGRES.value in connection_string.scheme.lower():
        return PostgresConnector(settings_store=settings_store, source=source)
    elif DbType.DB2.value in connection_string.scheme.lower():
        return Db2Connector(settings_store=settings_store, source=source)
    elif DbType.SQLITE.value in connection_string.scheme.lower():
        return SqliteConnector(settings_store=settings_store, source=source)
    else:
        raise NotImplementedError()
