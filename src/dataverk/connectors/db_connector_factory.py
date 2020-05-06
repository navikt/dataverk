from collections import Mapping
from dataverk.connectors.abc.db_base import DBBaseConnector
from dataverk.connectors import OracleConnector, SQLiteConnector, PostgresConnector


def get_db_connector(settings_store: Mapping, connector: str, source: str) -> DBBaseConnector:
    """ Factory function returning connector type

    :param settings_store: Mapping object with project specific configurations
    :param connector: str: Connector type
    :param source: str
    :return: database connector
    """
    if connector.lower() == 'oracle':
        return OracleConnector(settings_store=settings_store, source=source)
    elif connector.lower() == 'sqllite':
        return SQLiteConnector(settings_store=settings_store, source=source)
    elif connector.lower() == 'postgres':
        return PostgresConnector(settings_store=settings_store, source=source)
    else:
        raise NotImplementedError(f"{connector} is not a valid connector type. Valid types are oracle, "
                                  f"sqllite and postgres")
