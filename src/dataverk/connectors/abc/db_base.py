from abc import abstractmethod
from typing import Mapping

from dataverk.connectors.abc.base import BaseConnector


class DBBaseConnector(BaseConnector):

    def __init__(self, settings_store: Mapping, source: str):
        super().__init__()
        self._settings = settings_store
        self._source = source

        if self._source not in self._settings["db_connection_strings"]:
            raise ValueError(f'Database connection string not found in settings file. '
                             f'Unable to establish connection to database: {self._source}')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    @abstractmethod
    def get_pandas_df(self, query, verbose_output):
        """Get Pandas dataframe

        """
        raise NotImplementedError()

    @abstractmethod
    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists='append'):
        """ Write Pandas dataframe

        """
        raise NotImplementedError()

    def _connection_string(self):
        return self._settings["db_connection_strings"][self._source]
