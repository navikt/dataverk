from abc import abstractmethod
from dataverk.connectors import BaseConnector


class DBBaseConnector(BaseConnector):

    def __init__(self):
        super().__init__()

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
    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists='replace'):
        """ Write Pandas dataframe

        """
        raise NotImplementedError()
