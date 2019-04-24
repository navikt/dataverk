from abc import abstractmethod
from dataverk.connectors import BaseConnector


class DBBaseConnector(BaseConnector):

    def __init__(self):
        super().__init__()

    @abstractmethod
    def get_pandas_df(self, query):
        """Get Pandas dataframe

        """
        raise NotImplementedError()

    @abstractmethod
    def persist_pandas_df(self, table, schema=None, df=None, chunksize=10000, if_exists='replace'):
        """ Write Pandas dataframe

        """
        raise NotImplementedError()
