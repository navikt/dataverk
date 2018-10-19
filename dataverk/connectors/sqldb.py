import requests
import json
from dataverk.connectors import BaseConnector

# Abstract SQL database Connector
class SQLDbConnector(BaseConnector):
    """DB  based connections"""

    # Init
    def __init__(self):
        """Init"""
        super(SQLDbConnector, self).__init__()
     
    # Get dataframe
    def get_pandas_df(self, query):
        super(SQLDbConnector, self).get_pandas_df(query)
       