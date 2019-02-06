import requests
import json
from dataverk.connectors import BaseConnector

class SQLDbConnector(BaseConnector):
    """SQL Database connector"""

    def __init__(self):
        """Init"""
        super(SQLDbConnector, self).__init__()
     
    def get_pandas_df(self, query):
        super(SQLDbConnector, self).get_pandas_df(query)
       