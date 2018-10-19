import requests
import json
from pyjstat import pyjstat
#import stats_to_pandas as stp
from dataverk.connectors import BaseConnector


class JSONStatConnector(BaseConnector):
    """JSONStat based connections
    
    """
    
    def __init__(self):
        super(JSONStatConnector, self).__init__()
     
    def get_pandas_df(self, query):
        """Get Pandas dataframe
        
        """

        self.log(str(query))

        url = query['url']
        params = query['params']

        # TODO: return self._get_table(url) if no params

        response=requests.post(url,params).content
        response=response.decode('utf-8')
        df = pyjstat.from_json_stat(json.loads(response))[0]
        return df

    def _get_table(self, table):
        """Get table from jsonstat api url
        
        """
        #df = stp.read_all(table_id = table, language = 'no')
        #return df