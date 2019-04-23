import requests
import json
from pyjstat import pyjstat
from collections import OrderedDict
import pandas as pd
import ast
from dataverk.connectors import BaseConnector


class JSONStatConnector(BaseConnector):
    """JSONStat based connections
    
    """
    
    def __init__(self):
        super().__init__()
     
    def get_pandas_df(self, url, params=None, table_format='json'):
        """
        Get Pandas dataframe
        """

        self.log(str(url))

        if params == None:
            params = json.dumps(self._full_json(url))
           
        response=requests.post(url,params).content
        response=response.decode('utf-8')
        df = pyjstat.from_json_stat(json.loads(response))[0]
        return df

    def _get_table(self, url, table_format = 'json'):

        if table_format == 'json':
            response = requests.get(url)
            df = pyjstat.from_json_stat(response.json(object_pairs_hook=OrderedDict))[0]
        elif table_format == 'csv':
            df = pd.read_csv(url)
        else:
            print("""table_format param must be either 'json' or 'csv'""")
            df = None
        return df

    def _full_json(self,url):
        variables = self._get_variables(url)
        nvars = len(variables)
        var_list = list(range(nvars))
        
        query_element = {}
        
        for x in var_list:
            query_element[x] ='{{"code": "{code}", "selection": {{"filter": "item", "values": {values} }}}}'.format(
                    code = variables[x]['code'], 
                    values = variables[x]['values']) 
            query_element[x] = query_element[x].replace("\'", '"')
        all_elements = str(list(query_element.values()))
        all_elements = all_elements.replace("\'", "")
        
        query = '{{"query": {all_elements} , "response": {{"format": "json-stat" }}}}'.format(all_elements = all_elements)

        query = ast.literal_eval(query)
        
        return query

    def _get_variables(self,url):
        df = pd.read_json(url)
        variables = [values for values in df.iloc[:,1]]
        return variables

