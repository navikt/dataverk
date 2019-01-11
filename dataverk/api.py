import pandas as pd
import numpy as np
import os
import json
import datetime
import errno
import uuid

from .connectors import OracleConnector, ElasticsearchConnector, SQLiteConnector
from .utils import notebook2script, publish_data
from .datapackage import Datapackage
from dataverk.context import singleton_settings_store_factory
from pathlib import Path


def Datapackage():
    return Datapackage

def write_notebook():
    notebook2script()


def _current_dir() -> Path:
    return Path(".").absolute()

def is_sql_file(source):
    if '.sql' in source:
        return True
    return False

def read_sql(source, sql, connector='Oracle'):
    """
    Read pandas dataframe from SQL database 
    """

    if (connector == 'Oracle'):
        settings_store = singleton_settings_store_factory()
        conn = OracleConnector(source=source, settings=settings_store)

        if is_sql_file(sql):
            path = _current_dir()
            with open(os.path.join(path, sql)) as f:  
                    query = f.read()
                
            return conn.get_pandas_df(query)

        else:     
            return conn.get_pandas_df(sql) 

    if (connector == 'SQLite'):
        conn = SQLiteConnector(source=source)

        if is_sql_file(sql):
            path = _current_dir()
            with open(os.path.join(path, sql)) as f:  
                    query = f.read()
                
            return conn.get_pandas_df(query)

        else:     
            return conn.get_pandas_df(sql)         


def to_sql(df, table, sink=None, schema=None, connector='Oracle'):
    """Write records in dataframe to a SQL database table"""

    if (connector == 'Oracle'):
        settings_store = singleton_settings_store_factory()
        conn = OracleConnector(source=sink, settings=settings_store)
        return conn.persist_pandas_df(table, schema, df)

    # TODO: handle also not in-memory db
    if (connector == 'SQLite'):
        conn = SQLiteConnector(source=sink)
        return conn.persist_pandas_df(table, df)


def _get_csv_schema(df, filename):
    fields = []
    for name, dtype in zip(df.columns,df.dtypes):
        # TODO : Bool and others? Move to utility method
        if str(dtype) == 'object':
            dtype = 'string'
        else:
            dtype = 'number'

        fields.append({'name':name, 'description':'', 'type':dtype})

    return {
            'name': filename,
            'path': 'data/' + filename + '.csv',
            'format':'csv',
            'mediatype': 'text/csv',
            'schema':{'fields':fields}
            }

def _create_datapackage(datasets):
    today = datetime.date.today().strftime('%Y-%m-%d')
    guid = uuid.uuid4().hex
    resources = []
    dir_path = _current_dir()
    for filename, df in datasets.items():
        # TODO bruk Parquet i stedet for csv?
        resources.append(_get_csv_schema(df,filename))

    try:
        with open(os.path.join(dir_path, 'LICENSE.md'), encoding="utf-8") as f:
            license = f.read()
    except:
        license="No LICENSE file available"
        pass

    try:   
        with open(os.path.join(dir_path, 'README.md'), encoding="utf-8") as f:
            readme = f.read()
    except:
        readme="No README file available"
        pass

    metadata  = {}
        
    try:
        with open(os.path.join(dir_path, 'METADATA.json'), encoding="utf-8") as f:
            metadata = json.loads(f.read())
    except:
        # DCAT deprected use METADATA
        try:
            with open(os.path.join(dir_path, 'DCAT.json'), encoding="utf-8") as f:
                metadata = json.loads(f.read())
        except:
            pass

    try:
        with open(os.path.join(dir_path, 'METADATA.json'),'w', encoding="utf-8") as f:
            metadata ['Sist oppdatert'] = today
            #metadata ['Lisens'] = license
            metadata['Datapakke_navn'] = metadata.get('Datapakke_navn', guid)
            f.write(json.dumps( metadata , indent=2))
    except:
        pass
    
    return {
            'name':  metadata.get('name',''),
            'title':  metadata.get('title',''),
            'author':  metadata.get('author',''),
            'status':  metadata.get('status',''),
            'license': license, 
            'readme': readme,
            'metadata': json.dumps( metadata ), 
            'sources': metadata.get('sources',''),
            'last_updated': today,
            'resources': resources,
            'bucket_name': metadata.get('bucket_name', 'default-bucket-nav'),
            'datapackage_name': metadata.get('datapackage_name', guid)
            }

        
def write_datapackage(datasets):
    dir_path = _current_dir()
    with open(dir_path + '/datapackage.json', 'w') as outfile:
        dp = _create_datapackage(datasets)
        status = dp
        # TODO : hvis dp.status == 'Til godkjenning' dump til private s3 (aws?) bucket
        # hvis dp.status = "Offentlig" dump til public s3 aws    
        json.dump(_create_datapackage(datasets), outfile, indent=2, sort_keys=True)

        data_path = dir_path + '/data/'
        if not os.path.exists(data_path):
            try:
                os.makedirs(data_path)
            except OSError as ex: # Guard against race condition
                if ex.errno != errno.EEXIST:
                    raise
                
        for filename, df in datasets.items():
            df.to_csv(dir_path + '/data/' + filename + '.csv', index=False, sep=';')
    return [dp["bucket_name"], dp["datapackage_name"]]


def _datapackage_key_prefix(datapackage_name):
    return datapackage_name + '/'

def publish_datapackage(datasets, destination='nais'):
    # TODO Get destination from metadata instead?
    if destination == 'nais':
        return publish_datapackage_s3_nais(datasets)


    if destination == 'gcs':
        return publish_datapackage_google_cloud(datasets)

    return ValueError('destination not valid')

def publish_datapackage_google_cloud(datasets):
    dir_path = _current_dir()
    bucket_name, datapackage_name = write_datapackage(datasets)

    publish_data.publish_google_cloud(dir_path=dir_path,
                                      bucket_name=bucket_name,
                                      datapackage_key_prefix=_datapackage_key_prefix(datapackage_name))
    
    # index datapackage
    index = ElasticsearchConnector('public')
    test = index
    pass


def publish_datapackage_s3_nais(datasets):
    dir_path = _current_dir()
    bucket_name, datapackage_name = write_datapackage(datasets)

    publish_data.publish_s3_nais(dir_path=dir_path,
                                 bucket_name=bucket_name,
                                 datapackage_key_prefix=_datapackage_key_prefix(datapackage_name))
    # TODO: write to elastic index
    pass
