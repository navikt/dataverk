import json
import pandas as pd
import dataparcel as dp
import dataverk 

import io
import pprint

from dataverk.dataverk import Dataverk
from os import environ

def main():
    environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/users/b149899/projects/binder/binder/keys/mesiqi.json"

    readme = """
    # Readme
    ## Her er en overskrift
    Her er litt mer info om datasettet
    """

    #'store': 'gcs','github','ceph','s3'
    metadata = {
    #'store': 'github',
    #'repo': 'github.com/datasett/dataparcel',
    'store': 'gcs',
    'title': 'Dataparcel test',
    'readme': readme,
    'license':'MIT',
    'accessRights':'Open',
    'author': 'Paul Bencze',
    'description':'Test package for dataparcel',
    'name': 'Ledige stillinger SSB',
    'source':'NAV',
    'keywords':['test'],
    'provenance':'NAV',
    'publisher': 'NAV',
    'bucket': 'dataparcel',
    'bucket_name': 'dataparcel',
    'project': 'mesiqi'
    }
    
    pa = dp.Datapackage(metadata)
    dv = Dataverk()

    print(pa.path)

    data = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}
    df = pd.DataFrame.from_dict(data)

    pa.add_resource(df,'data_resource_name','data_resource_description')

    desc = '''
    Test table.
    '''

    pa.add_view(
        spec_type='table',
        description = desc,
        name='table name',
        title='table title',
        resources = 'data_resource_name',
        metadata = {'col1': {'format': 'string', 'description': 'numbers'},
                    'col2': {'format': 'number', 'description': 'strings'}, }
    )

    print(pa.datapackage_metadata)
    dv.publish(pa)

if __name__ == "__main__":
    main()

