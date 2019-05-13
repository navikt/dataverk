import copy
import pandas as pd
import json
import datetime
import sys
import uuid
import re
from .utils import (
    validators,
    metadata_utils
)
from pathlib import Path
from collections.abc import Mapping, Sequence


class Datapackage:
    """
    Understands packaging of data resources and views on those resources for publication
    """

    def __init__(self, metadata: Mapping):
        self._resources = {}
        self.views = []
        self._datapackage_metadata = self._create_datapackage(dict(metadata))

    
    def _create_datapackage(self, metadata):
        today = datetime.date.today().strftime('%Y-%m-%d')

        store = metadata.get('store', 'gcs')

        id = None
        
        project = metadata.get('project', None) 
        repo = metadata.get('repo', metadata.get('github-repo', '')) 
        bucket= metadata.get('bucket', metadata.get('bucket-name', None))      
        publisher = metadata.get('publisher', None)
        author = metadata.get('author', None)
        name = metadata.get("name", None)

        if metadata.get('id', None) is not None:
            id = metadata['id']
        
        if id is None:
            id = '-'.join(filter(None, (publisher, author, name)))

        if id is None:
            id = uuid.uuid4()

        id = re.sub('[^0-9a-z]+', '-', id.lower())

        if store=='s3':
            path = f's3://{bucket}/{id}'
            store_path= f's3://{bucket}/{id}'
        elif store=='github':
            path = f'https://raw.githubusercontent.com/{repo}/master/{bucket}/packages/{id}'
            store_path = f'{bucket}/{id}'
        else: #defeault to gcs
            path = f'https://storage.googleapis.com/{bucket}/{id}'
            store_path = f'gs://{bucket}/{id}'


        metadata['id'] = id
        metadata['store_path'] = store_path
        metadata['path'] = path
        metadata['store'] = store
        metadata['updated'] = today
        metadata['version'] = "0.0.1"
        metadata["views"] = []
        metadata["resources"] = []
        metadata["datasets"] = {}
        return metadata

    @property
    def datapackage_metadata(self):
        return copy.deepcopy(self._datapackage_metadata)

    @property
    def resources(self):
        return self._resources

    @property
    def id(self):
        return self._datapackage_metadata.get("id")

    @property
    def project(self):
        return self._datapackage_metadata["project"]

    @property
    def path(self):
        return self._datapackage_metadata.get("path")

    @property
    def uri(self):
        return self._datapackage_metadata.get("uri")

    def add_resource(self, df: pd.DataFrame, dataset_name: str, dataset_description: str=""):
        """
        Adds a provided DataFrame as a resource in the Datapackage object with provided name and description.

        :param df: DataFrame to add as resource
        :param dataset_name: Name of the dataset
        :param dataset_description: Description of the dataset
        :return: None
        """

        self._verify_add_resource_input_types(df, dataset_name, dataset_description)
        self.resources[dataset_name] = df
        self._datapackage_metadata["datasets"][dataset_name] = dataset_description
        self._datapackage_metadata['resources'].append(metadata_utils.get_csv_schema(df, dataset_name))

    def _verify_add_resource_input_types(self, df, dataset_name, dataset_description):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f'df must be of type pandas.Dataframe()')
        if not isinstance(dataset_name, str):
            raise TypeError(f'dataset_name must be of type string')
        if not isinstance(dataset_description, str):
            raise TypeError(f'dataset_description must be of type string')

    def add_view(self, name: str, resources: Sequence, title: str="", description: str="", attribution: str="", spec_type: str="simple",
                 spec: Mapping=None, type: str="", group: str="", series: Sequence=list(), row_limit: int=500, metadata: Mapping=None):
        """
        Adds a view to the Datapackage object. A view is a specification of a visualisation the datapackage provides.

        :param name: View name
        :param resources: resource the view is for
        :param title: Title to be presented in the visualisation
        :param description: Description of the view
        :param attribution:
        :param spec_type: Spec type eg. (matplotlib, vega-lite)
        :param spec:
        :param type:
        :param group:
        :param series:
        :param row_limit:
        :param metadata:
        :return: None
        """
        if spec is None:
            spec = {"type": type,
                    "group": group,
                    "series": series}

        view = {'name': name,
                'title': title,
                'description': description,
                'attribution': attribution,
                'resources': resources,
                'specType': spec_type,
                'spec': spec,
                'transform': {
                    "limit": row_limit
                },
                'metadata': metadata
                }

        self._datapackage_metadata["views"].append(view)