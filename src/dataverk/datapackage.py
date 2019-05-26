import copy
import pandas as pd
import datetime
import uuid
import hashlib
import re
from dataverk.utils import (
    validators,
    metadata_utils
)
from collections.abc import Mapping, Sequence
from dataverk.connectors.bucket_connector_factory import BucketType


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

        try:
            bucket = metadata['bucket']
        except KeyError:
            raise AttributeError(f"<bucket> is required to be set in datapackage metadata")
        else:
            validators.validate_bucket_name(bucket)

        try:
            metadata['name']
        except KeyError:
            raise AttributeError(f"<name> is required to be set in datapackage metadata")

        # set defaults for store and repo when not specified
        metadata['store'] = metadata.get('store', BucketType.GITHUB)
        metadata['repo'] = metadata.get('repo', metadata.get('github-repo', ''))

        try:
            dp_id = metadata['id']
        except KeyError:
            dp_id = self._generate_id(metadata)

        metadata['id'] = dp_id
        path, store_path = self._generate_paths(metadata)

        metadata['store_path'] = store_path
        metadata['path'] = path
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
    def dp_id(self):
        return self._datapackage_metadata.get("id")

    @property
    def project(self):
        return self._datapackage_metadata.get("project")

    @property
    def path(self):
        return self._datapackage_metadata.get("path")

    @property
    def uri(self):
        return self._datapackage_metadata.get("uri")

    def add_resource(self, df: pd.DataFrame, dataset_name: str, dataset_description: str="", format="csv", separator=","):
        """
        Adds a provided DataFrame as a resource in the Datapackage object with provided name and description.

        :param df: DataFrame to add as resource
        :param dataset_name: Name of the dataset
        :param dataset_description: Description of the dataset
        :param separator: field separator
        :return: None
        """

        resource_metadata = metadata_utils.get_schema(df=df, dataset_name=dataset_name, format=format, separator=separator)

        self._verify_add_resource_input_types(df, dataset_name, dataset_description)
        self.resources[dataset_name] = resource_metadata
        self._datapackage_metadata["datasets"][dataset_name] = dataset_description
        #self._datapackage_metadata['resources'].append(metadata_utils.get_csv_schema(df, dataset_name, separator))

    def _verify_add_resource_input_types(self, df, dataset_name, dataset_description):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f'df must be of type pandas.Dataframe()')
        if not isinstance(dataset_name, str):
            #TODO: check if valid filename
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

    @staticmethod
    def _generate_id(metadata):
        project = metadata.get("project", None)
        publisher = metadata.get("publisher", None)
        author = metadata.get("author", None)
        name = metadata.get("name", None)
        bucket = metadata.get("bucket", None)

        id_string = '-'.join(filter(None, (project, bucket, publisher, author, name)))
        if id_string:
            hash_object = hashlib.md5(id_string.encode())
            dp_id = hash_object.hexdigest()
            return re.sub('[^0-9a-z]+', '-', dp_id.lower())
        else:
            return uuid.uuid4()

    @staticmethod
    def _generate_paths(metadata):
        store = metadata['store']
        repo = metadata['repo']
        bucket = metadata['bucket']
        name = metadata['name']

        if BucketType(store) is BucketType.DATAVERK_S3:
            path = f's3://{bucket}/{name}'
            store_path = f's3://{bucket}/{name}'
        elif BucketType(store) is BucketType.GCS:
            path = f'https://storage.googleapis.com/{bucket}/{name}'
            store_path = f'gs://{bucket}/{name}'
        else: #default is local storage
            path = f'https://raw.githubusercontent.com/{repo}/master/{bucket}/packages/{name}'
            store_path = f'{bucket}/{name}'

        return path, store_path
