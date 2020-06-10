import copy
from typing import Any
import pandas as pd
import datetime
import uuid
import hashlib
import re

from os import environ


from data_catalog_dcat_validator.models.dataset import DatasetModel
from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet
from dataverk.utils import validators, file_functions
from collections.abc import Sequence
from dataverk.connectors.storage.storage_connector_factory import StorageType
from dataverk.resources.dataframe_resource import DataFrameResource
from dataverk.resources.remote_resource import RemoteResource
from dataverk.resources.pdf_resource import PDFResource


class Datapackage:
    """
    Understands packaging of data resources and views on those resources for publication
    """

    def __init__(self, metadata: dict):
        self._resources = {}
        self.views = []
        self._validate_metadata(metadata)
        self._datapackage_metadata = self._create_datapackage(dict(metadata))

    def _create_datapackage(self, metadata):
        today = datetime.date.today().strftime('%Y-%m-%d')

        try:
            bucket = metadata['bucket']
        except KeyError:
            raise AttributeError(f"bucket is required to be set in datapackage metadata")
        else:
            validators.validate_bucket_name(bucket)

        try:
            metadata['title']
        except KeyError:
            raise AttributeError(f"title is required to be set in datapackage metadata")

        # set defaults for store and repo when not specified
        metadata['store'] = metadata.get('store', StorageType.LOCAL)
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

    @staticmethod
    def _validate_metadata(metadata: dict):
        validator = DatasetModel(metadata)
        validator.validate()
        validator.error_report()

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
    def url(self):
        return self._datapackage_metadata.get("url")

    def add_resource(self, resource: Any, resource_type: str, resource_name: str,
                     resource_description: str = "", spec: dict = None):
        """
        :param resource:
        :param resource_type:
        :param resource_name:
        :param resource_description:
        :param spec:
        :return:
        """

        if resource_type == 'df':
            fmt = spec.get('format', 'csv')
            compress = spec.get('compress', True)
            formatted_resource = DataFrameResource(resource=resource, datapackage_path=self.path,
                                                   resource_name=resource_name,
                                                   resource_description=resource_description,
                                                   fmt=fmt, compress=compress, spec=spec).get_schema()

            self._datapackage_metadata["datasets"][formatted_resource.get('resource_name')] = resource_description

        elif resource_type == 'remote':
            formatted_resource = RemoteResource(resource=resource, datapackage_path=self.path,
                                                resource_name=resource_name,
                                                resource_description=resource_description,
                                                fmt="", compress=False, spec=spec)
        elif resource_type == 'pdf':
            compress = spec.get('compress', False)
            formatted_resource = PDFResource(resource=resource, datapackage_path=self.path,
                                             resource_name=resource_name,
                                             resource_description=resource_description,
                                             fmt="pdf", compress=compress, spec=spec)

        else:
            raise TypeError(f"Resources of type {resource_type} is not supported.")

        formatted_resource_name = formatted_resource.get('resource_name')

        self.resources[formatted_resource_name] = formatted_resource
        if resource_type == 'df':
            self.resources[formatted_resource_name]['df'] = resource

        self._datapackage_metadata['resources'].append(formatted_resource)

    # TODO: IMPLEMENT THIS METHOD
    @staticmethod
    def _verify_add_resource_input_types(df, dataset_name, dataset_description):
        if not isinstance(df, pd.DataFrame):
            raise TypeError(f'df must be of type pandas.Dataframe()')
        if not isinstance(dataset_name, str):
            raise TypeError(f'dataset_name must be of type string')
        if not isinstance(dataset_description, str):
            raise TypeError(f'dataset_description must be of type string')

    def add_view(self, name: str, resources: Sequence, title: str = "", description: str = "", attribution: str = "",
                 spec_type: str = "simple", spec: dict = None, type: str = "", group: str = "",
                 series: Sequence = list(), row_limit: int = 500, metadata: dict = None):
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

        view = {'name': file_functions.remove_whitespace(name),
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
        author = metadata.get("author", None)
        title = metadata.get("title", None)
        bucket = metadata.get("bucket", None)

        id_string = '-'.join(filter(None, (bucket, author, title)))
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
        dp_id = metadata['id']

        if StorageType(store) is StorageType.NAIS:
            path, store_path = Datapackage._nais_specific_paths(bucket, dp_id)
        elif StorageType(store) is StorageType.GCS:
            path = f'https://storage.googleapis.com/{bucket}/{dp_id}'
            store_path = f'gs://{bucket}/{dp_id}'
        else: #default is local storage
            path = f'https://raw.githubusercontent.com/{repo}/master/{bucket}/packages/{dp_id}'
            store_path = f'{bucket}/{dp_id}'

        return path, store_path

    @staticmethod
    def _nais_specific_paths(bucket, dp_id):
        try:
            api_endpoint = environ["DATAVERK_API_ENDPOINT"]
        except KeyError as missing_env:
            raise EnvironmentVariableNotSet(str(missing_env))
        else:
            path = f'{api_endpoint}/{bucket}/{dp_id}'

        try:
            bucket_endpoint = environ["DATAVERK_BUCKET_ENDPOINT"]
        except KeyError as missing_env:
            raise EnvironmentVariableNotSet(str(missing_env))
        else:
            store_path = f'{bucket_endpoint}/{bucket}/{dp_id}'

        return path, store_path
