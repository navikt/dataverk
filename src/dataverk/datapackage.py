import copy
import pandas as pd
import datetime
import uuid
import hashlib
import re
from urllib3.util import url
from os import environ

from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet
from dataverk.utils import validators, file_functions
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
            raise AttributeError(f"bucket is required to be set in datapackage metadata")
        else:
            validators.validate_bucket_name(bucket)

        try:
            metadata['title']
        except KeyError:
            raise AttributeError(f"title is required to be set in datapackage metadata")

        # set defaults for store and repo when not specified
        metadata['store'] = metadata.get('store', BucketType.LOCAL)
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
    def url(self):
        return self._datapackage_metadata.get("url")

    def _get_schema(self, df, path, resource_name, resource_description, format, compress, dsv_separator, spec):
        fields = []

        for name, dtype in zip(df.columns, df.dtypes):
            if str(dtype) == 'object':
                dtype = 'string'
            else:
                dtype = 'number'

            description = ''
            if spec and spec.get('fields'):
                spec_fields = spec['fields']
                for field in spec_fields:
                    if field['name'] == name:
                        description = field['description']

            fields.append({'name': name, 'description': description, 'type': dtype})

        mediatype = self._media_type(format)

        return {
            'name': resource_name,
            'description': resource_description,
            'path': self._resource_path(path, resource_name, format, compress),
            'format': format,
            'dsv_separator': dsv_separator,
            'compressed': compress,
            'mediatype': mediatype,
            'schema': {'fields': fields},
            'spec': spec
        }

    @staticmethod
    def _media_type(fmt):
        if fmt == 'csv':
            return 'text/csv'
        elif fmt == 'json':
            return 'application/json'
        else:
            return 'text/csv'

    @staticmethod
    def _resource_path(path, resource_name, fmt, compress):
        if compress:
            return f'{path}/resources/{resource_name}.{fmt}.gz'
        else:
            return f'{path}/resources/{resource_name}.{fmt}'

    def add_resource(self, df: pd.DataFrame, resource_name: str, resource_description: str="",
                     format="csv", compress: bool=True, dsv_separator=";", spec: Mapping=None):
        """
        Adds a provided DataFrame as a resource in the Datapackage object with provided name and description.

        :param df: DataFrame to add as resource
        :param resource_name: Name of the resource
        :param resource_description: Description of the resource
        :param format: file format of resource
        :param compress: boolean value indicating whether to compress resource before storage
        :param dsv_separator: field separator
        :param spec: resource specification
        :return: None
        """
        self._verify_add_resource_input_types(df, resource_name, resource_description)
        resource_name = file_functions.remove_whitespace(resource_name)
        self.resources[resource_name] = self._get_schema(df=df, path=self.path, resource_name=resource_name,
                                                         resource_description=resource_description, format=format,
                                                         compress=compress, dsv_separator=dsv_separator, spec=spec)
        self.resources[resource_name]['df'] = df
        self._datapackage_metadata["datasets"][resource_name] = resource_description
        self._datapackage_metadata['resources'].append(self._get_schema(df=df, path=self.path,
                                                                        resource_name=resource_name,
                                                                        resource_description=resource_description,
                                                                        format=format, compress=compress,
                                                                        dsv_separator=dsv_separator, spec=spec))

    def add_remote_resource(self, resource_url: str, resource_description: str=""):
        resource_name, resource_fmt = self._resource_name_and_type_from_url(resource_url)
        self._datapackage_metadata['datasets'][resource_name] = resource_description
        self._datapackage_metadata['resources'].append({
            'name': resource_name,
            'description': resource_description,
            'path': resource_url,
            'format': resource_fmt
        })

    @staticmethod
    def _resource_name_and_type_from_url(resource_url):
        parsed_url = url.parse_url(resource_url)

        if not parsed_url.scheme == "https" and not parsed_url.scheme == "http":
            raise ValueError(f"Remote resource needs to be a web address, scheme is {parsed_url.scheme}")

        resource = parsed_url.path.split('/')[-1]
        resource_name_and_format = resource.split('.', 1)
        return resource_name_and_format[0], resource_name_and_format[1]

    @staticmethod
    def _verify_add_resource_input_types(df, dataset_name, dataset_description):
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

        if BucketType(store) is BucketType.NAIS:
            path, store_path = Datapackage._nais_specific_paths(bucket, dp_id)
        elif BucketType(store) is BucketType.GCS:
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
            raise EnvironmentVariableNotSet(missing_env)
        else:
            path = f'{api_endpoint}/{bucket}/{dp_id}'

        try:
            bucket_endpoint = environ["DATAVERK_BUCKET_ENDPOINT"]
        except KeyError as missing_env:
            raise EnvironmentVariableNotSet(missing_env)
        else:
            store_path = f'{bucket_endpoint}/{bucket}/{dp_id}'

        return path, store_path
