import copy
import datetime
import uuid
import hashlib
import re

from typing import Any
from os import environ

from data_catalog_dcat_validator.models.dataset import DatasetModel
from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet
from dataverk.utils import validators, file_functions
from collections.abc import Sequence
from dataverk.connectors.storage.storage_connector_factory import StorageType
from dataverk.resources.factory_resources import get_resource_object, ResourceType


class Datapackage:
    """
    Understands packaging of data resources and views on those resources for publication
    """

    def __init__(self, metadata: dict, validate: bool = True):
        self._resources = {}
        self.views = []

        if validate:
            self._validate_metadata(metadata)

        self.datapackage_metadata = self._create_datapackage(dict(metadata))

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
    def resources(self):
        return self._resources

    @property
    def dp_id(self):
        return self.datapackage_metadata.get("id")

    @property
    def project(self):
        return self.datapackage_metadata.get("project")

    @property
    def path(self):
        return self.datapackage_metadata.get("path")

    @property
    def url(self):
        return self.datapackage_metadata.get("url")

    def add_resource(self, resource: Any, resource_name: str = "",
                     resource_description: str = "", resource_type: str = ResourceType.DF.value,
                     spec: dict = None):
        """
        Adds a resource to the Datapackage object. Supported resource types are "df", "remote" and "pdf".

        :param resource: any, resource to be added to Datapackage

        :param resource_type: str, type of resource. Supported types are "df", "remote" and "pdf".
        "df" expects a pandas DataFrame
        "remote" expects a valid url(str) to an already available resource and
        "pdf" expects a bytes representation of a pdf file.

        :param resource_name: str, name of resource, default = ""
        Not applicable for remote resources.

        :param resource_description: str, description of resource, default = ""
        :param spec: dict, resource specification e.g hidden, fields, format, compress, etc, default = None
        :return: None
        """
        resource = get_resource_object(resource_type=resource_type, resource=resource,
                                       datapackage_path=self.path, resource_name=resource_name,
                                       resource_description=resource_description, spec=spec)
        resource.add_to_datapackage(self)

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

        self.datapackage_metadata["views"].append(view)

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
        else:  # default is local storage
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
