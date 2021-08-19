import datetime
import os
import uuid
import hashlib
import re

from typing import Any
from dataverk.abc.base import DataverkBase
from dataverk.utils import validators, storage_paths
from collections.abc import Sequence
from dataverk.connectors.storage.storage_connector_factory import StorageType
from dataverk.resources.factory_resources import get_resource_object, ResourceType
from dataverk.utils.metadata_utils import is_nav_environment
from dataverk.views.view_factory import get_view_object


class Datapackage(DataverkBase):
    """
    Understands packaging of data resources and views on those resources for publication
    """

    def __init__(self, metadata: dict, validate: bool = False):
        super().__init__()
        self._resources = []
        self._bucket = metadata.get("bucket")
        self._dp_id = self._get_dp_id(metadata)
        self._title = self._get_dp_title(metadata)

        if validate:
            validators.validate_metadata(metadata)

        self.datapackage_metadata = self._create_datapackage(metadata)

    def _create_datapackage(self, metadata: dict):
        today = datetime.date.today().strftime('%Y-%m-%d')

        if is_nav_environment():
            path, store_path = storage_paths.create_nav_paths(self.dp_id)
        else:
            metadata['store'] = metadata.get('store', StorageType.LOCAL)
            path, store_path = self._generate_paths(metadata)

        metadata['repo'] = metadata.get('repo', metadata.get('github-repo', ''))
        metadata['id'] = self._dp_id
        metadata['store_path'] = store_path
        metadata['path'] = path
        metadata['updated'] = today
        metadata['version'] = "0.0.1"
        metadata["views"] = []
        metadata["resources"] = []

        return metadata

    @property
    def dp_id(self):
        return self._dp_id

    @property
    def bucket(self):
        return self._bucket

    @property
    def resources(self):
        return self._resources

    def add_resource(self, resource: Any, resource_name: str = "",
                     resource_description: str = "", resource_type: str = ResourceType.DF.value,
                     spec: dict = None) -> str:
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
        :return: path: str: resource path
        """
        resource = get_resource_object(resource_type=resource_type, resource=resource,
                                       datapackage_path=self.datapackage_metadata.get("path"),
                                       resource_name=resource_name,
                                       resource_description=resource_description, spec=spec)

        return resource.add_to_datapackage(self)

    def add_view(self, name: str = None, resources: Sequence = None, title: str = "", description: str = "", attribution: str = "",
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
        view = get_view_object(name=name, resources=resources, title=title, description=description, attribution=attribution,
                               spec_type=spec_type, spec=spec, type=type, group=group,
                               series=series, row_limit=row_limit, metadata=metadata)

        view.add_to_datapackage(self)

    def _get_dp_title(self, metadata):
        try:
            return metadata['title']
        except KeyError:
            raise AttributeError(f"title is required to be set in datapackage metadata")

    def _get_dp_id(self, metadata):
        try:
            return metadata['id']
        except KeyError:
            return self._generate_id(metadata)

    def _generate_id(self, metadata):
        author = metadata.get("author", None)
        title = metadata.get("title", None)

        id_string = '-'.join(filter(None, (self._bucket, author, title)))
        if id_string:
            hash_object = hashlib.md5(id_string.encode())
            dp_id = hash_object.hexdigest()
            dp_id = re.sub('[^0-9a-z]+', '-', dp_id.lower())
        else:
            dp_id = uuid.uuid4()

        self.log.info(f"Datapackage id: {dp_id}")
        return dp_id

    def _generate_paths(self, metadata):
        store = metadata.get('store')

        if StorageType(store) is StorageType.GCS:
            path, store_path = storage_paths.create_gcs_paths(self._bucket, self.dp_id)
        elif StorageType(store) is StorageType.LOCAL:
            path, store_path = storage_paths.create_local_paths(self._bucket, self.dp_id)
        else:
            raise NotImplementedError(f"""StorageType {store} is not supported.
             Supported types are {[name.value for name in StorageType]}.""")

        return path, store_path
