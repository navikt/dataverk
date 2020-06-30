from dataverk.utils import file_functions

from dataverk.resources.base_resource import BaseResource


class JSONResource(BaseResource):
    def __init__(
        self,
        resource: bytes,
        datapackage_path: str,
        resource_name: str,
        resource_description: str,
        spec: dict = None,
    ):
        super().__init__(resource, datapackage_path, resource_description, spec)

        self._resource_name = resource_name
        self._fmt = self._spec.get("format", "json")
        self._schema = self._get_schema()

    def formatted_resource_name(self):
        return file_functions.url_encode(self._resource_name)

    def _resource_path(self):
        return self._create_resource_path(
            self._datapackage_path,
            self.formatted_resource_name(),
            self._fmt,
            False
        )

    def _get_schema(self):
        return {
            "name": self.formatted_resource_name(),
            "description": self._resource_description,
            "path": self._resource_path(),
            "format": self._fmt,
            "mediatype": self._media_type(self._fmt),
            "spec": self._spec,
        }

    def add_to_datapackage(self, dp) -> str:
        """ Adds a json object to the datapackage

        :param dp: Datapackage object to append resources to
        :return: path: str: Path to resource
        """
        formatted_resource_name = self._schema.get("name")
        dp.resources[formatted_resource_name] = self._schema
        dp.resources[formatted_resource_name]["data"] = self._resource
        return self._schema.get("path")
