from urllib3.util import url
from dataverk.resources.base_resource import BaseResource


class RemoteResource(BaseResource):
    def __init__(
        self,
        resource: str,
        datapackage_path: str,
        resource_description: str,
        spec: dict = None,
    ):
        super().__init__(resource, datapackage_path, resource_description, spec)
        self._schema = self._get_schema()

    def formatted_resource_name(self):
        formatted_resource_name, self._fmt = self._resource_name_and_type_from_url(
            self._resource_data
        )
        return formatted_resource_name

    def _resource_path(self):
        return self._resource_data

    def _get_schema(self):
        return {
            "name": self.formatted_resource_name(),
            "description": self._resource_description,
            "path": self._resource_path(),
            "format": self._fmt,
            "spec": self._spec,
        }

    @staticmethod
    def _resource_name_and_type_from_url(resource_url):
        parsed_url = url.parse_url(resource_url)

        if not parsed_url.scheme == "https" and not parsed_url.scheme == "http":
            raise ValueError(
                f"Remote resource needs to be a web address, scheme is {parsed_url.scheme}"
            )

        resource = parsed_url.path.split("/")[-1]
        resource_name_and_format = resource.split(".", 1)
        return resource_name_and_format[0], resource_name_and_format[1]

    def add_to_datapackage(self, dp) -> None:
        """ Adds a remote resource to the datapackage

        :param dp: Datapackage object to append resources to
        :return: path: str: Path to resource
        """
        dp.datapackage_metadata["resources"].append(self._schema)
        return self._schema.get("path")
