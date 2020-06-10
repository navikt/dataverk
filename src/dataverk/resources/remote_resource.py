from urllib3.util import url
from dataverk.resources.base_resource import BaseResource


class RemoteResource(BaseResource):
    def formatted_resource_name(self):

        parsed_url = url.parse_url(self._resource)

        if not parsed_url.scheme == "https" and not parsed_url.scheme == "http":
            raise ValueError(f"Remote resource needs to be a web address, scheme is {parsed_url.scheme}")

        resource = parsed_url.path.split('/')[-1]
        resource_name_and_format = resource.split('.', 1)
        self._fmt = resource_name_and_format[1]
        return resource_name_and_format[0]

    def _resource_path(self):
        return self._resource

    def get_schema(self):
        return {
            'name': self.formatted_resource_name(),
            'description': self._resource_description,
            'path': self._resource_path(),
            'format': self._fmt,
            'spec': self._spec
        }
