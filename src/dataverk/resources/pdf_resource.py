from dataverk.utils import file_functions

from dataverk.resources.base_resource import BaseResource


class PDFResource(BaseResource):
    def formatted_resource_name(self):
        return file_functions.remove_whitespace(self._resource_name)

    def _resource_path(self):
        return self._create_resource_path(self._datapackage_path, self.formatted_resource_name(), self._fmt,
                                          self._compress)

    def get_schema(self):
        return {
            'name': self.formatted_resource_name(),
            'description': self._resource_description,
            'path': self._resource_path(),
            'format': self._fmt,
            'spec': self._spec
        }
