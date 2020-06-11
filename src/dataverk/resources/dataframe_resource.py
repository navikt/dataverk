from dataverk.utils import file_functions

from dataverk.resources.base_resource import BaseResource


class DataFrameResource(BaseResource):
    def formatted_resource_name(self):
        return file_functions.remove_whitespace(self._resource_name)

    def _resource_path(self):
        return self._create_resource_path(self._datapackage_path, self.formatted_resource_name(), self._fmt,
                                          self._compress)

    def get_schema(self):
        fields = []

        for name, dtype in zip(self._resource.columns, self._resource.dtypes):
            if str(dtype) == 'object':
                dtype = 'string'
            else:
                dtype = 'number'

            description = ''
            if self._spec and self._spec.get('fields'):
                spec_fields = self._spec['fields']
                for field in spec_fields:
                    if field['name'] == name:
                        description = field['description']

            fields.append({'name': name, 'description': description, 'type': dtype})

        dsv_separator = self._spec.pop('dsv_separator', ';')
        mediatype = self._media_type(self._fmt)

        return {
            'name': self.formatted_resource_name(),
            'description': self._resource_description,
            'path': self._resource_path(),
            'format': self._fmt,
            'dsv_separator': dsv_separator,
            'compressed': self._compress,
            'mediatype': mediatype,
            'schema': {'fields': fields},
            'spec': self._spec
        }
