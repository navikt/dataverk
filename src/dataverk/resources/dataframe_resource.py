import copy
import io
import pandas as pd

from dataverk import Datapackage
from dataverk.utils import file_functions
from dataverk.resources.base_resource import BaseResource


class DataFrameResource(BaseResource):
    def __init__(self, resource: pd.DataFrame, datapackage_path: str, resource_name: str, resource_description: str,
                 fmt: str, compress: bool, spec: dict = None):
        super().__init__(resource, datapackage_path, resource_description, fmt, compress, spec)

        self._resource_name = resource_name

    def formatted_resource_name(self):
        return file_functions.remove_whitespace(self._resource_name)

    def _resource_path(self):
        return self._create_resource_path(self._datapackage_path, self.formatted_resource_name(), self._fmt,
                                          self._compress)

    def _get_schema(self):
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

    def add_to_datapackage(self, dp: Datapackage) -> None:
        """ Converts a pandas Dataframe object to csv and adds it to the datapackage

        :param dp: Datapackage object to append
        :return: None
        """
        formatted_resource_name = self._schema.get('name')
        dsv_separator = self._schema.get("dsv_separator")
        dp.datapackage_metadata['resources'].append(self._schema)
        dp.resources[formatted_resource_name] = copy.deepcopy(self._schema)

        data_buff = io.StringIO()
        self._resource.to_csv(data_buff, sep=dsv_separator, index=False, encoding="utf-8-sig")

        if self._compress:
            dp.resources[formatted_resource_name]["data"] = file_functions.compress_content(data_buff)
            dp.resources[formatted_resource_name]["format"] += ".gz"
        else:
            dp.resources[formatted_resource_name]["data"] = data_buff.getvalue()
