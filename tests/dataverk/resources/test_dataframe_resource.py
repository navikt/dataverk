import unittest
import pandas as pd

from dataverk.resources.dataframe_resource import DataFrameResource

resource_name = "my resource"
resource_description = "desc"
fmt = "csv"
dsv_separator = ";"
compress = True
mediatype = "text/csv"
path = "https://some.bucket.storage.com"
spec = {
    'format': fmt,
    'compress': compress
}

expected_resource_name = resource_name.replace(" ", "_")

expected_schema = {
    'name': expected_resource_name,
    'description': resource_description,
    'path': f'{path}/resources/{expected_resource_name}.{fmt}.gz',
    'format': fmt,
    'dsv_separator': dsv_separator,
    'compressed': compress,
    'mediatype': mediatype,
    'schema': {'fields': [
        {'name': 'col1', 'description': '', 'type': 'number'},
        {'name': 'col2', 'description': '', 'type': 'number'}
    ]},
    'spec': spec
}
df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})


class TestMethodReturnValues(unittest.TestCase):

    def setUp(self):
        self.resource_formatter = DataFrameResource(resource=df, resource_name=resource_name,
                                                    resource_description=resource_description, datapackage_path=path,
                                                    spec=spec)

    def test__resource_path(self):
        actual_path = self.resource_formatter._resource_path()
        expected_path = expected_schema.get('path')
        self.assertEqual(expected_path, actual_path)

    def test__formatted_resource_name(self):
        actual_formatted_name = self.resource_formatter.formatted_resource_name()
        expected_formatted_name = expected_schema.get('name')
        self.assertEqual(expected_formatted_name, actual_formatted_name)

    def test__get_schema(self):
        actual_schema = self.resource_formatter._get_schema()
        self.assertEqual(expected_schema, actual_schema)

