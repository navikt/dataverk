import unittest
import pandas as pd
from dataverk.datapackage import Datapackage
from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet

valid_metadata = {
    'title': 'title',
    'readme': "readme",
    'license': 'MIT',
    'accessRights': 'Open',
    'auth': 'unknown',
    'description': 'unknown',
    'source': 'unknown',
    'keywords': ['unknown'],
    'provenance': 'unknown',
    'publisher': 'unknown',
    'bucket': 'opendata',
    'store': 'local',
    'format': ['datapackage'],
    'pii': '',
    'purpose': 'open data',
    'master': 'secret'
}


class TestClassInstanciation(unittest.TestCase):

    def test_instanciation_valid(self):
        expected_id = "2138c6203baa39c3c573afdec4404416"
        dp = Datapackage(valid_metadata)
        self.assertIsInstance(dp, Datapackage)
        self.assertEqual(expected_id, dp.dp_id)

    def test_instanciation_invalid_bucket_env_not_set(self):
        invalid_metadata = valid_metadata.copy()
        invalid_metadata['store'] = 'nais'
        with self.assertRaises(EnvironmentVariableNotSet):
            dp = Datapackage(invalid_metadata)

    def test_instanciation_invalid_bucket_not_set(self):
        invalid_metadata = valid_metadata.copy()
        del invalid_metadata['bucket']

        with self.assertRaises(AttributeError):
            dp = Datapackage(invalid_metadata)

    def test_instanciation_invalid_title_not_set(self):
        invalid_metadata = valid_metadata.copy()
        del invalid_metadata['title']

        with self.assertRaises(AttributeError):
            dp = Datapackage(invalid_metadata)


class TestMethodReturnValues(unittest.TestCase):

    def setUp(self):
        self.dp = Datapackage(valid_metadata)

    def test__get_schema(self):
        resource_name = "my-package"
        resource_description = "desc"
        fmt = "csv"
        dsv_separator = ";"
        compress = True
        mediatype = "text/csv"
        path = "https://some.bucket.storage.com"
        spec = None
        expected_schema = {
            'name': resource_name,
            'description': resource_description,
            'path': f'{path}/resources/{resource_name}.{fmt}.gz',
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
        schema = self.dp._get_schema(df, path=path,
                                     resource_name=resource_name, resource_description=resource_description,
                                     format=fmt, compress=True, dsv_separator=";", spec=None)
        self.assertEqual(expected_schema, schema)

    def test__resource_name_and_type_from_url_zipped(self):
        resource_name_in = "resource"
        resource_fmt_in = "csv.gz"
        resource_url = f"https://remote.storage.location.com/bucket/" \
                       f"datapackage/resources/{resource_name_in}.{resource_fmt_in}"
        resource_name, resource_fmt = Datapackage._resource_name_and_type_from_url(resource_url)
        self.assertEqual(resource_name_in, resource_name)
        self.assertEqual(resource_fmt_in, resource_fmt)

    def test__resource_name_and_type_from_url(self):
        resource_name_in = "resource"
        resource_fmt_in = "csv"
        resource_url = f"http://remote.storage.location.com/bucket/" \
                       f"datapackage/resources/{resource_name_in}.{resource_fmt_in}"
        resource_name, resource_fmt = Datapackage._resource_name_and_type_from_url(resource_url)
        self.assertEqual(resource_name_in, resource_name)
        self.assertEqual(resource_fmt_in, resource_fmt)

    def test__resource_name_and_type_from_url_invalid(self):
        resource_url = "/not/a/web/url/bucket/datapackage/resources/resource.csv.gz"
        with self.assertRaises(ValueError):
            Datapackage._resource_name_and_type_from_url(resource_url)
