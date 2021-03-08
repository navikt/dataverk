import os
import unittest
from dataverk.datapackage import Datapackage
from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet
from dataverk.utils import storage_paths

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

    def tearDown(self):
        for env in ["DATAVERK_API_ENDPOINT", "DATAVERK_BUCKET_ENDPOINT"]:
            try:
                del os.environ[env]
            except KeyError:
                pass

    def test_instanciation_valid(self):
        expected_id = "2138c6203baa39c3c573afdec4404416"
        dp = Datapackage(valid_metadata)
        self.assertIsInstance(dp, Datapackage)
        self.assertEqual(expected_id, dp.dp_id)

    def test_instanciation_invalid_bucket_env_not_set(self):
        metadata = valid_metadata.copy()
        os.environ["DATAVERK_BUCKET_ENDPOINT"] = "https://test.com/some/path"
        os.environ["DATAVERK_API_ENDPOINT"] = "https://test.com/some/path"
        with self.assertRaises(EnvironmentVariableNotSet):
            dp = Datapackage(metadata)

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

    def tearDown(self):
        for env in ["DATAVERK_API_ENDPOINT", "DATAVERK_BUCKET_ENDPOINT",
                    "DATAVERK_BUCKET", "DATAVERK_BUCKET_SHORT"]:
            try:
                del os.environ[env]
            except KeyError:
                pass

    def test__nais_specific_paths_valid(self):
        api_endpoint = "https://dataverk.no"
        bucket_endpoint = "https://dataverk.no"
        bucket = "bucket"
        dp_id = "id123"
        os.environ["DATAVERK_API_ENDPOINT"] = api_endpoint
        os.environ["DATAVERK_BUCKET_ENDPOINT"] = bucket_endpoint
        os.environ["DATAVERK_BUCKET"] = bucket
        path, store_path = storage_paths.create_nav_paths(dp_id)
        self.assertEqual(path, f"{api_endpoint}/{bucket}/{dp_id}")
        self.assertEqual(store_path, f"{bucket_endpoint}/{bucket}/{dp_id}")

    def test__nais_specific_paths_bucket_short_name_valid(self):
        api_endpoint = "https://dataverk.no"
        bucket_endpoint = "https://dataverk.no"
        bucket = "bucket-name"
        bucket_short = "bucket"
        dp_id = "id123"
        os.environ["DATAVERK_API_ENDPOINT"] = api_endpoint
        os.environ["DATAVERK_BUCKET_ENDPOINT"] = bucket_endpoint
        os.environ["DATAVERK_BUCKET"] = bucket
        os.environ["DATAVERK_BUCKET_SHORT"] = bucket_short
        path, store_path = storage_paths.create_nav_paths(dp_id)
        self.assertEqual(path, f"{api_endpoint}/{bucket_short}/{dp_id}")
        self.assertEqual(store_path, f"{bucket_endpoint}/{bucket_short}/{dp_id}")

    def test__nais_specific_paths_invalid_api_not_set(self):
        bucket_endpoint = "https://dataverk.no"
        bucket = "bucket"
        dp_id = "id123"
        os.environ["DATAVERK_BUCKET"] = bucket
        os.environ["DATAVERK_BUCKET_ENDPOINT"] = bucket_endpoint
        with self.assertRaises(EnvironmentVariableNotSet):
            path, store_path = storage_paths.create_nav_paths(dp_id)

    def test__nais_specific_paths_invalid_bucket_not_set(self):
        api_endpoint = "https://dataverk.no"
        bucket = "bucket"
        dp_id = "id123"
        os.environ["DATAVERK_BUCKET"] = bucket
        os.environ["DATAVERK_API_ENDPOINT"] = api_endpoint
        with self.assertRaises(EnvironmentVariableNotSet):
            path, store_path = storage_paths.create_nav_paths(dp_id)
        del os.environ["DATAVERK_API_ENDPOINT"]