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

    def test_instanciation_invalid_title_not_set(self):
        invalid_metadata = valid_metadata.copy()
        del invalid_metadata['title']

        with self.assertRaises(AttributeError):
            dp = Datapackage(invalid_metadata)


class TestMethodReturnValues(unittest.TestCase):

    def setUp(self):
        self.dp = Datapackage(valid_metadata)

    def test__nais_specific_paths_valid(self):
        dp_id = "id123"
        path, store_path = storage_paths.create_nav_paths(dp_id)
        self.assertEqual(path, f"/api/{dp_id}")
        self.assertEqual(store_path, f"/{dp_id}")
