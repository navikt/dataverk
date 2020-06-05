import os
import shutil
import tempfile
import unittest

from unittest import mock
from dataverk.connectors import NaisS3Connector, GoogleStorageConnector
from dataverk.connectors.storage import storage_connector_factory
from dataverk.connectors.storage.file_storage import FileStorageConnector
from dataverk.connectors.storage.storage_connector_factory import StorageType
from tests.dataverk.connectors.storage.test_resources.google_storage_common import GOOGLE_SERVICE_ACCOUNT, GCS_BUCKET_NAME
from tests.dataverk.connectors.storage.test_resources.mock_google_cloud_api import MockGoogleClient, MockGoogleCredentials


class TestStorageConnectorFactory(unittest.TestCase):

    def setUp(self):
        os.environ["DATAVERK_BUCKET_ENDPOINT"] = "https://bucket-endpoint.something.com"
        self.storage_dir = tempfile.mkdtemp()
        self.settings = {
            "bucket_storage": {
                "local": {
                    "path": f"{self.storage_dir}"
                },
                "gcs": {
                    "credentials": GOOGLE_SERVICE_ACCOUNT
                }
            }
        }

    def tearDown(self):
        shutil.rmtree(self.storage_dir)

    @mock.patch("google.cloud.storage.Client", side_effect=MockGoogleClient)
    @mock.patch("google.oauth2.service_account.Credentials", side_effect=MockGoogleCredentials)
    def test_get_storage_connector(self, mock_client, mock_creds):
        connector_types = [("nais", "opendata", NaisS3Connector),
                           ("gcs", GCS_BUCKET_NAME, GoogleStorageConnector),
                           ("local", None, FileStorageConnector)]

        for connector_type in connector_types:
            with self.subTest(msg="Testing bucket connector factory method", _input=connector_type):
                self.assertIsInstance(
                    storage_connector_factory.get_storage_connector(storage_type=StorageType(connector_type[0]),
                                                                    bucket_name=connector_type[1],
                                                                    settings=self.settings),
                    connector_type[2])
