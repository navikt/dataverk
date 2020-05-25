import json
import shutil
import tempfile
import unittest

from unittest import mock
from pathlib import Path
from dataverk.exceptions import dataverk_exceptions
from dataverk.connectors import GoogleStorageConnector
from tests.dataverk.connectors.storage.test_utils.google_storage_common import GCS_BUCKET_NAME, SETTINGS, \
    GOOGLE_SERVICE_ACCOUNT, BLOB_PUBLIC_URL, BLOB_DATA, EXPECTED_BLOB_METADATA_MARKDOWN, EXPECTED_BLOB_METADATA_OBJECT
from tests.dataverk.connectors.storage.test_utils.mock_google_cloud_api import MockGoogleClient, MockGoogleCredentials


class TestGoogleStorageConnectorStaticMethods(unittest.TestCase):

    def setUp(self):
        self.credentials_dir = tempfile.mkdtemp()
        with open(Path(self.credentials_dir).joinpath("service-account.json"), "w") as serviceaccount_file:
            json.dump(GOOGLE_SERVICE_ACCOUNT, serviceaccount_file)

    def tearDown(self):
        shutil.rmtree(self.credentials_dir)

    def test__parse_gcp_credentials_valid(self):
        credential_formats = [f"{Path(self.credentials_dir).joinpath('service-account.json')}",
                              GOOGLE_SERVICE_ACCOUNT,
                              json.dumps(GOOGLE_SERVICE_ACCOUNT)]

        for credential in credential_formats:
            with self.subTest(msg="Test parse gcp credentials", _input=credential):
                creds = GoogleStorageConnector.parse_gcp_credentials(credential)
                self.assertEqual(creds, GOOGLE_SERVICE_ACCOUNT)

    def test_is_json_file_true(self):
        is_json = GoogleStorageConnector.is_json_file(f"{Path(self.credentials_dir).joinpath('service-account.json')}")
        self.assertTrue(is_json)

    def test_is_json_file_false(self):
        is_json = GoogleStorageConnector.is_json_file(json.dumps(GOOGLE_SERVICE_ACCOUNT))
        self.assertFalse(is_json)


class TestGoogleStorageConnector(unittest.TestCase):

    @mock.patch("google.cloud.storage.Client", side_effect=MockGoogleClient)
    @mock.patch("google.oauth2.service_account.Credentials", side_effect=MockGoogleCredentials)
    def test_instantiation_valid(self, mock_client, mock_creds):
        storage_conn = GoogleStorageConnector(GCS_BUCKET_NAME, SETTINGS)
        self.assertIsInstance(storage_conn, GoogleStorageConnector)

    @mock.patch("google.cloud.storage.Client", side_effect=MockGoogleClient)
    @mock.patch("google.oauth2.service_account.Credentials", side_effect=MockGoogleCredentials)
    def test_instantiation_invalid_settings_object(self, mock_client, mock_creds):
        with self.assertRaises(dataverk_exceptions.IncompleteSettingsObject):
            storage_conn = GoogleStorageConnector(GCS_BUCKET_NAME, {})

    @mock.patch("google.cloud.storage.Client", side_effect=MockGoogleClient)
    @mock.patch("google.oauth2.service_account.Credentials", side_effect=MockGoogleCredentials)
    def test_instantiation_invalid_bucket_not_found(self, mock_client, mock_creds):
        with self.assertRaises(dataverk_exceptions.StorageBucketDoesNotExist):
            storage_conn = GoogleStorageConnector("non-existent-bucket", SETTINGS)

    @mock.patch("google.cloud.storage.Client", side_effect=MockGoogleClient)
    @mock.patch("google.oauth2.service_account.Credentials", side_effect=MockGoogleCredentials)
    def test_write_valid(self, mock_client, mock_creds):
        test_data = "test;test;test\n1;2;3"
        metadata = {
            "id": "test",
            "title": "test",
            "description": "test",
            "bucket": GCS_BUCKET_NAME,
            "author": "test"
        }
        storage_conn = GoogleStorageConnector(GCS_BUCKET_NAME, SETTINGS)
        public_url = storage_conn.write(data=test_data, destination_blob_name="pakke/ressurs", fmt="csv", metadata=metadata)
        self.assertEqual(public_url, BLOB_PUBLIC_URL)

    @mock.patch("google.cloud.storage.Client", side_effect=MockGoogleClient)
    @mock.patch("google.oauth2.service_account.Credentials", side_effect=MockGoogleCredentials)
    def test_read_valid(self, mock_client, mock_creds):
        storage_conn = GoogleStorageConnector(GCS_BUCKET_NAME, SETTINGS)
        blob = storage_conn.read("blob_name")
        self.assertEqual(blob, BLOB_DATA)

    @mock.patch("google.cloud.storage.Client", side_effect=MockGoogleClient)
    @mock.patch("google.oauth2.service_account.Credentials", side_effect=MockGoogleCredentials)
    def test_get_blob_metadata_valid(self, mock_client, mock_creds):
        storage_conn = GoogleStorageConnector(GCS_BUCKET_NAME, SETTINGS)
        valid_formats = [("markdown", EXPECTED_BLOB_METADATA_MARKDOWN), ("object", EXPECTED_BLOB_METADATA_OBJECT)]
        for metadata_format in valid_formats:
            with self.subTest(msg=f"Testing blob metadata format {metadata_format}", _input=metadata_format):
                blob_metadata = storage_conn.get_blob_metadata("blob_name", metadata_format[0])
                self.assertEqual(blob_metadata, metadata_format[1])

    @mock.patch("google.cloud.storage.Client", side_effect=MockGoogleClient)
    @mock.patch("google.oauth2.service_account.Credentials", side_effect=MockGoogleCredentials)
    def test_get_blob_metadata_invalid_format(self, mock_client, mock_creds):
        storage_conn = GoogleStorageConnector(GCS_BUCKET_NAME, SETTINGS)
        with self.assertRaises(NotImplementedError):
            blob_metadata = storage_conn.get_blob_metadata("blob_name", "not-implemented-format")

    @mock.patch("google.cloud.storage.Client", side_effect=MockGoogleClient)
    @mock.patch("google.oauth2.service_account.Credentials", side_effect=MockGoogleCredentials)
    def test__set_blob_metadata(self, mock_client, mock_creds):
        storage_conn = GoogleStorageConnector(GCS_BUCKET_NAME, SETTINGS)
        blob = storage_conn.bucket.blob("blob_name")
        metadata = {"key1": "value1", "key2": "value2"}
        storage_conn._set_blob_metadata(blob=blob, fmt="csv.gz", metadata=metadata)
        self.assertEqual(blob.content_encoding, "gzip")
        self.assertEqual(blob.cache_control, "no-cache")
        self.assertEqual(blob.metadata, metadata)
