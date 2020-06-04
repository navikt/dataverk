import unittest
import requests

from unittest import mock
from dataverk.connectors import NaisS3Connector
from tests.dataverk.connectors.storage.test_resources.mock_nais_s3_api import mock_requests_put, mock_requests_get
from tests.dataverk.connectors.storage.test_resources.nais_s3_storage_common import NAIS_S3_ENDPOINT, NAIS_S3_BLOB_NAME, \
    NAIS_S3_RESOURCE_FMT, NAIS_S3_BUCKET_NAME, NAIS_S3_RESOURCE_CONTENT


class TestNaisS3Connector(unittest.TestCase):

    def test_class_instantiation(self):
        s3_conn = NaisS3Connector(NAIS_S3_BUCKET_NAME, NAIS_S3_ENDPOINT)
        self.assertIsInstance(s3_conn, NaisS3Connector)

    @mock.patch("requests.put", side_effect=mock_requests_put)
    def test_write_valid(self, mock_put):
        s3_conn = NaisS3Connector(NAIS_S3_BUCKET_NAME, NAIS_S3_ENDPOINT)
        s3_conn.write(data=NAIS_S3_RESOURCE_CONTENT, destination_blob_name=NAIS_S3_BLOB_NAME, fmt=NAIS_S3_RESOURCE_FMT)

    @mock.patch("requests.get", side_effect=mock_requests_get)
    def test_read_valid(self, mock_get):
        s3_conn = NaisS3Connector(NAIS_S3_BUCKET_NAME, NAIS_S3_ENDPOINT)
        resource = s3_conn.read(blob_name=f"{NAIS_S3_BLOB_NAME}.{NAIS_S3_RESOURCE_FMT}")
        self.assertEqual(resource, NAIS_S3_RESOURCE_CONTENT)

    @mock.patch("requests.get", side_effect=mock_requests_get)
    def test_read_invalid_resource_not_found(self, mock_get):
        s3_conn = NaisS3Connector(NAIS_S3_BUCKET_NAME, NAIS_S3_ENDPOINT)
        with self.assertRaises(requests.exceptions.HTTPError):
            resource = s3_conn.read(blob_name=f"resource/not-found.{NAIS_S3_RESOURCE_FMT}")
