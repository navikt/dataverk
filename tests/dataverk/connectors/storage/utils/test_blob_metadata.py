import unittest

from dataverk.connectors.storage.utils import blob_metadata
from tests.dataverk.connectors.storage.test_utils.google_storage_common import GCS_BUCKET_NAME, \
    EXPECTED_BLOB_METADATA_MARKDOWN, EXPECTED_BLOB_METADATA_OBJECT
from tests.dataverk.connectors.storage.test_utils.mock_google_cloud_api import MockBucket


class TestBlobMetadata(unittest.TestCase):

    def test_as_markdown(self):
        bucket = MockBucket(GCS_BUCKET_NAME)
        metadata_as_markdown = blob_metadata.as_markdown(bucket.blob("blob_name"))
        self.assertEqual(metadata_as_markdown, EXPECTED_BLOB_METADATA_MARKDOWN)

    def test_as_json(self):
        bucket = MockBucket(GCS_BUCKET_NAME)
        metadata_as_json = blob_metadata.as_json(bucket.blob("blob_name"))
        self.assertEqual(metadata_as_json, EXPECTED_BLOB_METADATA_OBJECT)
