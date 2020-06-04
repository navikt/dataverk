import io

from google.cloud import exceptions
from tests.dataverk.connectors.storage.test_resources.google_storage_common import GCS_BUCKET_NAME, BLOB_PUBLIC_URL, BLOB_DATA


class MockBucket:

    class ACL:
        def reload(self):
            pass

    class Blob:
        def __init__(self, bucket, name):
            self.id = "id"
            self.size = 1234
            self.name = name
            self.content_encoding = None
            self.cache_control = "no-cache"
            self.metadata = {"key1": "value1", "key2": "value2"}
            self.public_url = BLOB_PUBLIC_URL
            self.bucket = bucket
            self.storage_class = "storage_class"
            self.updated = "2020-05-25"
            self.generation = "generation"
            self.metageneration = "metageneration"
            self.etag = "etag"
            self.owner = "owner"
            self.component_count = "component_count"
            self.crc32c = "crc32c"
            self.md5_hash = "2378965178969djfhkasjs"
            self.content_type = "content_type"
            self.content_disposition = "content_disposition"
            self.content_language = "Norsk"

        def upload_from_string(self, data):
            pass

        def download_to_file(self, string_buffer: io.BytesIO):
            string_buffer.write(BLOB_DATA.encode('utf-8'))

    def __init__(self, name):
        if name != GCS_BUCKET_NAME:
            raise exceptions.NotFound(name)
        self.name = name
        self.acl = MockBucket.ACL()

    def blob(self, name):
        return MockBucket.Blob(self, name)

    def get_blob(self, name):
        return MockBucket.Blob(self, name)


class MockGoogleClient:

    def __init__(self, credentials: dict):
        self._credentials = credentials

    def get_bucket(self, name):
        return MockBucket(name)


class MockGoogleCredentials:

    @staticmethod
    def from_service_account_info(info, scopes):
        return info
