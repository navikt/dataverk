import json
import requests

from tests.dataverk.connectors.storage.test_utils.nais_s3_storage_common import NAIS_S3_ENDPOINT, NAIS_S3_BLOB_NAME, \
    NAIS_S3_RESOURCE_FMT, NAIS_S3_BUCKET_NAME, NAIS_S3_RESOURCE_CONTENT


class MockResponse:
    def __init__(self, response_content, status_code):
        self._response_content = response_content
        self._status_code = status_code

    @property
    def status_code(self):
        return self._status_code

    @property
    def text(self):
        return self._response_content

    def raise_for_status(self):
        if self._status_code >= 400:
            raise requests.exceptions.HTTPError(self._status_code)


# Mock method used to replace requests.put
def mock_requests_put(url, **kwargs):
    if url == f"{NAIS_S3_ENDPOINT}/{NAIS_S3_BUCKET_NAME}/{NAIS_S3_BLOB_NAME}.{NAIS_S3_RESOURCE_FMT}":
        return MockResponse({"status": "ok"}, 200)
    else:
        return MockResponse(None, 404)


# Mock method used to replace requests.get
def mock_requests_get(url, **kwargs):
    if url == f"{NAIS_S3_ENDPOINT}/{NAIS_S3_BUCKET_NAME}/{NAIS_S3_BLOB_NAME}.{NAIS_S3_RESOURCE_FMT}":
        return MockResponse(NAIS_S3_RESOURCE_CONTENT, 200)
    else:
        return MockResponse(None, 404)
