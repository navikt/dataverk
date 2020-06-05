GCS_BUCKET_NAME = "mybucket"

GOOGLE_SERVICE_ACCOUNT = {
  "type": "service_account",
  "project_id": "project",
  "private_key_id": "123456789",
  "private_key": "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n",
  "client_email": "email@email.email",
  "client_id": "987654321",
  "auth_uri": "https://auth.auth",
  "token_uri": "https://auth.auth/token",
  "auth_provider_x509_cert_url": "https://gooogleapis.com/cert",
  "client_x509_cert_url": "https://www.googleapis.com/cert"
}

SETTINGS = {
    "bucket_storage": {
        "gcs": {
            "credentials": GOOGLE_SERVICE_ACCOUNT
        }
    }
}

BLOB_PUBLIC_URL = "https://googleapis.com/publicUrl"
BLOB_DATA = "data;data;data\n1;2;3"

EXPECTED_BLOB_METADATA_MARKDOWN = """### Dataset:

### Blob:

Blob: _blob_name_

Bucket: _mybucket_

Storage class: _storage_class_

ID: _id_

Size: _1234_

Updated: _2020-05-25_

Generation: _generation_

Metageneration: _metageneration_

Etag: _etag_

Owner: _owner_

Component count: _component_count_

Crc32c: _crc32c_

md5_hash: _2378965178969djfhkasjs_

Cache-control: _no-cache_

Content-type: _content_type_

Content-disposition: _content_disposition_

Content-encoding: _None_

Content-language: _Norsk_

### Metadata:

key1: _value1_

key2: _value2_

"""

EXPECTED_BLOB_METADATA_OBJECT = {
    "bucket": "mybucket",
    "cache_control": "no-cache",
    "component_count": "component_count",
    "content_disposition": "content_disposition",
    "content_encoding": None,
    "content_language": "Norsk",
    "content_type": "content_type",
    "crc32c": "crc32c",
    "etag": "etag",
    "generation": "generation",
    "id": "id",
    "md5_hash": "2378965178969djfhkasjs",
    "metadata": {"key1": "value1", "key2": "value2"},
    "metageneration": "metageneration",
    "name": "blob_name",
    "owner": "owner",
    "public_url": "https://googleapis.com/publicUrl",
    "size": 1234,
    "storage_class": "storage_class",
    "updated": "2020-05-25"
}
