import os
from dataverk.connectors.bucket_storage_base import BucketStorageConnector


def upload_to_storage_bucket(dir_path: str, conn: BucketStorageConnector, datapackage_key_prefix: str):
    conn.upload_blob(os.path.join(dir_path, 'datapackage.json'), datapackage_key_prefix + 'datapackage.json')
    for f in os.listdir(os.path.join(dir_path, 'resources')):
        conn.upload_blob(os.path.join(dir_path, 'resources', f), f'{datapackage_key_prefix}resources/{f}')
