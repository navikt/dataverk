import os
from dataverk.connectors.bucket_storage_base import BucketStorageConnector


def upload_to_storage_bucket(dir_path: str, conn: BucketStorageConnector, datapackage_key_prefix: str) -> None:
    ''' Publish data to bucket storage.

    :param dir_path: str: path to directory where generated resources ar located locally
    :param conn: BucketStorageConnector object: the connection object for chosen bucket storage.
                 If no bucket storage connector is configured (conn=None) no resources shall be published to bucket storage
    :param datapackage_key_prefix: str: prefix for datapackage key
    :return: None
    '''
    if conn is not None:
        conn.upload_blob(os.path.join(dir_path, 'datapackage.json'), datapackage_key_prefix + 'datapackage.json')
        for f in os.listdir(os.path.join(dir_path, 'resources')):
            conn.upload_blob(os.path.join(dir_path, 'resources', f), f'{datapackage_key_prefix}resources/{f}')
