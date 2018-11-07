import os

from dataverk.connectors import GoogleStorageConnector, AWSS3Connector

def publish_google_cloud(dir_path, bucket_name, datapackage_key_prefix):
    conn = GoogleStorageConnector(encrypted=False, bucket=bucket_name)
    conn.upload_blob(os.path.join(dir_path, 'datapackage.json'), datapackage_key_prefix + 'datapackage.json')
    for f in os.listdir(os.path.join(dir_path, 'data')):
        conn.upload_blob(os.path.join(dir_path, 'data', f), datapackage_key_prefix + f)

def publish_s3_nais(dir_path, bucket_name, datapackage_key_prefix):
    conn = AWSS3Connector(encrypted=False, bucket_name=bucket_name)
    conn.upload_from_file(os.path.join(dir_path, 'datapackage.json'), datapackage_key_prefix + 'datapackage.json')
    for f in os.listdir(os.path.join(dir_path, 'data')):
        conn.upload_from_file(os.path.join(dir_path, 'data', f), datapackage_key_prefix + f)