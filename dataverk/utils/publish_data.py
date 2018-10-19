import os

from dataverk.connectors import GoogleStorageConnector, AWSS3Connector


def publish_google_cloud(dir_path, datapackage_name):
    conn = GoogleStorageConnector(encrypted=False, bucket=datapackage_name)
    conn.upload_blob(os.path.join(dir_path, 'datapackage.json'), datapackage_name + '/datapackage.json')
    for f in os.listdir(os.path.join(dir_path, 'data')):
        conn.upload_blob(os.path.join(dir_path, 'data', f), datapackage_name + '/data/' + f)

def publish_s3_nais(dir_path, datapackage_name):
    conn = AWSS3Connector(encrypted=False, bucket_name=datapackage_name)
    conn.upload_from_file(os.path.join(dir_path, 'datapackage.json'), datapackage_name + '/datapackage.json')
    for f in os.listdir(os.path.join(dir_path, 'data')):
        conn.upload_from_file(os.path.join(dir_path, 'data', f), datapackage_name + '/data/' + f)