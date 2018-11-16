import os
from dataverk.connectors import GoogleStorageConnector, AWSS3Connector
from dataverk import SettingsStore

def publish_google_cloud(dir_path, datapackage_key_prefix, settings: SettingsStore):
    conn = GoogleStorageConnector(encrypted=False, settings=settings)
    conn.upload_blob(os.path.join(dir_path, 'datapackage.json'), datapackage_key_prefix + 'datapackage.json')
    for f in os.listdir(os.path.join(dir_path, 'data')):
        conn.upload_blob(os.path.join(dir_path, 'data', f), datapackage_key_prefix + f)

def publish_s3_nais(dir_path, datapackage_key_prefix, settings: SettingsStore):
    conn = AWSS3Connector(encrypted=False, settings=settings)
    conn.upload_from_file(os.path.join(dir_path, 'datapackage.json'), datapackage_key_prefix + 'datapackage.json')
    for f in os.listdir(os.path.join(dir_path, 'data')):
        conn.upload_from_file(os.path.join(dir_path, 'data', f), datapackage_key_prefix + f)