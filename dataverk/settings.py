import os
import sys
import json
import getpass
import requests

from os import environ

dataverk_path = os.path.dirname(sys.modules['dataverk'].__file__)
root_path = os.path.abspath(os.path.join(dataverk_path, os.pardir))
env_path = f'{root_path}/.env'

# set env variables listed in .env file
if os.path.isfile(env_path):

    with open(f'{root_path}/.env') as f:
        envs = f.readlines()

    for env in envs:
        env = env.strip()
        if env[0]=='#': 
            continue

        var = env.split('=')
        os.environ[var[0]] = var[1]

# Database connections
db_connection_strings = {}

# Index connections
index_connections = {
    "elastic_local": "http://localhost:9200"
}

# File storage
file_storage_connections = {
    "local": {
        "path": ".",
        "bucket": ""
    }
}

#Bucket storage connections
bucket_storage_connections = {
    "AWS_S3": {
        "host": "https://s3.nais.preprod.local",
        "bucket": "default-bucket-nav"
    },
    "google_cloud": {
        "client": "nav-datalab",
        "bucket": "default-bucket-nav",
        "credentials": {
            "type": "service_account",
            "project_id": "nav-datalab",
            "private_key_id": "dde5f22788c09a8736979b4beedd6cf9af962441",
            "client_email": "credentials@nav-datalab.iam.gserviceaccount.com",
            "client_id": "112379116389045910216",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",

            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs"
        }
    }
}

if environ.get('VKS_SECRET_DEST_PATH') is not None:
    db_connection_strings["dvh"] = open(os.getenv('VKS_SECRET_DEST_PATH') + '/DVH_CONNECTION_STRING', 'r').read()
    db_connection_strings["datalab"] = open(os.getenv('VKS_SECRET_DEST_PATH') + '/DATALAB_CONNECTION_STRING', 'r').read()
    bucket_storage_connections["AWS_S3"]["access_key"] = open(os.getenv('VKS_SECRET_DEST_PATH') + '/S3_ACCESS_KEY', 'r').read()
    bucket_storage_connections["AWS_S3"]["secret_key"] = open(os.getenv('VKS_SECRET_DEST_PATH') + '/S3_SECRET_KEY', 'r').read()
    bucket_storage_connections["google_cloud"]["credentials"]["private_key"] = open(os.getenv('VKS_SECRET_DEST_PATH') +
                                                                                    '/GCLOUD_PRIVATE_KEY', 'r').read()
elif environ.get("RUN_FROM_VDI") is not None:
    # Get secrets from vault
    user_ident = input('Enter user ident: ')
    password = getpass.getpass(prompt="Enter password: ")

    auth_response = requests.post(url='https://vault.adeo.no:8200/v1/auth/ldap/login/' + user_ident,
                                  data=json.dumps({"password": password}))
    if auth_response.status_code != 200:
        auth_response.raise_for_status()

    auth = json.loads(auth_response.text)

    secrets_response = requests.get(url="https://vault.adeo.no:8200/v1/kv/prod/fss/datasett/opendata",
                                    headers={"X-Vault-Token": auth["auth"]["client_token"]})
    if secrets_response.status_code != 200:
        secrets_response.raise_for_status()

    secrets = json.loads(secrets_response.text)

    db_connection_strings["dvh"] = secrets["data"]["DVH_CONNECTION_STRING"]
    db_connection_strings["datalab"] = secrets["data"]["DATALAB_CONNECTION_STRING"]
    bucket_storage_connections["AWS_S3"]["access_key"] = secrets["data"]["S3_ACCESS_KEY"]
    bucket_storage_connections["AWS_S3"]["secret_key"] = secrets["data"]["S3_SECRET_KEY"]
    bucket_storage_connections["google_cloud"]["credentials"]["private_key"] = secrets["data"]["GCLOUD_PRIVATE_KEY"]

elif environ.get("CONFIG_PATH") is not None:
    # For testing and running locally
    config_path = environ.get("CONFIG_PATH")
    config = {}

    with open(os.path.join(config_path, 'dataverk-secrets.json')) as secrets:
        try:
            config = json.load(secrets)
        except:
            raise IOError(f'Error loading config from file: {config_path}dataverk-secret.json')

    if 'db_connections' in config:
        db_connection_strings = {**db_connection_strings, **config['db_connection_strings']}

    if 'index_connections' in config:
        index_connections = {**index_connections, **config['index_connections']}

    if 'bucket_storage_connections' in config:
        bucket_storage_connections = {**bucket_storage_connections, **config['bucket_storage_connections']}

    if 'file_storage_connections' in config:
        file_storage_connections = {**file_storage_connections, **config['file_storage_connections']}
 
else:
    # Locally or Travis ci
    try: 
        with open(os.path.join(os.path.dirname(os.getcwd()), 'client-secret.json')) as gcloud_credentials:
            bucket_storage_connections["google_cloud"]["credentials"] = json.load(gcloud_credentials)
    except:
        pass