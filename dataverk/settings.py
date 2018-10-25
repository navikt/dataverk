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

#Bucket storage connections
bucket_storage_connections = {}

bucket_storage_connections["AWS_S3"] = {}
bucket_storage_connections["AWS_S3"]["host"] = "https://s3.nais.preprod.local"
bucket_storage_connections["AWS_S3"]["bucket"] = "default-bucket-nav"

bucket_storage_connections["google_cloud"] = {}
bucket_storage_connections["google_cloud"]["client"] = "nav-datalab"
bucket_storage_connections["google_cloud"]["bucket"] = "default-bucket-nav"
bucket_storage_connections["google_cloud"]["credentials"] = {}
bucket_storage_connections["google_cloud"]["credentials"]["type"] = "service_account"
bucket_storage_connections["google_cloud"]["credentials"]["project_id"] = "nav-datalab"
bucket_storage_connections["google_cloud"]["credentials"]["private_key_id"] = "dde5f22788c09a8736979b4beedd6cf9af962441"
bucket_storage_connections["google_cloud"]["credentials"]["client_email"] = "credentials@nav-datalab.iam.gserviceaccount.com"
bucket_storage_connections["google_cloud"]["credentials"]["client_id"] = "112379116389045910216"
bucket_storage_connections["google_cloud"]["credentials"]["auth_uri"] = "https://accounts.google.com/o/oauth2/auth"
bucket_storage_connections["google_cloud"]["credentials"]["token_uri"] = "https://oauth2.googleapis.com/token"
bucket_storage_connections["google_cloud"]["credentials"]["auth_provider_x509_cert_url"] = "https://www.googleapis.com/oauth2/v1/certs"
bucket_storage_connections["google_cloud"]["credentials"]["client_x509_cert_url"] = "https://www.googleapis.com/robot/v1/metadata/x509/credentials%40nav-datalab.iam.gserviceaccount.com"

if environ.get('DEPLOY_TO_NAIS') is not None:
    db_connection_strings["dvh"] = open(os.getenv('VAULT_SECRETS') + '/DVH_CONNECTION_STRING', 'r').read()
    db_connection_strings["datalab"] = open(os.getenv('VAULT_SECRETS') + '/DATALAB_CONNECTION_STRING', 'r').read()
    bucket_storage_connections["AWS_S3"]["access_key"] = open(os.getenv('VAULT_SECRETS') + '/S3_ACCESS_KEY', 'r').read()
    bucket_storage_connections["AWS_S3"]["secret_key"] = open(os.getenv('VAULT_SECRETS') + '/S3_SECRET_KEY', 'r').read()
    bucket_storage_connections["google_cloud"]["credentials"]["private_key"] = open(os.getenv('VAULT_SECRETS') +
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

elif environ.get("RUN_ON_DESKTOP") is not None:
    config_path = environ.get("CONFIG_PATH")

    import importlib.util

    try:
        spec = importlib.util.spec_from_file_location("config", config_path)
        config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(config)
    except:
        raise ValueError(f'Error loading config from file: {config_path}')

else:
    # Locally or Travis ci
    with open(os.path.join(os.path.dirname(os.getcwd()), 'client-secret.json')) as gcloud_credentials:
        bucket_storage_connections["google_cloud"]["credentials"] = json.load(gcloud_credentials)