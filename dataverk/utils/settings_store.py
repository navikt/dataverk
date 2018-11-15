from pathlib import Path
import json
from os import environ
import os
import requests
from . import EnvStore

class SettingsStore:
    """ Klassen har ansvar for å gjøre eksterne ressurser tilgjengelige og håndtere forskjellige kjøre miljøer

        The Setting class handles the reading of environments variables to
        differentiate actions between environments like development and
        production. It handles the reading and accessing of settings values
        like connections strings for other parts of the system.

    """

    def __init__(self, settings_json_url: Path, env_store: EnvStore=None):
        self._validate_json_file(settings_json_url)
        self._settings_file_path = settings_json_url
        self._settings_store = {}
        self._env_store = env_store

        # loads data into the setting data store from the settings.json file
        self._set_settings_data_store()

        # sets commonly used setting keys if they are not set already
        self._set_common_settings_keys()

        self._set_enviroment_fields()

    def __getitem__(self, item):
        if not isinstance(item, str):
            raise ValueError("field should be a str")
        self._assert_fields_exist(item)
        return self._settings_store[item]

    def _validate_json_file(self, url: Path):
        if not url.is_file():
            raise FileNotFoundError("The provided url does not resolve to a file")
        if self._get_url_suffix(str(url)) != "json":
            raise FileNotFoundError("The provided url does not resolve to a json file")

    def _get_url_suffix(self, url:str):
        return url.split(".")[-1]

    def _set_settings_data_store(self):
        self._settings_store = self._json_to_dict(self._settings_file_path)

    def _set_common_settings_keys(self):
        common_fields = ("db_connection_strings", "bucket_storage_connections")
        for fields in common_fields:
            self._set_field_if_not_set(fields, {}, self._settings_store)

    def _set_field_if_not_set(self, key, value, store):
        if key not in store:
            store[key] = value

    def _json_to_dict(self, path: Path):
        return json.loads(self._read_file(path))

    def _read_file(self, path: Path):
        with path.open("r") as reader:
            return reader.read()

    def _assert_fields_exist(self, field, *fields):
        if not field in self._settings_store:
            raise KeyError("Field does not exist in Settings data store")
        for field in fields:
            if not field in self._settings_store:
                raise KeyError("Field does not exist in Settings data store")

    def _set_enviroment_fields(self):

        if environ.get('VKS_SECRET_DEST_PATH') is not None:
            self._set_vks_fields()
        elif environ.get("RUN_FROM_VDI") is not None:
            self._set_vdi_fields()
        elif environ.get("CONFIG_PATH") is not None:
            self._set_config_path_fields()
        else:
            self._set_travis_fields()


    def _set_vks_fields(self):
        """ Setter nødvendige felt for VKS

        :return:
        """
        # [TODO] Kanskje ikke nødvendig og sjekke at denne eksisterer?
        self._assert_fields_exist("bucket_storage_connections")

        self._settings_store["db_connection_strings"] = {}
        db_connection_strings = self._settings_store["db_connection_strings"]
        db_connection_strings["dvh"] = open(os.getenv('VKS_SECRET_DEST_PATH') + '/DVH_CONNECTION_STRING',
                                            'r').read()
        db_connection_strings["datalab"] = open(os.getenv('VKS_SECRET_DEST_PATH') + '/DATALAB_CONNECTION_STRING',
                                                'r').read()

        bucket_storage_connections = self._settings_store["bucket_storage_connections"]
        bucket_storage_connections["AWS_S3"]["access_key"] = open(
            os.getenv('VKS_SECRET_DEST_PATH') + '/S3_ACCESS_KEY', 'r').read()
        bucket_storage_connections["AWS_S3"]["secret_key"] = open(
            os.getenv('VKS_SECRET_DEST_PATH') + '/S3_SECRET_KEY', 'r').read()
        bucket_storage_connections["google_cloud"]["credentials"]["private_key"] = open(
            os.getenv('VKS_SECRET_DEST_PATH') +
            '/GCLOUD_PRIVATE_KEY', 'r').read()

    def _set_vdi_fields(self):
        """ Setter nødvendige felt for kjøring i VDI miljø

        :return: Void
        """
        # Get secrets from vault

        # asserts that the vault url is set in settings as it is needed to get secrets response
        self._assert_fields_exist("vault")
        secrets_response_uri = self["vault"]["secrets_uri"]
        authentication_response_uri = self["vault"]["auth_uri"]

        # Make sure .env file is created, passed to _inint__ and contains fields below
        user_ident = self._env_store["USER_IDENT"]
        password = self._env_store["PASSWORD"]

        auth_response = requests.post(url=authentication_response_uri + user_ident,
                                      data=json.dumps({"password": password}))

        if auth_response.status_code != 200:
            auth_response.raise_for_status()

        auth = json.loads(auth_response.text)

        secrets_response = requests.get(url=secrets_response_uri,
                                        headers={"X-Vault-Token": auth["auth"]["client_token"]})
        if secrets_response.status_code != 200:
            secrets_response.raise_for_status()

        secrets = json.loads(secrets_response.text)

        bucket_storage_connections = self._settings_store["bucket_storage_connections"]

        db_connection_strings = self._settings_store["db_connection_strings"]

        db_connection_strings["dvh"] = secrets["data"]["DVH_CONNECTION_STRING"]
        db_connection_strings["datalab"] = secrets["data"]["DATALAB_CONNECTION_STRING"]
        bucket_storage_connections["AWS_S3"]["access_key"] = secrets["data"]["S3_ACCESS_KEY"]
        bucket_storage_connections["AWS_S3"]["secret_key"] = secrets["data"]["S3_SECRET_KEY"]
        bucket_storage_connections["google_cloud"]["credentials"]["private_key"] = secrets["data"][
            "GCLOUD_PRIVATE_KEY"]

    def _set_config_path_fields(self):
        # For testing and running locally
        config_path = environ.get("CONFIG_PATH")
        config = {}

        # Hent ut connections dicts, NB: hvis de ikke har blitt lagt til i gjennom __init__() så vil det feile
        bucket_storage_connections = self._settings_store["bucket_storage_connections"]
        db_connections = self._settings_store["db_connection_strings"]
        index_connections = self["index_connections"]
        file_storage_connections = self["file_storage_connections"]

        with open(os.path.join(config_path, 'dataverk-secrets.json')) as secrets:
            try:
                config = json.load(secrets)
            except:
                raise ValueError(f'Error loading config from file: {config_path}dataverk-secret.json')

        if 'db_connections' in config:
            db_connections = {**db_connections, **config['db_connections']}

        if 'index_connections' in config:
            index_connections = {**index_connections, **config['index_connections']}

        if 'bucket_storage_connections' in config:
            bucket_storage_connections = {**bucket_storage_connections, **config['bucket_storage_connections']}

        if 'file_storage_connections' in config:
            file_storage_connections = {**file_storage_connections, **config['file_storage_connections']}

        self._settings_store["config"] = config

    def _set_travis_fields(self):
        # Locally or Travis ci

        bucket_storage_connections = self._settings_store["bucket_storage_connections"]
        try:
            with open(os.path.join(os.path.dirname(os.getcwd()), 'client-secret.json')) as gcloud_credentials:
                bucket_storage_connections["google_cloud"]["credentials"] = json.load(gcloud_credentials)
        except:
            pass





