
""" Funskjoner som setter sammen og tilgjengeliggjør data definerte konfigurasjoner i dataverk.
"""

from collections.abc import Mapping
from dataverk.utils.settings_builder import SettingsBuilder
from pathlib import Path
import os
import requests
import json


def create_settings_store(settings_file_path: Path, env_store: Mapping) -> Mapping:
    """ Lager et nytt SettingsStore objekt fra en settings.json fil og modifiserer den basert på env variabler.

    :param settings_file_path: Path til settings.json filen
    :param env_store: EnvStore objekt
    :return: Ferdig konfigurert SettingsStore Objekt
    """

    _validate_params(settings_file_path=settings_file_path)
    if env_store is None:
        env_store = {}
    settings_builder = SettingsBuilder(settings_file_path, env_store)
    settings_builder = _set_env_specific_settings(env_store, settings_builder)
    return settings_builder.build()


def _set_env_specific_settings(env_store: Mapping, settings_builder: SettingsBuilder):
    """ Gjør endringer på settings_store gjennom SettingsBuilder basert på felter satt i EnvStore

    :param env_store: EnvStore objekt for å gjøre konfigurasjoner basert på env variabler eller felter i .env filen
    :param settings_builder: SettingsBuilder objektet som håndterer modifikasjoner til settings_store objektet
    :return: SettingsBuilder
    """

    if env_store.get('VKS_SECRET_DEST_PATH') is not None:
        settings_builder.apply(_set_vks_fields)
    elif env_store.get("RUN_FROM_VDI") is not None:
        settings_builder.apply(_set_vdi_fields)
    elif env_store.get("CONFIG_PATH") is not None:
        settings_builder.apply(_set_config_path_fields)
    else:
        settings_builder.apply(_set_travis_fields)
    return settings_builder


def _set_vks_fields(settings_builder: SettingsBuilder) -> None:
    """ Setter nødvendige felt for VKS

    :return: dict med endepunkter og keys
    """
    settings_dict = settings_builder.settings_store

    _field_asserter(settings_dict, "db_connection_strings")
    
    db_connection_strings = settings_dict["db_connection_strings"]
    db_connection_strings["dvh"] = open(settings_builder.env_store['VKS_SECRET_DEST_PATH'] + '/DVH_CONNECTION_STRING',
                                        'r').read()
    db_connection_strings["datalab"] = open(settings_builder.env_store['VKS_SECRET_DEST_PATH'] + '/DATALAB_CONNECTION_STRING',
                                            'r').read()

    bucket_storage_connections = settings_dict["bucket_storage_connections"]
    bucket_storage_connections["AWS_S3"]["access_key"] = open(
        settings_builder.env_store['VKS_SECRET_DEST_PATH'] + '/S3_ACCESS_KEY', 'r').read()
    bucket_storage_connections["AWS_S3"]["secret_key"] = open(
        settings_builder.env_store['VKS_SECRET_DEST_PATH'] + '/S3_SECRET_KEY', 'r').read()
    bucket_storage_connections["google_cloud"]["credentials"]["private_key"] = open(
        settings_builder.env_store['VKS_SECRET_DEST_PATH'] +
        '/GCLOUD_PRIVATE_KEY', 'r').read()


def _set_vdi_fields(settings_builder: SettingsBuilder) -> None:
    """ Setter nødvendige felt for kjøring i VDI miljø

    :return: dict med endepunkter og keys
    """

    settings_store = settings_builder.settings_store

    # asserts that the vault url is set in settings as it is needed to get secrets response
    _field_asserter(settings_store, "vault")
    secrets_response_uri = settings_store["vault"]["secrets_uri"]
    authentication_response_uri = settings_store["vault"]["auth_uri"]

    # Make sure .env file is created, passed to _inint__ and contains fields below
    user_ident = settings_builder.env_store["USER_IDENT"]
    password = settings_builder.env_store["PASSWORD"]

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

    bucket_storage_connections = settings_store["bucket_storage_connections"]

    db_connection_strings = settings_store["db_connection_strings"]

    db_connection_strings["dvh"] = secrets["data"]["DVH_CONNECTION_STRING"]
    db_connection_strings["datalab"] = secrets["data"]["DATALAB_CONNECTION_STRING"]
    bucket_storage_connections["AWS_S3"]["access_key"] = secrets["data"]["S3_ACCESS_KEY"]
    bucket_storage_connections["AWS_S3"]["secret_key"] = secrets["data"]["S3_SECRET_KEY"]
    bucket_storage_connections["google_cloud"]["credentials"]["private_key"] = secrets["data"][
        "GCLOUD_PRIVATE_KEY"]


def _set_config_path_fields(settings_builder: SettingsBuilder) -> None:
    # For testing and running locally

    settings_store = settings_builder.settings_store
    env_store = settings_builder.env_store
    config_path = env_store["CONFIG_PATH"]
    config = {}

    # Hent ut connections dicts, NB: hvis de ikke har blitt lagt til i gjennom __init__() så vil det feile
    bucket_storage_connections = settings_store["bucket_storage_connections"]
    db_connections = settings_store["db_connection_strings"]
    index_connections = settings_store["index_connections"]
    file_storage_connections = settings_store["file_storage_connections"]

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

    settings_store["config"] = config


def _set_travis_fields(settings_builder):
    # Locally or Travis ci

    settings_store = settings_builder.settings_store
    bucket_storage_connections = settings_store["bucket_storage_connections"]
    try:
        path = os.path.join(os.path.dirname(os.getcwd()), 'client-secret.json')
        with open(path) as gcloud_credentials:
            bucket_storage_connections["google_cloud"]["credentials"] = json.load(gcloud_credentials)
    except:
        FileNotFoundError(f" file at path: {path} was not found")


def _field_asserter(store, *fields):
    for field in fields:
        if field not in store:
            raise KeyError(f" {field} not in store {store}")


def _validate_params(settings_file_path: Path):
    if not isinstance(settings_file_path, Path):
        raise TypeError(f"settings_file_path: {settings_file_path} should be a Path object")

    if not settings_file_path.is_file():
        raise FileNotFoundError(f"settings_file_path={settings_file_path} does not resolve to a file")
    if _get_url_suffix(str(settings_file_path)) != "json":
        raise FileNotFoundError(f"settings_file_path={settings_file_path} does not resolve to a json file")


def _get_url_suffix(url:str):
    return url.split(".")[-1]