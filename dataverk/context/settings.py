
""" Funksjoner som setter sammen og tilgjengeliggjør data definerte konfigurasjoner i dataverk.
"""

from collections.abc import Mapping
from dataverk.context.settings_classes import  SettingsStore
from pathlib import Path
from dataverk.utils import file_functions
from .env_store import EnvStore
from dataverk.utils import resource_discoverer
from dataverk.context.secset_replacer.secrets_importer import APISecretsImporter, FileSecretsImporter, SecretsImporter
from dataverk.context.secset_replacer.replacer import Replacer

_settings_store_ref = None  # SettingsStore ref for create_singleton_settings_store()


def singleton_settings_store_factory() -> Mapping:
    """ Lager et nytt SettingsStore objekt om et ikke allerede har blitt laget. Hvis et SettingsStore objekt har blitt
    laget returnerer den de istedet.

    :param settings_file_path: Path til settings.json filen
    :param env_store: EnvStore objekt
    :return: Ferdig konfigurert SettingsStore Objekt
    """
    global _settings_store_ref
    if _settings_store_ref is None:
        _settings_store_ref = _create_settings_store()
    return _settings_store_ref


def _create_settings_store():
    """ Creates a new SettingsStore object

    :param settings_file_path: path to settings.json file
    :param env_store: EnvStore object
    :return:
    """
    resource_files = resource_discoverer.search_for_files(start_path=Path("."),
                                                          file_names=('settings.json', '.env'), levels=1)

    settings = _read_settings(resource_files=resource_files)
    env_store = _read_envs(resource_files=resource_files)
    settings = _apply_secrets(settings=settings, env_store=env_store)

    return SettingsStore(settings)


def _read_settings(resource_files: Mapping):
    settings_file_path = resource_files['settings.json']

    return file_functions.json_to_dict(settings_file_path)


def _read_envs(resource_files: Mapping):
    env_path = resource_files.get(".env")
    if env_path is not None:
        env_store = EnvStore(env_path)
    else:
        env_store = EnvStore()

    return env_store


def _apply_secrets(settings: Mapping, env_store: Mapping):
    importer = get_secrets_importer(settings, env_store)
    replacer = Replacer(importer=importer)
    return replacer.get_filled_mapping(str(settings))


def get_secrets_importer(settings: Mapping, env_store: Mapping) -> SecretsImporter:
    if env_store.get("SECRETS_FROM_FILES") is not None:
        return FileSecretsImporter(resource=settings["pod_secret_mount_path"])
    elif env_store.get("SECRETS_FROM_API") is not None:
        return APISecretsImporter(resource=settings["secrets_url_path"], mount_point=settings["secrets_mount_path"],
                                  secrets_path=settings["remote_secrets_path"], env_store=env_store)
    else:
        raise Warning(f'No secrets sources found')
