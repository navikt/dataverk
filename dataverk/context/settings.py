
""" Funksjoner som setter sammen og tilgjengeliggjør data definerte konfigurasjoner i dataverk.
"""

from collections.abc import Mapping
from dataverk.context.settings_classes import  SettingsStore
from pathlib import Path
from dataverk.utils import file_functions
from .env_store import EnvStore
from dataverk.utils import resource_discoverer
from dataverk.context.secrets_importer import get_secrets_importer
from dataverk_cli.cli.cli_utils.user_message_templates import INFO_TEMPLATE

_settings_store_ref = None  # SettingsStore ref for create_singleton_settings_store()


def singleton_settings_store_factory(settings_file_path: Path=None, env_store: Mapping=None) -> Mapping:
    """ Lager et nytt SettingsStore objekt om et ikke allerede har blitt laget. Hvis et SettingsStore objekt har blitt
    laget returnerer den de istedet.

    :param settings_file_path: Path til settings.json filen
    :param env_store: EnvStore objekt
    :return: Ferdig konfigurert SettingsStore Objekt
    """
    global _settings_store_ref
    if _settings_store_ref is None:
        _settings_store_ref = _create_settings_store(settings_file_path=settings_file_path, env_store=env_store)
    return _settings_store_ref


def _create_settings_store(settings_file_path: Path=None, env_store: Mapping=None):
    """ Creates a new SettingsStore object

    :param settings_file_path: path to settings.json file
    :param env_store: EnvStore object
    :return:
    """
    resource_files = resource_discoverer.search_for_files(start_path=Path("."),
                                                          file_names=('settings.json', '.env'), levels=1)

    settings = _read_settings(resource_files=resource_files, settings_file_path=settings_file_path)

    if env_store is None:
        env_store = _read_envs(resource_files=resource_files)

    settings = _apply_secrets(settings=settings, env_store=env_store)

    return SettingsStore(settings)


def _read_settings(resource_files: Mapping, settings_file_path: Path=None):
    if settings_file_path is None:
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
    try:
        secrets_importer = get_secrets_importer(env_store=env_store)
    except Warning as no_secrets_source:
        print(INFO_TEMPLATE.format(no_secrets_source))
        return settings
    else:
        return secrets_importer.apply_secrets(settings)
