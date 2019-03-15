
""" Funksjoner som setter sammen og tilgjengeliggjør data definerte konfigurasjoner i dataverk.
"""
import json
from collections.abc import Mapping
from dataverk.context.settings_classes import SettingsStore
from pathlib import Path
from dataverk.utils import file_functions
from .env_store import EnvStore
from dataverk.utils import resource_discoverer
from dataverk.context import values_importer
from dataverk.context.replacer import Replacer

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
        resource_files = resource_discoverer.search_for_files(start_path=Path("."),
                                                              file_names=('settings.json', '.env'))
        _settings_store_ref = _create_settings_store(resource_files)
    return _settings_store_ref


def _create_settings_store(resource_files):
    """ Creates a new SettingsStore object

    :param settings_file_path: path to settings.json file
    :param env_store: EnvStore object
    :return:
    """
    settings = _read_settings(resource_files["settings.json"])
    env_store = _read_envs(resource_files.get(".env"))
    settings = _try_apply_secrets(settings=settings, env_store=env_store)

    return SettingsStore(settings)


def _read_settings(settings_file_path: Path):
    return file_functions.json_to_dict(settings_file_path)


def _read_envs(env_path):
    return EnvStore(env_path)


def _try_apply_secrets(settings: Mapping, env_store: Mapping):
    return _apply_secrets(env_store, settings)


def _apply_secrets(env_store, settings):
    importer = values_importer.get_secrets_importer(settings, env_store)
    replacer = Replacer(importer.import_values())
    return replacer.get_filled_mapping(json.dumps(settings), json.loads)
