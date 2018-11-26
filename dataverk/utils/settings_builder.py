from collections.abc import Mapping
from pathlib import Path
import json


class SettingsBuilder:
    """ Bygger SettingsStore objektet fra json fil og eventuelle modifiers

    """

    def __init__(self, settings_json_path: Path, env_store: Mapping):
        self._validate_json_file(settings_json_path)
        self._settings_json = settings_json_path
        self._env_store = env_store
        self._mut_settings_store = self._set_settings_data_store(settings_json_path)
        self._set_common_settings_keys()

    @property
    def settings(self):
        return self._mut_settings_store

    @property
    def env_store(self):
        return self._env_store

    def apply(self, modifier):
        modifier(self)

    def build(self) -> Mapping:
        return SettingsStore(self._mut_settings_store)
    
    def _validate_json_file(self, url: Path):
        if not url.is_file():
            raise FileNotFoundError("The provided url does not resolve to a file")
        if self._get_url_suffix(str(url)) != "json":
            raise FileNotFoundError("The provided url does not resolve to a json file")

    def _get_url_suffix(self, url:str):
        return url.split(".")[-1]

    def _set_settings_data_store(self, settings_path):
        return self._json_to_dict(settings_path)

    def _set_common_settings_keys(self):
        common_fields = ("db_connection_strings", "bucket_storage_connections")
        for fields in common_fields:
            self._set_field_if_not_set(fields, {}, self._mut_settings_store)

    def _set_field_if_not_set(self, key, value, store):
        if key not in store:
            store[key] = value

    def _json_to_dict(self, path: Path):
        return json.loads(self._read_file(path))

    def _read_file(self, path: Path):
        with path.open("r") as reader:
            return reader.read()

    def _assert_fields_exist(self, field, *fields):
        if not field in self._mut_settings_store:
            raise KeyError("Field does not exist in Settings data store")
        for field in fields:
            if not field in self._mut_settings_store:
                raise KeyError("Field does not exist in Settings data store")


class SettingsStore(Mapping):
    """ Klassen har ansvar for å gjøre eksterne ressurser tilgjengelige

    """

    def __init__(self, settings_dict: dict):

        self._settings_store = settings_dict

    def __getitem__(self, item):
        if not isinstance(item, str):
            raise ValueError("field should be a str")
        return self._settings_store[item]

    def __iter__(self):
        return self._settings_store.__iter__()

    def __len__(self):
        return len(self._settings_store)

    def __contains__(self, item):
        return item in self._settings_store




