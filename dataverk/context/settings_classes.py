from collections.abc import Mapping
from pathlib import Path
import json
from typing import Callable

COMMON_FIELDS = ("db_connection_strings", "bucket_storage_connections", "jenkins", "vault", "nais_namespace")


class SettingsBuilder:
    """ Bygger SettingsStore objektet fra json fil og tilgjenngeliggjør modifikasjon gjennom apply() metoden.

    """

    def __init__(self, settings: Mapping, env_store: Mapping=None):



        if env_store is None:
            env_store = {}
        self._env_store = env_store
        self._mut_settings_store = settings
        self._set_common_settings_keys()

    @property
    def settings_store(self):
        return self._mut_settings_store

    @property
    def env_store(self):
        return self._env_store

    def apply(self, modifier):
        """ public metode som gir eksterne funskjoner tilgang til å endre, berike og/eller fjerne felter i settings_store
        """

        if not isinstance(modifier, Callable):
            raise TypeError(f"modifier: {modifier} must be callable")
        modifier(self)

    def build(self) -> Mapping:
        return self._mut_settings_store

    def _set_common_settings_keys(self):

        for fields in COMMON_FIELDS:
            self._set_field_if_not_set(fields, {}, self._mut_settings_store)

    def _set_field_if_not_set(self, key, value, store):
        if key not in store:
            store[key] = value

    def _assert_fields_exist(self, field, *fields):
        if not field in self._mut_settings_store:
            raise KeyError("Field does not exist in Settings data store")
        for field in fields:
            if not field in self._mut_settings_store:
                raise KeyError("Field does not exist in Settings data store")

    def _assert_params(self, env_store, settings):
        if not isinstance(env_store, Mapping):
            raise TypeError(f"env_store({env_store}) should be of type Mapping")
        if not isinstance(settings, Mapping):
            raise TypeError(f"settings({settings}) should be of type Mapping")


class SettingsStore(Mapping):
    """ Klassen har ansvar for å gjøre settings som eksterne URLer, keys, flagg og andre ressurser tilgjengelige

    """

    def __init__(self, settings_dict: Mapping):
        if not isinstance(settings_dict, Mapping):
            raise TypeError(f"param settings_dict={settings_dict} has to be Mapping")
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

    def get(self, key):
        if not isinstance(key, str):
            raise ValueError("field should be a str")
        return self._settings_store.get(key)

    def keys(self):
        return self._settings_store.keys()

    def values(self):
        return self._settings_store.values()

    def items(self):
        return self._settings_store.items()




