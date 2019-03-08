from collections.abc import Mapping


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




