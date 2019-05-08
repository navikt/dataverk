import json
from collections import Mapping
from dataverk.context import EnvStore
from dataverk.context import values_importer
from dataverk.context.replacer import Replacer
from dataverk.utils import file_functions


class DataverkContext:

    def __init__(self, env_store: EnvStore, resource_path: str=".", auth_token: str=None):
        self._resource_path = resource_path
        self._http_headers = self._set_http_headers(auth_token)
        self._env_store = env_store
        self._settings_store = self._load_settings()
        self._load_and_apply_secrets()

    @property
    def settings(self):
        return self._settings_store

    def _load_settings(self):
        return json.loads(file_functions.get_package_resource("settings.json", self._resource_path, self._http_headers))

    def _load_and_apply_secrets(self):
        self._settings_store = self._try_apply_secrets(self._settings_store, self._env_store)

    def get_sql_query(self, sql: str):
        return file_functions.get_package_resource(sql, self._resource_path, self._http_headers)

    @staticmethod
    def _set_http_headers(auth_token: str):
        if auth_token is not None:
            return {"Authorization": f"token {auth_token}"}
        else:
            return None

    def _try_apply_secrets(self, settings: Mapping, env_store: Mapping):
        return self._apply_secrets(env_store, settings)

    @staticmethod
    def _apply_secrets(env_store, settings):
        importer = values_importer.get_secrets_importer(settings, env_store)
        replacer = Replacer(importer.import_values())
        return replacer.get_filled_mapping(json.dumps(settings), json.loads)
