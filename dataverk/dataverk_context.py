import json
from pathlib import Path
from collections import Mapping
from dataverk.context import EnvStore, values_importer
from dataverk.context.replacer import Replacer
from dataverk.utils import file_functions


class DataverkContext:

    def __init__(self, resource_path: str=".", auth_token: str=None):
        self._resource_path = resource_path
        self._settings_store = {}
        self._env_store = EnvStore()
        self._http_headers = self._set_http_headers(auth_token)

    @property
    def settings(self):
        return self._settings_store

    def set_envs_from_file(self, local_env_file: str):
        self._env_store = EnvStore(Path(local_env_file))

    def load_settings(self):
        self._settings_store = json.loads(file_functions.get_package_resource("settings.json", self._resource_path, self._http_headers))

    def load_secrets(self):
        self._try_apply_secrets(self.settings, self._env_store)

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
