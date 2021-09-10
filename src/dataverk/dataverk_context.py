import json

from dataverk.abc.base import DataverkBase
from requests import HTTPError

from dataverk.context import EnvStore
from dataverk.context import values_importer
from dataverk.context.replacer import Replacer
from dataverk.utils import file_functions


class DataverkContext(DataverkBase):

    def __init__(self, env_store: EnvStore, resource_path: str=".", auth_token: str=None):
        super().__init__()
        self._resource_path = resource_path
        self._http_headers = self._set_http_headers(auth_token)
        self._env_store = env_store
        self._settings_store = self._load_settings()
        self._load_and_apply_secrets()

    @property
    def settings(self):
        return self._settings_store

    def _load_settings(self):
        try:
            return json.loads(file_functions.get_package_resource("settings.json",
                                                                  self._resource_path,
                                                                  self._http_headers))
        except FileNotFoundError:
            return {}

    def _load_and_apply_secrets(self):
        try:
            self._settings_store = self._apply_secrets(self._env_store, self._settings_store)
        except HTTPError:
            self._logger.warning(f"Vault integration not setup for current environment")

    def get_sql_query(self, sql: str):
        return file_functions.get_package_resource(sql, self._resource_path, self._http_headers)

    @staticmethod
    def _set_http_headers(auth_token: str):
        if auth_token is not None:
            return {"Authorization": f"token {auth_token}"}
        else:
            return None

    @staticmethod
    def _apply_secrets(env_store, settings):
        importer = values_importer.get_secrets_importer(settings, env_store)
        replacer = Replacer(importer.import_values())
        return replacer.get_filled_mapping(json.dumps(settings), json.loads)
