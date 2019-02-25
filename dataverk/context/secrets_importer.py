import hvac
from abc import ABC, abstractmethod
from dataverk.context.secset_replacer import finder, replacer
from collections.abc import Mapping
from pathlib import Path


class SecretsImporter(ABC):

    def __init__(self, settings_dict, env_store):
        self._settings_dict = settings_dict
        self._env_store = env_store

    def update_settings(self):
        secrets = self._import_secrets()
        secret_replacer = replacer.Replacer(secrets)

        return secret_replacer.get_filled_dict(self._settings_dict)

    @abstractmethod
    def _import_secrets(self) -> Mapping:
        raise NotImplementedError()


class SecretsFromFileImporter(SecretsImporter):

    def __init__(self, settings_dict, env_store):
        super().__init__(settings_dict, env_store)

    def _import_secrets(self) -> Mapping:
        return finder.FileResourceFinder(Path(self._settings_dict["secrets_path"]).absolute())


class SecretsFromApiImporter(SecretsImporter):

    def __init__(self, settings_dict, env_store):
        super().__init__(settings_dict, env_store)

    def _import_secrets(self) -> Mapping:
        client = hvac.Client(url=self._settings_dict["vault_path"])
        client.auth.ldap.login(
            username=self._env_store["USER_IDENT"],
            password=self._env_store["PASSWORD"],
            mount_point=self._settings_dict["secrets_mount_point"]
        )

        return client.read(path=self._settings_dict["secrets_path"])["data"]

