import hvac
from abc import ABC, abstractmethod
from dataverk.context.secset_replacer import finder, replacer
from collections.abc import Mapping
from pathlib import Path


class SecretsImporter(ABC):

    def __init__(self, env_store):
        self._env_store = env_store

    def apply_secrets(self, settings_dict):
        secrets = self._import_secrets(settings_dict)
        secret_replacer = replacer.Replacer(secrets)

        return secret_replacer.get_filled_dict(settings_dict)

    @abstractmethod
    def _import_secrets(self, settings_dict):
        raise NotImplementedError()


class SecretsFromFilesImporter(SecretsImporter):

    def __init__(self, env_store):
        super().__init__(env_store)

    def _import_secrets(self, settings_dict) -> Mapping:
        return finder.FileResourceFinder(Path(settings_dict["secrets_path"]).absolute())


class SecretsFromApiImporter(SecretsImporter):

    def __init__(self, env_store):
        super().__init__(env_store)

    def _import_secrets(self, settings_dict) -> Mapping:
        client = hvac.Client(url=settings_dict["vault_path"])
        client.auth.ldap.login(username=self._env_store["USER_IDENT"],
                               password=self._env_store["PASSWORD"],
                               mount_point=settings_dict["secrets_mount_point"])

        return client.read(path=settings_dict["secrets_path"])["data"]


def get_secrets_importer(env_store: Mapping) -> SecretsImporter:
    if env_store.get("SECRETS_FROM_FILES") is not None:
        return SecretsFromFilesImporter(env_store=env_store)
    elif env_store.get("SECRETS_FROM_API") is not None:
        return SecretsFromApiImporter(env_store=env_store)
    else:
        raise Warning(f'No secrets sources found')

