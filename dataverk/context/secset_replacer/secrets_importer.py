from pathlib import Path
from collections.abc import Mapping
from abc import ABC, abstractmethod

import hvac


class SecretsImporter(ABC):

    def __init__(self, resource):
        self._resource = resource

    @abstractmethod
    def import_secrets(self) -> Mapping:
        pass


class FileSecretsImporter(SecretsImporter):

    def __init__(self, resource):
        super().__init__(resource)
        self._resource_path = Path(resource)

    def import_secrets(self) -> Mapping:
        token_value_map = {}
        for secret in self._resource_path.iterdir():
            token_value_map += self._import_secret_value(secret)
        return token_value_map

    @staticmethod
    def _import_secret_value(secret):
        token_value_map = {}
        try:
            f = secret.open("r")
        except OSError:
            raise Warning(f"Could not open secret={secret}")
        else:
            token_value_map[secret.name] = f.read()
            f.close()
        return token_value_map


class APISecretsImporter(SecretsImporter):

    def __init__(self, resource, mount_point, secrets_path, env_store):
        super().__init__(resource)
        self._mount_point = mount_point
        self._env_store = env_store
        self._secrets_path = secrets_path

    def import_secrets(self) -> Mapping:
        client = hvac.Client(url=self._resource)
        client.auth.ldap.login(username=self._env_store["USER_IDENT"],
                               password=self._env_store["PASSWORD"],
                               mount_point=self._mount_point)
        return client.read(path=self._secrets_path)["data"]
