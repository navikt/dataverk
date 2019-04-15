from pathlib import Path
from collections.abc import Mapping
from abc import ABC, abstractmethod
from urllib import parse
from urllib.parse import ParseResult
import json
import hvac


class ValuesImporter(ABC):

    def __init__(self, resource):
        self._resource = resource

    @abstractmethod
    def import_values(self) -> Mapping:
        pass


class FileValuesImporter(ValuesImporter):

    def __init__(self, resource):
        super().__init__(resource)
        self._resource_path = Path(resource)

    def import_values(self) -> Mapping:
        token_value_map = {}
        for value in self._resource_path.iterdir():
            token_value_map.update(self._import_value(value))
        return token_value_map

    @staticmethod
    def _import_value(value):
        token_value_map = {}
        try:
            f = value.open("r")
        except OSError:
            raise Warning(f"Could not open value={value}")
        else:
            token_value_map[value.name] = f.read()
            f.close()
        return token_value_map


class APIValuesImporter(ValuesImporter):

    def __init__(self, resource, mount_point, secrets_path, env_store):
        super().__init__(resource)
        self._mount_point = mount_point
        self._env_store = env_store
        self._secrets_path = secrets_path

    def import_values(self) -> Mapping:
        client = hvac.Client(url=self._resource)
        client.auth.ldap.login(username=self._env_store["USER_IDENT"],
                               password=self._env_store["PASSWORD"],
                               mount_point=self._mount_point)

        values = client.read(path=self._secrets_path)["data"]
        return self._clean_json_string(json.dumps(values))

    def _clean_json_string(self, values_string: str):
        values_string = values_string.replace(r"\n", "").replace(r"ẗ", "").replace(r"\r", "")
        return json.loads(values_string)


class NullValuesImporter(ValuesImporter):

    def import_values(self):
        return {"": ""}


def get_secrets_importer(settings: Mapping, env_store: Mapping) -> ValuesImporter:
    # TODO add null object support
    if env_store.get("SECRETS_FROM_FILES") is not None:
        return FileValuesImporter(resource=settings["secret_path"])
    elif env_store.get("SECRETS_FROM_API") is not None:
        parsed_url = _parse_url(settings["remote_secrets_url"])
        return APIValuesImporter(resource=f"{parsed_url.scheme}://{parsed_url.hostname}:{parsed_url.port}",
                                 mount_point=settings["secrets_auth_method"],
                                 secrets_path=parsed_url.path[1:], env_store=env_store)
    elif env_store.get("DATAVERK_NO_SETTINGS_SECRETS"):
        return NullValuesImporter(resource=None)
    else:
        raise KeyError(f'No secrets sources found')


def _parse_url(url: str) -> ParseResult:
    return parse.urlparse(url)
