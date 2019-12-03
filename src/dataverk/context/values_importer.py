from pathlib import Path
from collections.abc import Mapping
from abc import ABC, abstractmethod
from urllib import parse
from urllib.parse import ParseResult
import dataverk_vault.api as vault_api


class ValuesImporter(ABC):

    @abstractmethod
    def import_values(self) -> Mapping:
        pass


class FileValuesImporter(ValuesImporter):

    def __init__(self, resource):
        super().__init__()
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

    def __init__(self):
        super().__init__()

    def import_values(self) -> Mapping:
        return vault_api.read_secrets()


class NullValuesImporter(ValuesImporter):

    def import_values(self):
        return {"": ""}


def get_secrets_importer(settings: Mapping, env_store: Mapping) -> ValuesImporter:
    if env_store.get("DATAVERK_SECRETS_FROM_FILES") is not None:
        return FileValuesImporter(resource=settings["secret_path"])
    elif env_store.get("DATAVERK_SECRETS_FROM_API") is not None:
        return APIValuesImporter()
    else:
        return NullValuesImporter()


def _parse_url(url: str) -> ParseResult:
    return parse.urlparse(url)
