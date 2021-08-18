from abc import ABC, abstractmethod
from typing import Any


class BaseResource(ABC):
    def __init__(
        self,
        resource: Any,
        datapackage_path: str,
        resource_description: str,
        spec: dict = None,
    ):

        self._resource_data = resource
        self._datapackage_path = datapackage_path
        self._resource_description = resource_description
        self._spec = spec

    @property
    @abstractmethod
    def _resource_path(self):
        raise NotImplementedError()

    @property
    @abstractmethod
    def formatted_resource_name(self):
        raise NotImplementedError()

    @abstractmethod
    def _get_schema(self):
        raise NotImplementedError()

    @staticmethod
    def _create_resource_path(
        formatted_resource_name: str, fmt: str, compress: bool
    ):
        if compress:
            return f"{formatted_resource_name}.{fmt}.gz"
        else:
            return f"{formatted_resource_name}.{fmt}"

    @staticmethod
    def _media_type(fmt: str):
        if fmt == "csv":
            return "text/csv"
        elif fmt == "json":
            return "application/json"
        elif fmt == "pdf":
            return "application/pdf"
        else:
            return "text/csv"

    def add_to_datapackage(self, dp):
        raise NotImplementedError()
