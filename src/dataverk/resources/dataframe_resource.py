import io
import copy
import pandas as pd
from dataverk.utils.file_functions import remove_special_characters

from dataverk.utils import file_functions
from dataverk.resources.base_resource import BaseResource


class DataFrameResource(BaseResource):
    def __init__(
        self,
        resource: pd.DataFrame,
        datapackage_path: str,
        resource_name: str,
        resource_description: str,
        spec: dict = None,
    ):
        super().__init__(resource, datapackage_path, resource_description, spec)

        self._resource_name = resource_name
        self._compress = self._spec.get("compress", True)
        self._fmt = self._spec.get("format", "csv")
        self._schema = self._get_schema()

    def formatted_resource_name(self):
        return remove_special_characters(self._resource_name)

    def _resource_path(self):
        return self._create_resource_path(
            self.formatted_resource_name(),
            self._fmt,
            self._compress,
        )

    def _get_schema(self):
        fields = []

        for name, dtype in zip(self._resource_data.columns, self._resource_data.dtypes):
            if str(dtype) == "object":
                dtype = "string"
            else:
                dtype = "number"

            description = ""
            if self._spec and self._spec.get("fields"):
                spec_fields = self._spec["fields"]
                for field in spec_fields:
                    if field["name"] == name:
                        description = field["description"]

            fields.append({"name": name, "description": description, "type": dtype})

        dsv_separator = self._spec.pop("dsv_separator", ";")
        mediatype = self._media_type(self._fmt)

        return {
            "name": self._resource_name,
            "description": self._resource_description,
            "path": self._resource_path(),
            "format": self._fmt,
            "dsv_separator": dsv_separator,
            "compressed": self._compress,
            "mediatype": mediatype,
            "schema": {"fields": fields},
            "spec": self._spec,
        }

    def add_to_datapackage(self, dp) -> str:
        """ Converts a pandas Dataframe object to csv and adds it to the datapackage

        :param dp: Datapackage object to append resources to
        :return: path: str: Path to resource
        """
        dsv_separator = self._schema.get("dsv_separator")
        dp.datapackage_metadata["resources"].append(self._schema)
        resource = copy.deepcopy(self._schema)

        data_buff = io.StringIO()
        self._resource_data.to_csv(
            data_buff, sep=dsv_separator, index=False, encoding="utf-8-sig"
        )

        if self._compress:
            resource["data"] = file_functions.compress_content(data_buff)
            resource["format"] += ".gz"
        else:
            resource["data"] = data_buff.getvalue()

        dp.resources.append(resource)
        return resource.get("path")
