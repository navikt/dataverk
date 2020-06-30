from enum import Enum
from typing import Any


from dataverk.resources.dataframe_resource import DataFrameResource
from dataverk.resources.json_resource import JSONResource
from dataverk.resources.remote_resource import RemoteResource
from dataverk.resources.pdf_resource import PDFResource


class ResourceType(Enum):
    DF: str = "df"
    REMOTE: str = "remote"
    PDF: str = "pdf"
    JSON: str = "json"


def get_resource_object(
    resource_type: str,
    resource: Any,
    datapackage_path: str,
    resource_name: str,
    resource_description: str,
    spec: dict,
):

    if not spec:
        spec = {}

    if resource_type == ResourceType.DF.value:
        return DataFrameResource(
            resource=resource,
            datapackage_path=datapackage_path,
            resource_name=resource_name,
            resource_description=resource_description,
            spec=spec,
        )

    elif resource_type == ResourceType.REMOTE.value:
        return RemoteResource(
            resource=resource,
            datapackage_path=datapackage_path,
            resource_description=resource_description,
            spec=spec,
        )

    elif resource_type == ResourceType.PDF.value:
        return PDFResource(
            resource=resource,
            datapackage_path=datapackage_path,
            resource_name=resource_name,
            resource_description=resource_description,
            spec=spec,
        )
    elif resource_type == ResourceType.JSON.value:
        return JSONResource(
            resource=resource,
            datapackage_path=datapackage_path,
            resource_name=resource_name,
            resource_description=resource_description,
            spec=spec,
        )
    else:
        raise NotImplementedError(
            f"""Resource type {resource_type} is not supported.
             Supported types are {[name.value for name in ResourceType]}."""
        )
