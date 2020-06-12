from enum import Enum
from typing import Any

from dataverk.resources.dataframe_resource import DataFrameResource
from dataverk.resources.remote_resource import RemoteResource
from dataverk.resources.pdf_resource import PDFResource


class ResourceType(Enum):
    DF: str = "df"
    REMOTE: str = "remote"
    PDF: str = "pdf"


def get_resource_object(resource_type: str, resource: Any, datapackage_path: str, resource_name: str,
                        resource_description: str, spec: dict):

    if resource_type == ResourceType.DF.value:
        fmt = spec.get('format', 'csv')
        compress = spec.get('compress', True)
        return DataFrameResource(resource=resource, datapackage_path=datapackage_path,
                                 resource_name=resource_name,
                                 resource_description=resource_description,
                                 fmt=fmt, compress=compress, spec=spec)

    elif resource_type == ResourceType.REMOTE.value:
        return RemoteResource(resource=resource, datapackage_path=datapackage_path,
                              resource_description=resource_description,
                              fmt="", compress=False, spec=spec)

    elif resource_type == ResourceType.PDF.value:
        compress = spec.get('compress', False)
        return PDFResource(resource=resource, datapackage_path=datapackage_path,
                           resource_name=resource_name, resource_description=resource_description, fmt="pdf",
                           compress=compress, spec=spec)
    else:
        raise NotImplementedError(
            f"""Resource type {resource_type} is not supported.
             Supported types are {[name.value for name in ResourceType]}.""")
