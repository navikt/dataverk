import os
import pandas as pd

from dataverk.connectors.storage.storage_connector_factory import StorageType
from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet
from dataverk.utils.storage_paths import get_nav_bucket_for_path


def is_nav_environment():
    return all(env in os.environ.keys() for env in ["DATAVERK_API_ENDPOINT", "DATAVERK_BUCKET_ENDPOINT"])


def set_nav_config(datapackage_metadata: dict):
    datapackage_metadata["store"] = os.getenv("DATAVERK_STORAGE_SINK", datapackage_metadata.get('store', StorageType.LOCAL))

    if "bucket" not in datapackage_metadata.keys():
        try:
            datapackage_metadata["bucket"] = os.environ["DATAVERK_BUCKET"]
        except KeyError as missing:
            raise EnvironmentVariableNotSet(str(missing))

    datapackage_metadata["path"] = f"{os.environ['DATAVERK_API_ENDPOINT']}/" \
                                   f"{get_nav_bucket_for_path(datapackage_metadata)}/" \
                                   f"{datapackage_metadata['id']}"


def get_schema(df: pd.DataFrame, path: str, dataset_name: str, format: str, dsv_separator: str):
    fields = []

    for name, dtype in zip(df.columns, df.dtypes):
        # TODO : Bool and others
        if str(dtype) == 'object':
            dtype = 'string'
        else:
            dtype = 'number'

        fields.append({'name': name, 'description': '', 'type': dtype})

    if format == 'csv':
        mediatype = 'text/csv'
    elif format == 'json':
        mediatype = 'application/json'
    else:
        mediatype = 'text/csv'

    return {
        'name': dataset_name,
        'path': f'{path}/resources/{dataset_name}.{format}',
        'format': format,
        'separator': dsv_separator,
        'mediatype': mediatype,
        'schema': {'fields': fields}
    }
