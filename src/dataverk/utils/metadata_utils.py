import os
import pandas as pd

from dataverk.connectors.storage.storage_connector_factory import StorageType
from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet
from dataverk.utils.storage_paths import get_nav_bucket_for_path


def is_nav_environment():
    return all(env in os.environ.keys() for env in ["DATAVERK_API_ENDPOINT", "DATAVERK_BUCKET_ENDPOINT"])


def set_nav_config(datapackage_metadata: dict):
    datapackage_metadata["store"] = os.getenv("DATAVERK_STORAGE_SINK", datapackage_metadata.get('store', StorageType.LOCAL))

    try:
        datapackage_metadata["bucket"] = os.getenv("DATAVERK_BUCKET") if os.getenv("DATAVERK_BUCKET") else datapackage_metadata["bucket"]
    except KeyError:
        raise AttributeError(f"Bucket is not set in datapackage metadata "
                             f"nor as the DATAVERK_BUCKET environment variable")

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
