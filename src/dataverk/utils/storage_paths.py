import os

from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet


def _get_nav_bucket_for_path():
    try:
        return os.environ["DATAVERK_BUCKET_SHORT"]
    except KeyError:
        return os.environ["DATAVERK_BUCKET"]


def create_nav_paths(dp_id) -> tuple:
    try:
        api_endpoint = os.environ["DATAVERK_API_ENDPOINT"]
        path = f'{api_endpoint}/{_get_nav_bucket_for_path()}/{dp_id}'
    except KeyError as missing_env:
        raise EnvironmentVariableNotSet(str(missing_env))

    try:
        bucket_endpoint = os.environ["DATAVERK_BUCKET_ENDPOINT"]
        store_path = f'{bucket_endpoint}/{_get_nav_bucket_for_path()}/{dp_id}'
    except KeyError as missing_env:
        raise EnvironmentVariableNotSet(str(missing_env))

    return path, store_path


def create_gcs_paths(bucket, dp_id) -> tuple:
    path = f'https://storage.googleapis.com/{bucket}/{dp_id}'
    store_path = f'gs://{bucket}/{dp_id}'
    return path, store_path


def create_local_paths(bucket, dp_id) -> tuple:
    path = f'{bucket}/{dp_id}'
    store_path = path
    return path, store_path
