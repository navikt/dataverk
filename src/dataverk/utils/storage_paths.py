import os

from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet


def create_nais_paths(bucket, dp_id) -> tuple:
    try:
        api_endpoint = os.environ["DATAVERK_API_ENDPOINT"]
    except KeyError as missing_env:
        raise EnvironmentVariableNotSet(str(missing_env))
    else:
        path = f'{api_endpoint}/{bucket}/{dp_id}'

    try:
        bucket_endpoint = os.environ["DATAVERK_BUCKET_ENDPOINT"]
    except KeyError as missing_env:
        raise EnvironmentVariableNotSet(str(missing_env))
    else:
        store_path = f'{bucket_endpoint}/{bucket}/{dp_id}'

    return path, store_path


def create_gcs_paths(bucket, dp_id) -> tuple:
    path = f'https://storage.googleapis.com/{bucket}/{dp_id}'
    store_path = f'gs://{bucket}/{dp_id}'
    return path, store_path


def create_local_paths(bucket, dp_id) -> tuple:
    path = f'{bucket}/{dp_id}'
    store_path = path
    return path, store_path
