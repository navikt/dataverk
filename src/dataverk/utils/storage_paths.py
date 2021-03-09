import os

from dataverk.exceptions.dataverk_exceptions import EnvironmentVariableNotSet


def get_nav_bucket_for_path(metadata: dict) -> str:
    if os.getenv("DATAVERK_BUCKET_SHORT"):
        return os.environ["DATAVERK_BUCKET_SHORT"]
    elif os.getenv("DATAVERK_BUCKET"):
        return os.environ["DATAVERK_BUCKET"]
    elif metadata.get("bucket"):
        return metadata["bucket"]
    else:
        raise AttributeError(f"Bucket is not set in datapackage metadata "
                             f"nor as the DATAVERK_BUCKET or DATAVERK_BUCKET_SHORT environment variables")


def create_nav_paths(dp_id: str, metadata: dict) -> tuple:
    bucket = get_nav_bucket_for_path(metadata)
    try:
        api_endpoint = os.environ["DATAVERK_API_ENDPOINT"]
        path = f'{api_endpoint}/{bucket}/{dp_id}'
    except KeyError as missing_env:
        raise EnvironmentVariableNotSet(str(missing_env))

    try:
        bucket_endpoint = os.environ["DATAVERK_BUCKET_ENDPOINT"]
        store_path = f'{bucket_endpoint}/{bucket}/{dp_id}'
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
