def create_nav_paths(dp_id: str) -> tuple:
    path = f'/api/{dp_id}'

    return path, f"/{dp_id}"


def create_gcs_paths(bucket, dp_id) -> tuple:
    path = f'https://storage.googleapis.com/{bucket}/{dp_id}'
    store_path = f'gs://{bucket}/{dp_id}'
    return path, store_path


def create_local_paths(bucket, dp_id) -> tuple:
    path = f'{bucket}/{dp_id}'
    store_path = path
    return path, store_path
