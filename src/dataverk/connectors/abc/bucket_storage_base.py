from abc import ABC
from dataverk.connectors.abc.base import BaseConnector


class BucketStorageConnector(BaseConnector, ABC):

    def __init__(self, encrypted=True):
        super().__init__()

    def write(self, source_string: str, destination_blob_name: str, fmt: str, metadata: dict={}):
        raise NotImplementedError(f'Abstract method. Needs to be implemented in subclass')

    def read(self, blob_name: str):
        raise NotImplementedError(f'Abstract method. Needs to be implemented in subclass')

    def upload_blob(self, source_file_name: str, destination_blob_name: str):
        raise NotImplementedError(f'Abstract method. Needs to be implemented in subclass')

    def delete_blob(self, blob_name: str):
        raise NotImplementedError(f'Abstract method. Needs to be implemented in subclass')

    def download_blob(self, blob_name: str, destination_file_name: str):
        raise NotImplementedError(f'Abstract method. Needs to be implemented in subclass')

    def get_blob_metadata(self, blob_name: str, format: str='markdown'):
        raise NotImplementedError(f'Abstract method. Needs to be implemented in subclass')
