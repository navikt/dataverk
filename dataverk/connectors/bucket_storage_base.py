from enum import Enum
from abc import ABC
from dataverk.connectors.base import BaseConnector
from collections.abc import Mapping


class BucketStorageConnector(ABC, BaseConnector):

    def __init__(self, settings: Mapping, encrypted=True):
        super().__init__(encrypted=encrypted)
        self.settings = settings

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
