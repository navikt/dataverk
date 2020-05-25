from typing import Mapping
from pathlib import Path
from dataverk.exceptions import dataverk_exceptions
from dataverk.connectors.storage.bucket_storage_base import BucketStorageBase
from dataverk.utils import file_functions
from dataverk.utils import dataverk_doc_address


class FileStorageConnector(BucketStorageBase):
    """File Storage connector"""

    def __init__(self, settings: Mapping):
        super().__init__()

        try:
            self._path = settings["bucket_storage"]["local"]["path"]
        except KeyError:
            raise dataverk_exceptions.IncompleteSettingsObject(
                f"""No path specified for file storage in settings object. 
                See {dataverk_doc_address} for guidelines on how to setup settings file."""
            )

    def write(self, data, destination_blob_name: str, fmt: str, **kwargs) -> None:
        """Write resource to file"""
        file_functions.write_file(
            path=f"{Path(self._path).joinpath(destination_blob_name)}.{fmt}",
            content=data,
            compressed=fmt.endswith("gz"),
        )
        self.log.info(
            f"Resource written to file: {self._path}/{destination_blob_name}.{fmt}"
        )

    def read(self, blob_name: str, **kwargs):
        """Read resource from file"""
        data = file_functions.read_file(f"{Path(self._path).joinpath(blob_name)}")
        self.log.info(f"Resource read from file: {self._path}/{blob_name}")
        return data
