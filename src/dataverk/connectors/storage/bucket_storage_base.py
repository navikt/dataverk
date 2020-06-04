from dataverk.abc.base import DataverkBase


class BucketStorageBase(DataverkBase):
    def __init__(self):
        super().__init__()

    def write(self, data, destination_blob_name: str, fmt: str, **kwargs):
        raise NotImplementedError(
            f"Abstract method. Needs to be implemented in subclass"
        )

    def read(self, blob_name: str, **kwargs):
        raise NotImplementedError(
            f"Abstract method. Needs to be implemented in subclass"
        )
