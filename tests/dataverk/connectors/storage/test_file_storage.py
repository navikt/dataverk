import shutil
import tempfile
import unittest

from pathlib import Path
from dataverk.exceptions import dataverk_exceptions
from dataverk.connectors.storage.file_storage import FileStorageConnector


class TestFileStorageConnector(unittest.TestCase):

    def setUp(self):
        self.storage_dir = tempfile.mkdtemp()
        self.settings = {
            "bucket_storage": {
                "local": {
                    "path": f"{self.storage_dir}"
                }
            }
        }

    def tearDown(self):
        shutil.rmtree(self.storage_dir)

    def test_class_instantiation_valid(self):
        storage_conn = FileStorageConnector(self.settings)
        self.assertIsInstance(storage_conn, FileStorageConnector)

    def test_class_instantiation_invalid_missing_storage_path(self):
        with self.assertRaises(dataverk_exceptions.IncompleteSettingsObject):
            storage_conn = FileStorageConnector({})

    def test_write_valid(self):
        content = "data\ndata\ndata"
        storage_conn = FileStorageConnector(self.settings)
        storage_conn.write(data=content, destination_blob_name=f"{Path('pakkeid').joinpath('ressurs')}", fmt="txt")

    def test_write_read_roundtrip_valid(self):
        data_write = "data\ndata\ndata"
        storage_conn = FileStorageConnector(self.settings)
        storage_conn.write(data=data_write, destination_blob_name=f"{Path('pakkeid').joinpath('ressurs')}", fmt="txt")
        data_read = storage_conn.read(f"{Path('pakkeid').joinpath('ressurs')}.txt")
        self.assertEqual(data_read, data_write)
