import json
import unittest
from pathlib import Path

import urllib3

from dataverk.dataverk import Dataverk
from dataverk.datapackage import Datapackage
from os import environ

from dataverk.utils.windows_safe_tempdir import WindowsSafeTempDirectory

SETTINGS = {
    "package_name": "min-pakke",
    "index_connections": {
    "elastic_host": "https://es-index.no",
    "index": "index-name"
    },

    "bucket_storage_connections": {
    "github": {
      "publish": "True",
      "host": "https://raw.githubusercontent.com"
    }
    },
}

METADATA = {
    "bucket_name": "my-bucket",
    "id": "my-package-id"
}

INVALID_METADATA = {
    "id": "my-package-id"
}

class DataverkTest(unittest.TestCase):

    def setUp(self):
        environ["DATAVERK_NO_SETTINGS_SECRETS"] = "true"

    def test_init(self):
        tempdir = WindowsSafeTempDirectory()
        with Path(tempdir.name).joinpath("settings.json").open("w") as settings_file:
            settings_file.write(json.dumps(SETTINGS))
        with Path(tempdir.name).joinpath("METADATA.json").open("w") as metadata_file:
            metadata_file.write(json.dumps(METADATA))
        dv = Dataverk(tempdir.name)
        datapackage = Datapackage(dv.context.metadata)
        self.assertEqual(dv.context.settings, SETTINGS)
        self.assertTrue("bucket_name" in datapackage.datapackage_metadata)
        self.assertTrue("id" in datapackage.datapackage_metadata)
        tempdir.cleanup()
