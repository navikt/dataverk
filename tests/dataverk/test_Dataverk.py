import json
import unittest
from pathlib import Path

from dataverk.dataverk import Dataverk
from os import environ

from tests.windows_safe_tempdir import WindowsSafeTempDirectory

SETTINGS = {
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
        self.assertEqual(dv.context.settings, SETTINGS)
        tempdir.cleanup()

    def test__is_sql_file(self):
        sql_path = "./myquery.sql"
        sql_query = "SELECT * FROM mytable"
        self.assertEqual(Dataverk._is_sql_file(source=sql_path), True)
        self.assertEqual(Dataverk._is_sql_file(source=sql_query), False)
