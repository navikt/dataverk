# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import tempfile
import json
from pathlib import Path
from git import Repo

# Common input parameters
# =======================


# Base classes
# ============
class SettingsResourceTestMixin(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.settings_file_dict = {
            "index_connections": {
                "elastic_local": "http://localhost:1010"
            },

            "file_storage_connections": {
                "local": {
                    "path": ".",
                    "bucket": ""
                }
            },

            "bucket_storage_connections": {
                "AWS_S3": {
                    "host": "https://some.test.url.no",
                    "bucket": "default-bucket-test"
                },
                "google_cloud": {
                    "client": "tester-client",
                    "bucket": "default-bucket-test",
                    "credentials": {
                        "type": "test_type",
                        "project_id": "test_id",
                        "private_key_id": "testtestkeyid1010",
                        "client_email": "test@test.tester.com",
                        "client_id": "test1010test",
                        "auth_uri": "https://test/test/oauth2/auth",
                        "token_uri": "https://test.test.com/token",

                        "auth_provider_x509_cert_url": "https://www.test.test/oauth2/v10/certs"
                    }
                }
            },

            "vault": {
                "auth_uri": "https://test.test.no:1010/v12/test/test/test/",
                "secrets_uri": "https://test.test.no:1010/v12/test/test/test/test/test"
            }

        }
        self.tmp_repo = self.create_tmp_repo()
        self.tmp_file_store = self.create_tmp_file()
        self.tmp_repo_git_path = self.tmp_repo.name + "/.git"
        self.tmp_file_store_settings_file_path = self.tmp_file_store.name + "/settings.json"

    def tearDown(self):
        self.tmp_repo.cleanup()
        self.tmp_file_store.cleanup()

    def create_tmp_repo(self) -> tempfile.TemporaryDirectory:
        tmpdir = tempfile.TemporaryDirectory()
        repo = Repo.init(tmpdir.name)
        filepath = Path(tmpdir.name).joinpath("settings.json")

        with filepath.open(mode="w", encoding="utf-8") as file:
            file.write(json.dumps(self.settings_file_dict))
        repo.index.add("*")
        repo.index.commit("initial commit")
        return tmpdir

    def create_tmp_file(self):
        tmpdir = tempfile.TemporaryDirectory()
        json_str = json.dumps(self.settings_file_dict)

        filepath = Path(tmpdir.name).joinpath("settings.json")
        with filepath.open(mode="w", encoding="utf-8") as file:
            file.write(json_str)

        return tmpdir
