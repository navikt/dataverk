# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from dataverk.context import settings
from pathlib import Path
from dataverk.utils import resource_discoverer
from dataverk.utils.windows_safe_tempdir import WindowsSafeTempDirectory
import os
import json
import requests

# Common input parameters
# =======================

SETTINGS_TEST_CONTENT = {
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
    "aws_s3": {
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

ENV_TEST_CONTENT = """PASSWORD=testpassword\nUSER_IDENT=testusername"""

# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """

    def setUp(self):

        self.tempdir = WindowsSafeTempDirectory()
        self.tempdir_path = Path(self.tempdir.name)
        self.test_settings_file_path = self.tempdir_path.joinpath("settings.json")
        self.test_env_file_path = self.tempdir_path.joinpath(".env")

        with self.test_settings_file_path.open("w") as test_settings_file:
            test_settings_file.write(json.dumps(SETTINGS_TEST_CONTENT))

        with self.test_env_file_path.open("w") as test_env_file:
            test_env_file.write(ENV_TEST_CONTENT)

        self.test_resource_files = {
            "settings.json": self.test_settings_file_path,
            ".env": self.test_env_file_path
        }



    def tearDown(self):
        self.tempdir.cleanup()


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_create_env_store(self):
        expected_env_store = {
            "USER_IDENT": "testusername",
            "PASSWORD": "testpassword"
        }

        res = settings._read_envs(self.test_env_file_path)
        for key, val in expected_env_store.items():
            self.assertIn(key, res.keys())
            self.assertIn(val, res.values())
