# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from dataverk_cli.cli.cli_utils import settings_loader
from git import Repo
from git.exc import GitCommandError
import tempfile
from pathlib import Path
import json
from pprint import pprint as pp

# Common input parameters
# =======================


# Base classes
# ============
class Base(unittest.TestCase):
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
        self.tmp_repo_git_path = Path(self.tmp_repo.name).joinpath(".git")
        self.tmp_file_store_settings_file_path = Path(self.tmp_file_store.name).joinpath("settings.json")

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


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """

    def test__clone_git_repo__sanity_check(self):
        settings_loader._get_settings_dict_from_git_repo(self.tmp_repo.name)

    def test_load_settings_file_from_resource__sanity_check(self):
        settings_loader.load_settings_file_from_resource(self.tmp_repo_git_path)


    # Input arguments wrong type
    # ==========================

    # Input arguments outside constraints
    # ===================================


class MethodsReturnType(Base):
    """
    Tests methods' output types
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test__get_settings_dict_from_git_repo__return_dict_is_correct(self):
        result_dict = settings_loader._get_settings_dict_from_git_repo(self.tmp_repo.name)
        self.assertEqual(result_dict, self.settings_file_dict)

    def test__get_settings_dict_from_git_repo__pass_file_url(self):
        settings_file = self.tmp_file_store_settings_file_path
        with self.assertRaises(AttributeError):
            settings_loader._get_settings_dict_from_git_repo(settings_file)

    def test_load_settings_file_from_resource_git_repo(self):
        result_dict = settings_loader.load_settings_file_from_resource(self.tmp_repo_git_path)
        self.assertEqual(result_dict, self.settings_file_dict)

    def test_load_settings_file_from_resource__file_store(self):
        result_dict = settings_loader.load_settings_file_from_resource(self.tmp_file_store_settings_file_path)
        self.assertEqual(result_dict, self.settings_file_dict)