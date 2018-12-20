# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from dataverk.context import settings
from pathlib import Path
from dataverk.utils import resource_discoverer
import os
import json
import requests
# Common input parameters
# =======================
bad_url_inputs = ("", "testfile_settings.json", 1, object(), [], None)
bad_get_field_inputs = (None, 1, object(), [])


# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """

    def setUp(self):
        self.resource_path = Path(__file__).parent.joinpath("static")
        self.files = resource_discoverer.search_for_files(start_path=self.resource_path,
                                                          file_names=('testfile_settings.json', '.env_test',
                                                                      'dataverk-secrets.json'),
                                                          levels=3)
        self.bad_url_inputs = bad_url_inputs

        self.mock_env = {"PASSWORD": "testpassword", "USER_IDENT": "testident"}

        self.bad_get_field_inputs = bad_get_field_inputs
        self.dataverk_secrets_dict = json.loads(self._read_file(Path(self.files["dataverk-secrets.json"])))
        self.test_file_settings_dict = json.loads(self._read_file(Path(self.files["testfile_settings.json"])))

    def tearDown(self):
        # Clean up env variables after testing
        if "CONFIG_PATH" in os.environ:
            del os.environ["CONFIG_PATH"]

        if "RUN_FROM_VDI" in os.environ:
            del os.environ["RUN_FROM_VDI"]

        if "VKS_SECRET_DEST_PATH" in os.environ:
            del os.environ["RUN_FROM_VDI"]

    def _read_file(self, path: Path):
        with path.open("r") as reader:
            return reader.read()

# Test classes
# ============


class Set(Base):
    """
    Tests all aspects of setting attributes

    Tests include: setting attributes of wrong type, setting attributes outside their constraints, etc.
    """
    pass

    # Set attribute wrong type
    # ========================

    # Set attribute outside constraint
    # ================================


class MethodsInput(Base):
    """
    Tests methods which take input parameters

    Tests include: passing invalid input, etc.
    """
    pass


class MethodsReturnType(Base):
    """
    Tests methods' output types
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_create_singleton_settings_store__normal_case(self):
        res = settings.singleton_settings_store_factory(self.files["testfile_settings.json"], {})
        for key in self.test_file_settings_dict:
            self.assertTrue(key in res, f" key={key} should be in {res}")

    def test_create_singleton_settings_store__object_ref(self):
        res = settings.singleton_settings_store_factory(self.files["testfile_settings.json"], {})
        res2 = settings.singleton_settings_store_factory(self.files["testfile_settings.json"], {})
        self.assertTrue(res is res2, f"{res} should be the same object as {res2}")


    def test_create_settings_store_normal_case(self):
        res = settings.settings_store_factory(self.files["testfile_settings.json"], {})
        for key in self.test_file_settings_dict:
            self.assertTrue(key in res, f" key={key} should be in {res}")

    def test_create_settings_store_CONFIG_PATH_SET(self):
        static_dir = str(self.files["testfile_settings.json"].parent)
        res = settings.singleton_settings_store_factory(self.files["testfile_settings.json"], {"CONFIG_PATH": static_dir})
        test_dict = {**self.test_file_settings_dict, **self.dataverk_secrets_dict}
        for key in test_dict:
            self.assertTrue(key in res, f" key={key} should be in {res}")

    def test_get_field__RUN_FROM_VDI_normal_case(self):
        # Should raise exception when trying to connect to the mock url endpoint
        with self.assertRaises((requests.exceptions.ConnectionError, requests.exceptions.HTTPError)) as cm:
            testObject = settings.settings_store_factory(Path(self.files["testfile_settings.json"]),
                                                        {"RUN_FROM_VDI": "True",
                                                         "USER_IDENT": "testuser",
                                                         "PASSWORD": "testpass"})

