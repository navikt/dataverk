# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from dataverk.utils import settings
from pathlib import Path
from dataverk.utils import resource_discoverer
import os
import json
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

    def test_create_settings_store_normal_case(self):
        res = settings.create_settingsStore(Path("static/testfile_settings.json"), Path("static/.env_test"))
        self.assertEqual(self.test_file_settings_dict, res)

    def test_create_settings_store_CONFIG_PATH_SET(self):
        res = settings.create_settingsStore(Path("static/testfile_settings.json"), {"CONFIG_PATH": "static/"})
        self.assertEqual({**self.test_file_settings_dict, **self.dataverk_secrets_dict}, res)