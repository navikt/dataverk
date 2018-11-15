
# -*- coding: utf-8 -*-
# Import statements
# =================
from unittest import TestCase
from pathlib import Path
from dataverk.utils import resource_discoverer, settings_store
import os
import json
import requests

# Common input parameters
# =======================

bad_url_inputs = ("", "testfile_settings.json", 1, object(), [], None)
bad_get_field_inputs = (None, 1, object(), [])


# Base classes
# ============


class Base(TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.resource_path = Path(__file__).parent.joinpath("static")
        self.files = resource_discoverer.search_for_files(start_path=self.resource_path,
                                                              file_names=('testfile_settings.json', '.env_test', 'dataverk-secrets.json'),
                                                              levels=3)
        self.bad_url_inputs = bad_url_inputs

        self.mock_env = {"PASSWORD": "testpassword", "USER_IDENT": "testident"}
        self.testObject = settings_store.SettingsStore(Path(self.files["testfile_settings.json"]), self.mock_env)
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
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """
    pass

    # Input arguments wrong type
    # ==========================

    def test_init__normal_case(self):
        settings = settings_store.SettingsStore(Path(self.files["testfile_settings.json"]))
        self.assertIsNotNone(settings)

    # Input arguments outside constraints
    # ===================================

    def test_init__wrong_param_type(self):
        for _input in self.bad_url_inputs:
            with self.subTest(_input=_input):
                with self.assertRaises(Exception) as cm:
                    res = settings_store.SettingsStore(_input)


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

    def test_get_field__normal_case(self):
        self.assertEqual(".", self.testObject["file_storage_connections"]["local"]["path"],
                         "Return value should be: .")

    def test_get_field__wrong_param_type(self):
        for _field in self.bad_get_field_inputs:
            with self.subTest(_field=_field):
                with self.assertRaises(ValueError) as cm:
                    res = self.testObject[_field]

class MethodsReturnType(Base):
    """
    Tests methods' output types
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_json_to_dict__normal_case(self):
        self.assertEqual(self.test_file_settings_dict,
                         self.testObject._json_to_dict(path=self.files["testfile_settings.json"]), "The Dicts should be equal")

    def test_get_field__CONFIG_PATH_SET_normal_case(self):
        path = Path()
        os.environ["CONFIG_PATH"] = str(self.files["testfile_settings.json"].parents[0]) #str(path.absolute()) + "/"
        expected_dict = self.dataverk_secrets_dict

        testObject = settings_store.SettingsStore(Path(self.files["testfile_settings.json"]), self.mock_env)
        result = testObject["config"]
        self.assertEqual(expected_dict, result, "The dictionaries should contain the same keys and values")

    def test_get_field__RUN_FROM_VDI_normal_case(self):

        os.environ["RUN_FROM_VDI"] = "True"
        # Should raise exception when trying to connect to the mock url endpoint
        # [TODO] Can we make the VDI settings setup more testable?
        with self.assertRaises(requests.exceptions.ConnectionError) as cm:
            testObject = settings_store.SettingsStore(Path(self.files["testfile_settings.json"]), self.mock_env)

