# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from pathlib import Path
from dataverk.utils.settings_builder import SettingsBuilder
from collections.abc import Mapping
import json
from dataverk.utils import resource_discoverer

# Common input parameters
# =======================
bad_type_settings_file_paths = ("", 1, "/", object(), None, {"test": "test"}, ())
bad_value_settings_file_paths = (Path(""), Path("/"), Path("doesnotexist.json"), Path("."))

bad_modifiers_type = (object(), "", 1, (), {})

# Base classes
# ============


class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.bad_type_settings_file_paths = tuple(bad_type_settings_file_paths)
        self.bad_value_settings_file_paths = tuple(bad_value_settings_file_paths)
        self.bad_modifiers_type = tuple(bad_modifiers_type)
        self.files = resource_discoverer.search_for_files(start_path=Path(__file__).parent.joinpath("static"),
                                                          file_names=('testfile_settings.json', '.env'), levels=1)

        self.basic_settings_builder = SettingsBuilder(settings_file_path=self.files['testfile_settings.json'],
                                                      env_store={})
        self.test_file_settings_dict = json.loads(self._read_file(self.files['testfile_settings.json']))

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

    def test_init__bad_settings_file_path_type(self):
        for input_type in self.bad_type_settings_file_paths:
            with self.subTest(msg="Wrong settings_file_path type param in SettingsBuilder", _input=input_type):
                with self.assertRaises(TypeError):
                    SettingsBuilder(settings_file_path=input_type, env_store={"test": "test"})


    # Input arguments outside constraints
    # ===================================
    def test_init__bad_settings_file_path_value(self):
        for input_type in self.bad_value_settings_file_paths:
            with self.subTest(msg="Wrong value for settings_file_path Path param in SettingsBuilder", _input=input_type):
                with self.assertRaises(FileNotFoundError):
                    SettingsBuilder(settings_file_path=input_type, env_store={"test": "test"})


class MethodsInput(Base):
    """
    Tests methods which take input parameters

    Tests include: passing invalid input, etc.
    """

    def test_apply(self):
        for input_type in self.bad_modifiers_type:
            with self.subTest(msg="Wrong type for modifier param in SettingsBuilder.apply()", _input=input_type):
                with self.assertRaises(TypeError):
                    self.basic_settings_builder.apply(input_type)


class MethodsReturnType(Base):
    """
    Tests methods' output types
    """

    def test_build_normal_case(self):
        self.assertTrue(isinstance(self.basic_settings_builder.build(), Mapping))

    def test_settings_store_property_normal_case(self):
        self.assertTrue(isinstance(self.basic_settings_builder.settings_store, Mapping))

    def test_settings_store_property_normal_case(self):
        self.assertTrue(isinstance(self.basic_settings_builder.env_store, Mapping))

    def test_build_normal_case(self):
        self.assertTrue(isinstance(self.basic_settings_builder.build(), Mapping))

class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_build_normal_case(self):
        self.test_file_settings_dict["db_connection_strings"] = {}
        self.test_file_settings_dict["bucket_storage_connections"] = {}
        self.test_file_settings_dict["jenkins"] = {}
        self.test_file_settings_dict["nais-namespace"] = {}
        self.test_file_settings_dict["vault"] = {}
        self.assertEqual(len(self.test_file_settings_dict), len(self.basic_settings_builder.build()))

