
# -*- coding: utf-8 -*-
# Import statements
# =================
from unittest import TestCase
from dataverk import oop_settings
from pathlib import Path
from pprint import pprint as pp


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
        self.bad_url_inputs = bad_url_inputs
        self.testObject = oop_settings.Settings(Path("testfile_settings.json"))
        self.bad_get_field_inputs = bad_get_field_inputs


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
        settings = oop_settings.Settings(Path("testfile_settings.json"))
        self.assertIsNotNone(settings)

    # Input arguments outside constraints
    # ===================================

    def test_init__wrong_param_type(self):
        for _input in self.bad_url_inputs:
            with self.subTest(_input=_input):
                with self.assertRaises(Exception) as cm:
                    res = oop_settings.Settings(_input)


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
        self.assertEqual(".", self.testObject.get_field("file_storage_connections")["local"]["path"],
                         "Return value should be: .")

    def test_get_field__wrong_param_type(self):
        for _field in self.bad_get_field_inputs:
            with self.subTest(_field=_field):
                with self.assertRaises(ValueError) as cm:
                    res = self.testObject.get_field(_field)

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
        pp(self.testObject._json_to_dict())