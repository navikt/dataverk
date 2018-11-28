# -*- coding: utf-8 -*-
# Import statements
# =================
from unittest import TestCase
from dataverk.utils import validators

# Common input parameters
# =======================

# Base classes
# ============
class Base(TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        pass

    def tearDown(self):
        pass


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

    # Input arguments outside constraints
    # ===================================


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

    # Input arguments valid

    def test_validate_datapackage_name_valid(self):
        valid_datapackage_names = ["name", "name-with-separator"]

        for name in valid_datapackage_names:
            with self.subTest(msg="Valid datapackage name", _input=name):
                validators.validate_datapackage_name(name)

    def test_validate_cronjob_schedule_valid(self):
        valid_schedule_strings = ["* * * * *", "59 * * * *", "* 23 * * *", "* * 31 * *", "* * * 12 *", "* * * * 6",
                                  "0,55 * * * *", "* 0,12 * * *", "* * 1,15 * *", "* * * 1,5 *", "* * * * 0,6"]

        for schedule_string in valid_schedule_strings:
            with self.subTest(msg="Valid schedule string", _input=schedule_string):
                validators.validate_cronjob_schedule(schedule=schedule_string)

    # Input arguments invalid

    def test_validate_datapackage_name_invalid(self):
        invalid_datapackage_names = ["_name", "-name", "name with spaces", "name_", "name-", "Name", "name_with_underscore"]

        for name in invalid_datapackage_names:
            with self.subTest(msg="Invalid datapackage name", _input=name):
                with self.assertRaises(NameError):
                    validators.validate_datapackage_name(name)

    def test_validate_cronjob_schedule_invalid(self):
        invalid_schedule_strings = ["* * * *", "* * * * * *", "60 * * * *", "* 24 * * *",
                                    "* * 32 * *", "* * * 13 *", "* * * * 7"]

        for schedule_string in invalid_schedule_strings:
            with self.subTest(msg="Invalid schedule string", _input=schedule_string):
                with self.assertRaises(ValueError):
                    validators.validate_cronjob_schedule(schedule=schedule_string)


class MethodsReturnType(Base):
    """
    Tests methods' output types
    """
    pass


class MethodsReturnUnits(Base):
    """
    Tests methods' output units where applicable
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """
    pass