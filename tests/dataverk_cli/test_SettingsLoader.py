from unittest import TestCase


# -*- coding: utf-8 -*-
# Import statements
# =================
from dataverk_cli.cli_utils.settings_loader import SettingsLoader
from pathlib import Path

# Common input parameters
# =======================
actual_settings_url = "https://github.com/navikt/dataverk_settings.git"
bad_github_url = "https://github.com/naataverkttings.git"
empty_url = ""
test_dir = Path(__file__).parent

wrong_type_url_inputs = (2, [], (), object(), object)
bad_urls = ("sa", ",", "", "asdasd.nav.no", "nrk.no")

dataverk_settings_project_name = "dataverk_settings"

test_json_api = "https://jsonplaceholder.typicode.com/todos/1"
testfile_settings = "testfile_settings.json"

# Base classes
# ============


class Base(TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.settings_loader = SettingsLoader(actual_settings_url)
        self.local_test_dir = test_dir


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """

    def test_init_normal(self):
        settings = SettingsLoader(actual_settings_url)

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

    def test__is_valid_url_string(self):
        self.assertTrue(self.settings_loader._is_valid_url_string("test.no"))


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
