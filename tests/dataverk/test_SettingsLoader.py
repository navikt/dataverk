from unittest import TestCase


# -*- coding: utf-8 -*-
# Import statements
# =================
from dataverk_setup_scripts.settings_loader import SettingsLoader
from pathlib import Path

# Common input parameters
# =======================
actual_settings_url = "https://github.com/navikt/dataverk_settings.git"
bad_github_url = "https://github.com/naataverkttings.git"
empty_url = ""

wrong_type_url_inputs = (2, [], (), object(), object)
bad_urls = ("sa", ",", "", "asdasd.nav.no", "nrk.no")

dataverk_settings_project_name = "dataverk_settings"

test_json_api = "https://jsonplaceholder.typicode.com/todos/1"

# Base classes
# ============


class Base(TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.settings = SettingsLoader(actual_settings_url)


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

    def test__copy_file__normal_case(self):
        self.settings._copy_file(Path(test_json_api), Path(local_temp_dir))

    def test__is_valid_url_string(self):
        print(self.settings._is_valid_url_string("/connectors"))

    def test__download_file_from_online_resource(self):
        self.settings._download_file_from_online_resource(test_json_api, Path(local_temp_dir))


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


    def test__clone_git_repo_return_path_to_settings__normal_case(self):
        settings = SettingsLoader(actual_settings_url)
        tmp_path = Path(local_temp_dir)
        res = settings._clone_git_repo_return_path_to_settings(actual_settings_url, tmp_path)
        self.assertTrue(res.exists())

    def test__get_git_project_name_normal_case(self):
        settings = SettingsLoader(actual_settings_url)
        self.assertEqual(dataverk_settings_project_name, settings._get_git_project_name(actual_settings_url),
                         "Should return the correct project name")