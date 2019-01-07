# -*- coding: utf-8 -*-
# Import statements
# =================
from unittest import TestCase
from dataverk_cli.cli_utils.settings_creator import get_settings_creator
from argparse import Namespace

# Common input parameters
# =======================
OPTIONAL_PARAMETERS = Namespace(prompt_missing_args=True, package_name="min-pakke", update_schedule="* * 31 2 *",
                                nais_namespace="opendata", elastic_private="https://es.endpoint", aws_endpoint="https://aws.endpoint",
                                jenkins_endpoint="https://jenkins.endpoint", vault_secrets_uri="https://vault.secrets.uri",
                                vault_auth_path="vault/auth/path", vault_kv_path="vault/kv/path", vault_role="vault_role",
                                vault_service_account="vault_service_account")


# Base classes
# ============
class Base(TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.settings_creator_object = get_settings_creator(args=OPTIONAL_PARAMETERS)
        self.settings = self.settings_creator_object.create_settings()

    def tearDown(self):
        pass


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """

    # Correct instanciation
    def test_correct_instanciation_without_default_settingsfile(self):
        args = OPTIONAL_PARAMETERS
        settings_creator = get_settings_creator(args=args)
        settings = settings_creator.create_settings()

    # Input arguments wrong type
    # ==========================
    def test_invalid_factory_prompt_not_set_and_no_settingsfile(self):
        args = Namespace(prompt_missing_args=False)
        with self.assertRaises(FileNotFoundError):
            get_settings_creator(args=args, default_settings_path=None)

    def test_invalid_factory_prompt_not_set_and_invalid_settingsfile_path_type(self):
        args = Namespace(prompt_missing_args=False)
        invalid_settingsfile_paths = [False, 123, dict(), tuple(), list()]
        for invalid_settingsfile_path in invalid_settingsfile_paths:
            with self.subTest(msg="Invalid settingsfile path type", _input=invalid_settingsfile_path):
                with self.assertRaises(TypeError):
                    get_settings_creator(args=args, default_settings_path=invalid_settingsfile_path)

    def test_invalid_instaciation_prompt_not_set_and_no_existing_settingsfile(self):
        args = Namespace(prompt_missing_args=False)
        with self.assertRaises(OSError):
            get_settings_creator(args=args, default_settings_path=".")

        # Input arguments outside constraints
    # ===================================


class Set(Base):
    """
    Tests all aspects of setting attributes

    Tests include: setting attributes of wrong type, setting attributes outside their constraints, etc.
    """

    # Set attributes correct type
    # ===========================
    def test__set_settings_param_valid(self):
        self.settings_creator_object._set_settings_param(("index_connections", "elastic_private"), "https://my.es.index")
        self.assertEqual(self.settings_creator_object.settings["index_connections"]["elastic_private"], "https://my.es.index")

    # Set attribute wrong type
    # ========================
    def test__set_settings_param_invalid_type(self):
        invalid_param_types = [False, 123, dict(), list(), tuple()]

        for invalid_param_type in invalid_param_types:
            with self.subTest(msg="Test invalid param type for settings object creator", _input=invalid_param_type):
                with self.assertRaises(TypeError):
                    self.settings_creator_object._set_settings_param(("index_connections", "elastic_private"), invalid_param_type)

    # Set attribute outside constraint
    # ================================


class MethodsInput(Base):
    """
    Tests methods which take input parameters

    Tests include: passing invalid input, etc.
    """

    # Input arguments valid
    pass


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