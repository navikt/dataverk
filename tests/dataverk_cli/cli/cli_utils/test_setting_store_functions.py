# -*- coding: utf-8 -*-
# Import statements
# =================

from dataverk_cli.cli.cli_utils.setting_store_functions import create_settings_dict
from tests.settings_resource_test_mixin import SettingsResourceTestMixin


# Common input parameters
# =======================

class MockArgsInternal:
    internal = True


class MockArgsNotInternal:
    internal = False

# Base classes
# ============


class Base(SettingsResourceTestMixin):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """

    def setUp(self):
        super().setUp()
        self.git_env_store = {"SETTINGS_REPO": self.tmp_repo_git_path}
        self.mock_internal_args = MockArgsInternal
        self.mock_not_internal_args = MockArgsNotInternal

    def tearDown(self):
        super().tearDown()


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """

    def test_create_settings_dict__samity_check(self):
        create_settings_dict(self.mock_internal_args, self.git_env_store)


    # Input arguments wrong type
    # ==========================

    # Input arguments outside constraints
    # ===================================


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_create_settings_dict__internal_set(self):
        result = create_settings_dict(self.mock_internal_args, self.git_env_store)
        self.assertIsNotNone(result)

    def test_create_settings_dict__internal_not_set(self):
        result = create_settings_dict(self.mock_internal_args, self.git_env_store)
        self.assertIsNotNone(result)