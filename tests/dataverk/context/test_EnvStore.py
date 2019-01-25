# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from pathlib import Path
import tempfile


from dataverk.context.env_store import EnvStore
from tests.settings_resource_test_mixin import SettingsResourceTestMixin


# Common input parameters
# =======================


# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.test_dir = self.create_tmp_file_dir()
        self.test_file_path = Path(self.test_dir.name).joinpath(".env")

    def tearDown(self):
        self.test_dir.cleanup()

    def create_tmp_file_dir(self):
        tmpdir = tempfile.TemporaryDirectory()
        file_string = "USER_IDENT=testident\n" \
                      "PASSWORD=testpass"

        filepath = Path(tmpdir.name).joinpath(".env")
        with filepath.open(mode="w", encoding="utf-8") as file:
            file.write(file_string)
        return tmpdir




# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """

    def test_init__empty_sanity_check(self):
        EnvStore()

    def test_init__settings_file_sanity_check(self):
        EnvStore(self.test_file_path)

    def test_init__settings_file_and_envsetter_sanity_check(self):
        EnvStore(self.test_file_path, {})
        prin

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
    pass