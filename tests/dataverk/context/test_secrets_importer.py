# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import json
from tempfile import TemporaryDirectory
from dataverk.context.secrets_importer import SecretsFromFilesImporter, SecretsFromApiImporter, get_secrets_importer
from pathlib import Path

# Common input parameters
# =======================
SETTINGS_TEMPLATE = {
    "value1": "${REPLACE_ME1}",
    "value2": {
        "value3": "${REPLACE_ME2}"
    },
    "secrets_path": ""
}

REPLACE_ME1 = "replaced_value_1"
REPLACE_ME2 = "replaced_value_2"

# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self._tmp_secrets_files_path = self._create_tmp_secrets_dir()
        self._settings = SETTINGS_TEMPLATE
        self._settings["secrets_path"] = self._tmp_secrets_files_path

    def tearDown(self):
        self._tmp_secrets_files_path.cleanup()

    def _create_tmp_secrets_dir(self):
        tmp_secrets_dir = TemporaryDirectory()
        secret_file_1_path = Path(tmp_secrets_dir.name).joinpath("REPLACE_ME1")
        secret_file_2_path = Path(tmp_secrets_dir.name).joinpath("REPLACE_ME2")

        with Path(secret_file_1_path.name).open('w') as secret_file_1:
            secret_file_1.write(REPLACE_ME1)

        with Path(secret_file_2_path.name).open('w') as secret_file_2:
            secret_file_2.write(REPLACE_ME2)

        return tmp_secrets_dir.name

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
    # Test factory method
    def test_get_secrets_importer_secrets_from_file(self):
        env_store = {"SECRETS_FROM_FILE": "True"}
        secret_importer = get_secrets_importer(env_store=env_store)
        self.assertIsInstance(secret_importer, SecretsFromFilesImporter)

    def test_get_secrets_importer_secrets_from_api(self):
        env_store = {"SECRETS_FROM_API": "True"}
        secret_importer = get_secrets_importer(env_store=env_store)
        self.assertIsInstance(secret_importer, SecretsFromApiImporter)

    def test_get_secrets_importer_raise_warning(self):
        env_store = {}
        with self.assertRaises(Warning):
            get_secrets_importer(env_store=env_store)

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
    # def test_secrets_replacement_from_files(self):
    #     env_store = {"SECRETS_FROM_FILES": "True"}
    #     secrets_importer = get_secrets_importer(env_store=env_store)
    #
    #     settings = secrets_importer.apply_secrets(self._settings)
    #
    #     self.assertEqual(settings["value1"], REPLACE_ME1)
    #     self.assertEqual(settings["value3"], REPLACE_ME2)


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
