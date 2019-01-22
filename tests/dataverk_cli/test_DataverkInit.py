# -*- coding: utf-8 -*-
# Import statements
# =================
import json
from unittest import TestCase
from dataverk_cli.dataverk_init import DataverkInit
from dataverk.context.env_store import EnvStore
from tempfile import NamedTemporaryFile


# Common input parameters
# =======================
SETTINGS_TEMPLATE = {
    "index_connections": {},

    "file_storage_connections": {
        "local": {}
    },

    "bucket_storage_connections": {
        "AWS_S3": {},
        "google_cloud": {
            "credentials": {}
        }
    },

    "vault": {},

    "jenkins": {}
}

ENV_FILE_TEMPLATE = ''''''


# Base classes
# ============
class Base(TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.settings_path = NamedTemporaryFile(mode='w', encoding="utf-8")
        self.env_path = NamedTemporaryFile(mode='w', encoding="utf-8")

        self.settings_path.name = 'settings.json'
        self.env_path.name = '.env'

        print(self.settings_path)
        print(self.env_path)

        with open(self.settings_path, 'w') as settings_file:
            json.dump(SETTINGS_TEMPLATE, settings_file)

        with open(self.env_path, 'w') as env_path:
            env_path.write(ENV_FILE_TEMPLATE)

        with open(self.settings_path, 'r') as settings_file:
            self.settings = settings_file.read()

        with open(self.env_path, 'r') as env_file:
            self.envs = env_file.read()

        # self.dp = DataverkInit(settings=, envs=self.env_file)


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
    def test_instanciation(self):
        self.assertEqual(1, 1)


    # Input arguments wrong type
    # ==========================

        # Input arguments outside constraints
    # ===================================


class Set(Base):
    """
    Tests all aspects of setting attributes

    Tests include: setting attributes of wrong type, setting attributes outside their constraints, etc.
    """

    # Set attributes correct type
    # ===========================

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