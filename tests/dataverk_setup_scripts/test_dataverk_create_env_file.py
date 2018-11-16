# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import os
from dataverk_setup_scripts import dataverk_create_env_file

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
        pass

    def tearDown(self):
        try:
            os.remove('.env')
        except OSError:
            pass



# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """
    def setUp(self):
        self.invalid_path_types = [1, False, object(), list()]
        self.path_to_file = './mappe/fil.json'
        self.non_existant_path = './mappe_som_ikke_eksisterer'

    def test_normal_instanciation(self):
        dataverk_create_env_file.CreateEnvFile(user_ident="brukerident", password="passord")

    # Input arguments wrong type
    # ==========================
    def test_instanciation_with_wrong_input_types(self):
        for input in self.invalid_path_types:
            with self.subTest(msg="Wrong input parameter type in CreateSettingsTemplate class instantiation",
                              _input=input):
                with self.assertRaises(TypeError):
                    dataverk_create_env_file.CreateEnvFile(user_ident=input, password="passord", destination=".")
            with self.subTest(msg="Wrong input parameter type in CreateSettingsTemplate class instantiation",
                              _input=input):
                with self.assertRaises(TypeError):
                    dataverk_create_env_file.CreateEnvFile(user_ident="brukerident", password=input, destination=".")
            with self.subTest(msg="Wrong input parameter type in CreateSettingsTemplate class instantiation",
                              _input=input):
                with self.assertRaises(TypeError):
                    dataverk_create_env_file.CreateEnvFile(user_ident="brukerident", password="passord", destination=input)

    def test_instanctiation_with_path_to_file(self):
        with self.assertRaises(ValueError):
            dataverk_create_env_file.CreateEnvFile(user_ident="brukerident", password="passord", destination=self.path_to_file)

    def test_instanciation_with_non_existant_path(self):
        with self.assertRaises(ValueError):
            dataverk_create_env_file.CreateEnvFile(user_ident="brukerident", password="passord", destination=self.non_existant_path)


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


class MethodsReturnUnits(Base):
    """
    Tests methods' output units where applicable
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """
