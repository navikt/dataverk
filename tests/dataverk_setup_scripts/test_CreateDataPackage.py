# -*- coding: utf-8 -*-
# Import statements
# =================
import os
from unittest import TestCase
from dataverk_setup_scripts import dataverk_create
from shutil import rmtree

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

    # Valid instanciation # TODO: trenger brukerident og passord i constructoren.
    # def test_class_instantiation_normal(self):
    #     dataverk_create.CreateDataPackage(name="ny-datapakke", github_project="https://github.com/navikt/datasett.git",
    #                                       update_schedule="0 12 * * 1", namespace="opendata",
    #                                       settings_repo="https://github.com/navikt/dataverk_settings.git",
    #                                       user="userident", password="password")

    # Input arguments wrong type
    # ==========================
    def test_class_instantiation_wrong_input_param_type(self):
        wrong_input_param_types = [0, False, object(), list()]
        for input_type in wrong_input_param_types:
            with self.subTest(msg="Wrong input parameter type in CreateDataPackage class instantiation", _input=input_type):
                with self.assertRaises(TypeError):
                    dataverk_create.CreateDataPackage(name=input_type,
                                                      github_project="https://github.com/navikt/datasett.git",
                                                      update_schedule="0 12 * * 1")
                    dataverk_create.CreateDataPackage(name="ny-datapakke",
                                                      github_project=input_type,
                                                      update_schedule="0 12 * * 1")
                    dataverk_create.CreateDataPackage(name="ny-datapakke",
                                                      github_project="https://github.com/navikt/datasett.git",
                                                      update_schedule=input_type)

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

    # Input arguments valid


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