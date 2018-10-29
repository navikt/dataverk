# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from unittest import mock
from dataverk_setup_scripts import dataverk_init

# Common input parameters
# =======================
test_repo_name = "testrespo"
bad_repo_name_inputs = (2, "", False, [], ())

# Base classes
# ============


class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.test_repo_name = test_repo_name
        self.bad_repo_name_inputs = bad_repo_name_inputs


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """

    # Input arguments wrong type
    # ==========================

    def test_init_normal_case(self):
        new_rep = dataverk_init.NewRepoCreator("testrep")
        self.assertEqual(new_rep.name, "testrep")

    def test_init_bad_input_type(self):
        for _input in self.bad_repo_name_inputs:
            with self.subTest(_input=_input):
                with self.assertRaises(Exception) as cm:
                    res = dataverk_init.NewRepoCreator(_input)





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
