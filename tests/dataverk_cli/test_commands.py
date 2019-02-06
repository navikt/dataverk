# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import argparse
from dataverk_cli.cli.cli_utils import commands


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
        self.parser = argparse.parser = argparse.ArgumentParser()
        self.sub_parser = self.parser.add_subparsers(title='commands', dest='command')


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

    def test_schedule__normal_case(self):
        commands.add_update_schedule_command(self.sub_parser)
        res = self.parser.parse_args(("schedule", "--package-name", "test"))
        self.assertEqual(res.package_name, "test")

    def test_init__normal_case(self):
        commands.add_init_command(self.sub_parser)
        res = self.parser.parse_args(("init", "--package-name", "test"))
        self.assertEqual(res.package_name, "test")

