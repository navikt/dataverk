# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from unittest import mock
from unittest.mock import patch

from dataverk_cli.cli.cli_command_handlers import init_handler

# Common input parameters
# =======================


class MockArgsInternal:
    internal = True
    package_name = "test-package"


class MockArgsNotInternal:
    internal = False
    package_name = "test-package"

# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        pass


# Test classes
# ============


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

    @patch('builtins.input', return_value='y')
    def test_handler_yes_sanity_check(self, input):
        init_handler.handle(MockArgsInternal, {}, {})

    @patch('builtins.input', return_value='n')
    def test_handler_no_sanity_check(self, input):
        with self.assertRaises(KeyboardInterrupt):
            init_handler.handle(MockArgsInternal, {}, {})
