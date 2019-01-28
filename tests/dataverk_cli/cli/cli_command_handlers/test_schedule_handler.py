# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from unittest import mock
from unittest.mock import patch

from dataverk_cli.cli.cli_command_handlers import schedule_handler

# Common input parameters
# =======================


class MockArgs:
    update_schedule = "* * * * *"


class MockArgsNoSchedule:
    update_schedule = None


SETTINGS_DICT = {
    "package_name": "my-package"
}


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

    @patch('builtins.input', return_value='0 12 31 2 *')
    def test__create_update_schedule(self, input):
        schedule_handler._create_update_schedule(MockArgsNoSchedule)

    @patch('builtins.input', return_value='60 12 31 2 *')
    def test__create_update_schedule_invalid(self, input):
        with self.assertRaises(ValueError):
            schedule_handler._create_update_schedule(MockArgsNoSchedule)

    @patch('builtins.input', return_value=['y'])
    def test_handler_no_remote_repository(self, input):
        with self.assertRaises(FileNotFoundError):
            schedule_handler.handle(MockArgs, SETTINGS_DICT)
