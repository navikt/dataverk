# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import os
from pprint import pprint as pp
from dataverk.utils import resource_discoverer
from pathlib import Path

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


# Test classes
# ============



class MethodsInput(Base):
    """
    Tests methods which take input parameters

    Tests include: passing invalid input, etc.
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_search(self):
        search_path = Path(__file__).parent.absolute().joinpath("static")
        levels = 1
        files = (".env_test", "dataverk-secrets.json")
        ret_dict = resource_discoverer.search_for_files(search_path,
                                                       files, levels)
        for file in files:
            self.assertTrue(file in ret_dict)

    def test__validate_path_levels__root_and_level_1(self):
        self.assertTrue(resource_discoverer._validate_path_levels(Path("/"), 1))

    def test__validate_path_levels__to_high_level(self):
        with self.assertRaises(ValueError) as cm:
            resource_discoverer._validate_path_levels(Path("/home"), 3)

    def test___validate_search_path__normal_case(self):
        self.assertTrue(resource_discoverer._validate_search_path(Path(".")))

    def test___validate_search_path__pass_in_file_path(self):
        with self.assertRaises(ValueError) as cm:
            self.assertTrue(resource_discoverer._validate_search_path(Path(__file__)))


