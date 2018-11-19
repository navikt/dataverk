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
test_file_names = ("som.sql", "beef.json", "nosuffix", ".json", "java.java", "data.csv")
bad_file_names = ("", None, object(), 1, 1.0,)
static_files_path = Path(__file__).parent.absolute().joinpath("static")
static_files = (".env_test", "dataverk-secrets.json")
# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.test_file_names = test_file_names
        self.static_file_names = static_files
        self.bad_file_names = bad_file_names
        self.static_files_path = static_files_path

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
        levels = 1
        files = (".env_test", "dataverk-secrets.json")
        ret_dict = resource_discoverer.search_for_files(self.static_files_path,
                                                       files, levels)
        for file in files:
            self.assertTrue(file in ret_dict)

    def test___validate_search_path__normal_case(self):
        self.assertTrue(resource_discoverer._validate_search_path(Path(".")))

    def test___validate_search_path__pass_in_file_path(self):
        with self.assertRaises(ValueError) as cm:
            self.assertTrue(resource_discoverer._validate_search_path(Path(__file__)))

    def test__create_file_set__normal_case(self):
        self.assertCountEqual(self.test_file_names, resource_discoverer._create_file_set(self.test_file_names))

    def test__create_file_set__duplicate_filename(self):
            test_names = [x for x in self.test_file_names]
            # add duplicate file name
            test_names.append(test_names[0])
            result = resource_discoverer._create_file_set(test_names)
            self.assertCountEqual(self.test_file_names, result)

    def test__create_file_set__invalid_filename(self):
        for _filename in self.bad_file_names:
            with self.subTest(_filename=_filename):
                with self.assertRaises(ValueError) as cm:
                    res = resource_discoverer._create_file_set((_filename,))

    def test__search_paths_in_range__normal_case(self):
        result = resource_discoverer._search_paths_in_range(self.static_files_path, self.static_file_names, 1)
        for file in self.static_file_names:
            self.assertTrue(file in result)

    def test__search_paths_in_range__out_of_path_bounds(self):
        result = resource_discoverer._search_paths_in_range(self.static_files_path, self.static_file_names, 100)
        for file in self.static_file_names:
            self.assertTrue(file in result)



