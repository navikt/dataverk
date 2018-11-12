# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
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


class MethodsReturnType(Base):
    """
    Tests methods' output types
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_search(self):
        ret_dict = resource_discoverer.search(Path("."), ("__init__.py", "dataverk-secrets.json"), 2)
        self.assertTrue("__init__.py" in ret_dict)
        self.assertTrue("dataverk-secrets.json" in ret_dict)

    def test_search_for_files_from_working_dir(self):
        ret_dict = resource_discoverer.search_for_files_from_working_dir(("__init__.py", "dataverk-secrets.json"), 2)
        self.assertTrue("__init__.py" in ret_dict)
        self.assertTrue("dataverk-secrets.json" in ret_dict)


