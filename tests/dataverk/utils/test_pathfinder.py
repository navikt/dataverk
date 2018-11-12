
# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from dataverk.utils import pathfinder
import sys


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


class MethodsReturnType(Base):
    """
    Tests methods' output types
    """
    pass



class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_get_project_root__normal_case(self):
        self.assertEqual(sys.path[0], pathfinder.get_project_root())
