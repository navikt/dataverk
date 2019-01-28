# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from dataverk_cli.cli.cli_utils import repo_info
from git import exc

# Common input parameters
# =======================

HTTPS_URL = "https://github.com/org_name/repo_name.git"
SSH_URL = "git@github.com:org_name/repo_name.git"
ORG_NAME = "org_name"
REPO_NAME = "repo_name"

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
        pass


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """

    # Input arguments wrong type
    # ==========================

    # Input arguments outside constraints
    # ===================================


class MethodsReturnType(Base):
    """
    Tests methods' output types
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_convert_to_ssh_url(self):
        self.assertEqual(SSH_URL, repo_info.convert_to_ssh_url(https_url=HTTPS_URL))

    def test_get_org_name(self):
        self.assertEqual(ORG_NAME, repo_info.get_org_name(https_url=HTTPS_URL))

    def test_get_repo_name(self):
        self.assertEqual(REPO_NAME, repo_info.get_repo_name(https_url=HTTPS_URL))
