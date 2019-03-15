# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from dataverk_cli.deploy import deployer_factory

# Common input parameters
# =======================
jenkins_settings_store = {"jenkins": ""}


# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.jenkins_settings_store = jenkins_settings_store


# Test classes
# ============


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_read_scheduler_from_settings__normal_cases(self):
        deployer_type = deployer_factory.read_build_server_from_settings(self.jenkins_settings_store)
        self.assertEqual(deployer_type, deployer_factory.BuildServer.JENKINS,
                              f"BuildServer({deployer_type}) should be of type({deployer_factory.BuildServer.JENKINS})")

    def test_test_read_scheduler_from_settings__jenkins_is_main(self):
        all_schedulers_settings_store = {**self.jenkins_settings_store}

        scheduler_type = deployer_factory.read_build_server_from_settings(all_schedulers_settings_store)
        self.assertEqual(scheduler_type, deployer_factory.BuildServer.JENKINS,
                              f"Scheduler({scheduler_type}) should be of type({deployer_factory.BuildServer.JENKINS})")
