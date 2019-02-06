# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
from dataverk_cli.scheduling import scheduler_factory

# Common input parameters
# =======================
jenkins_settings_store = {"jenkins": ""}
travis_settings_store = {"travis": ""}
cricle_ci_settings_store = {"circle_ci": ""}


# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.travis_settings_store = travis_settings_store
        self.circle_ci_settings_store = cricle_ci_settings_store
        self.jenkins_settings_store = jenkins_settings_store


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


class MethodsReturnUnits(Base):
    """
    Tests methods' output units where applicable
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_read_scheduler_from_settings__normal_cases(self):
        scheduler_type = scheduler_factory.read_scheduler_from_settings(self.jenkins_settings_store)
        self.assertEqual(scheduler_type, scheduler_factory.Schedulers.JENKINS,
                              f"Scheduler({scheduler_type}) should be of type({scheduler_factory.Schedulers.JENKINS})")

        scheduler_type = scheduler_factory.read_scheduler_from_settings(self.travis_settings_store)
        self.assertEqual(scheduler_type, scheduler_factory.Schedulers.TRAVIS,
                              f"Scheduler({scheduler_type}) should be of type({scheduler_factory.Schedulers.TRAVIS})")

        scheduler_type = scheduler_factory.read_scheduler_from_settings(self.circle_ci_settings_store)
        self.assertEqual(scheduler_type, scheduler_factory.Schedulers.CIRCLE_CI,
                              f"Scheduler({scheduler_type}) should be of type({scheduler_factory.Schedulers.CIRCLE_CI})")

    def test_test_read_scheduler_from_settings__jenkins_is_main(self):
        all_schedulers_settings_store = {**self.circle_ci_settings_store,
                                         **self.travis_settings_store,
                                         **self.jenkins_settings_store}

        scheduler_type = scheduler_factory.read_scheduler_from_settings(all_schedulers_settings_store)
        self.assertEqual(scheduler_type, scheduler_factory.Schedulers.JENKINS,
                              f"Scheduler({scheduler_type}) should be of type({scheduler_factory.Schedulers.JENKINS})")