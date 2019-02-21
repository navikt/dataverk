# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import json
import pandas as pd
from datetime import datetime
from unittest import mock
from dataverk.connectors import KafkaConnector
from dataclasses import dataclass


# Common input parameters
# =======================
SETTINGS_TEMPLATE = {
  "package_name": "package_name",
  "nais_namespace": "dataverk",
  "image_endpoint": "mitt.image.endpoint.no:1234/",
  "update_schedule": "* * 31 2 *",

  "vault": {
    "auth_uri": "https://vault.auth.uri.com",
    "secrets_uri": "https://vault.secrets.uri.com",
    "vks_auth_path": "/path/to/auth",
    "vks_kv_path": "/path/to/kv",
    "vks_vault_role": "role_name",
    "service_account": "service_account_name"
  },

  "jenkins": {
      "url": "https//jenkins.server.com:1234"
  },

  "kafka": {
      "brokers": [""],
      "sasl_plain_username": None,
      "sasl_plain_password": None,
      "security_protocol": "PLAINTEXT",
      "sasl_mechanism": None,
      "ssl_cafile": None,
      "group_id": None
  }
}

TOPICS = ["test-topic"]

VALID_FETCH_MODES = ["from_beginning", "last_committed_offset"]

PANDAS_DF_REF = pd.DataFrame({'value1': [1, 2], 'value2': [3, 4]})

PANDAS_FIRST_HALF = pd.DataFrame({'value1': [1], 'value2': [3]})

KAFKA_MESSAGE = "{\"value1\": 2, \"value2\": 4}"


# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        pass
        self._settings_store = SETTINGS_TEMPLATE
        self._fake_instance = mock.Mock()
        self._fetch_mode = "from_beginning"


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

class MethodsReturnUnits(Base):
    """
    Tests methods' output units where applicable
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """
    pass