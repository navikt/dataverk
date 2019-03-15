# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import urllib3
from dataverk.connectors import ElasticsearchConnector
from dataverk_cli.dataverk_publish import PublishDataPackage
# Common input parameters
# =======================


class ElasticsearchConnectorMock(ElasticsearchConnector):

    def __init__(self, settings_store):
        super().__init__(settings_store)

    def write(self, id, js):
        pass


class FaultyElasticsearchConnectorMock(ElasticsearchConnector):

    def __init__(self, settings_store):
        super().__init__(settings_store)

    def write(self, id, js):
        raise urllib3.exceptions.LocationValueError()


# Base classes
# ============

class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        self.test_settings = {
            "package_name": "package_name",
            "nais_namespace": "dataverk",
            "image_endpoint": "mitt.image.endpoint.no:1234/",
            "update_schedule": "* * 31 2 *",
            "id": "2389748234058oguisdhlg",

            "index_connections": {
                "elastic_host": "https://my.es.index:53535",
                "index": "myindex"
            },

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
            }
        }
        self.test_datapackage_metadata = {
            "id": "2389748234058oguisdhlg"
        }



# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """
    def test_instanciation(self):
        PublishDataPackage(settings_store=self.test_settings, env_store={},
                           es_index=ElasticsearchConnector(self.test_settings),
                           datapackage_metadata=self.test_datapackage_metadata)

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
    def test__update_es_index_valid(self):
        datapackage = PublishDataPackage(settings_store=self.test_settings, env_store={},
                                         es_index=ElasticsearchConnectorMock(self.test_settings),
                                         datapackage_metadata=self.test_datapackage_metadata)
        datapackage._update_es_index()

    def test__update_es_index_invalid(self):
        datapackage = PublishDataPackage(settings_store=self.test_settings, env_store={},
                                         es_index=FaultyElasticsearchConnectorMock(self.test_settings),
                                         datapackage_metadata=self.test_datapackage_metadata)
        with self.assertRaises(urllib3.exceptions.LocationValueError):
            datapackage._update_es_index()


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