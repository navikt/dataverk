# -*- coding: utf-8 -*-
# Import statements
# =================
import os
import json
import pandas as pd
from unittest import TestCase
from dataverk import Datapackage
from pathlib import Path
from dataverk.utils import resource_discoverer

# Common input parameters
# =======================
metadata_file_template = {
  "Sist oppdatert": "today",
  "Bucket_navn": "nav-bucket123",
  "Datapakke_navn": "nav-datapakke123",
  "Lisens": "Test license"
}


# Base classes
# ============
class Base(TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        if "RUN_FROM_VDI" in os.environ:
            del os.environ["RUN_FROM_VDI"]

        self.files = resource_discoverer.search_for_files(start_path=Path(os.path.dirname(os.path.realpath(__file__))),
                                          file_names=('testfile_settings.json', '.env_test'), levels=3)

        self.datapackage = Datapackage(settings_file_path=Path(self.files["testfile_settings.json"]), public=False,
                                       env_file_path=Path(self.files[".env_test"]))

    def tearDown(self):
        try:
            os.remove(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')))
        except OSError:
            pass


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """
    def setUp(self):
        if "RUN_FROM_VDI" in os.environ:
            del os.environ["RUN_FROM_VDI"]

        self.files = resource_discoverer.search_for_files(start_path=Path(os.path.dirname(os.path.realpath(__file__))),
                                          file_names=('testfile_settings.json', '.env_test'), levels=3)

        with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'w+') as metadata_file:
            json.dump(metadata_file_template, metadata_file)

    def test_class_instantiation_normal(self):
        datapackage = Datapackage(settings_file_path=Path(self.files["testfile_settings.json"]), public=False,
                                  env_file_path=Path(self.files[".env_test"]))
        self.assertEqual(datapackage.is_public, False)

    # Input arguments wrong type
    # ==========================
    def test_class_instantiation_wrong_input_param_type(self):
        wrong_input_param_types = [0, "False", pd.DataFrame(), object(), list()]
        for input_type in wrong_input_param_types:
            with self.subTest(msg="Wrong input parameter type in Datapackage class instantiation", _input=input_type):
                with self.assertRaises(TypeError):
                    Datapackage(settings_file_path=Path(self.files["testfile_settings.json"]), public=input_type,
                                env_file_path=Path(self.files[".env_test"]))

    def test_class_instantiation_with_invalid_settings_file(self):
        with self.assertRaises(FileNotFoundError):
            Datapackage(settings_file_path=Path("settings_file_that_does_not_exist.json"), public=False,
                        env_file_path=Path(self.files[".env_test"]))

    # Input arguments outside constraints
    # ===================================
    def test_invalid_bucket_or_datapackage_names(self):
        invalid_names = ["_name", "-name", "name with spaces", "name_", "name-", "Name", "name_with_underscore"]

        for datapackage_name in invalid_names:
            with self.subTest(msg="Invalid data package name", _input=datapackage_name):
                with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'r') as metadata_file:
                    metadata = json.load(metadata_file)
                with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'w') as metadata_file:
                    metadata["Datapakke_navn"] = datapackage_name
                    json.dump(metadata, metadata_file)
                with self.assertRaises(NameError):
                    Datapackage(settings_file_path=Path(self.files["testfile_settings.json"]), public=False,
                                env_file_path=Path(self.files[".env_test"]))

        for bucket_name in invalid_names:
            with self.subTest(msg="Invalid data package name", _input=bucket_name):
                with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'r') as metadata_file:
                    metadata = json.load(metadata_file)
                with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'w') as metadata_file:
                    metadata["Bucket_navn"] = bucket_name
                    json.dump(metadata, metadata_file)
                with self.assertRaises(NameError):
                    Datapackage(settings_file_path=Path(self.files["testfile_settings.json"]), public=False,
                                env_file_path=Path(self.files[".env_test"]))


class Set(Base):
    """
    Tests all aspects of setting attributes

    Tests include: setting attributes of wrong type, setting attributes outside their constraints, etc.
    """

    # Set attribute wrong type
    # ========================

    # Set attribute outside constraint
    # ================================


class MethodsInput(Base):
    """
    Tests methods which take input parameters

    Tests include: passing invalid input, etc.
    """

    # Test normal method inputs
    def test_add_resource_normal(self):
        df = pd.DataFrame()
        dataset_name = "dataset"
        dataset_description = "dataset beskrivelse"

        self.datapackage.add_resource(df=df, dataset_name=dataset_name, dataset_description=dataset_description)
        self.assertIsInstance(self.datapackage.resources[dataset_name], pd.DataFrame)
        self.assertEqual(self.datapackage.datapackage_metadata['Datasett'][dataset_name], dataset_description)

    def test_update_metadata_normal(self):
        self.datapackage.update_metadata("test_key", "test_value")
        self.assertEqual(self.datapackage.datapackage_metadata["test_key"], "test_value")

    # Test wrong input types
    def test_add_resource_wrong_input_types(self):
        wrong_df_input_types = [0, "string", False, object(), list()]
        for input_type in wrong_df_input_types:
            with self.subTest(msg="add_resource: Wrong input parameter type for df parameter", _input=input_type):
                with self.assertRaises(TypeError):
                    self.datapackage.add_resource(df=input_type, dataset_name="dataset", dataset_description="")

        wrong_dataset_name_input_types = [0, pd.DataFrame(), False, object(), list()]
        for input_type in wrong_dataset_name_input_types:
            with self.subTest(msg="add_resource: Wrong input parameter type for dataset_name parameter", _input=input_type):
                with self.assertRaises(TypeError):
                    self.datapackage.add_resource(df=pd.DataFrame(), dataset_name=input_type, dataset_description="")

        wrong_dataset_desc_input_types = [0, pd.DataFrame(), False, object(), list()]
        for input_type in wrong_dataset_desc_input_types:
            with self.subTest(msg="add_resource: Wrong input parameter type for dataset_description parameter", _input=input_type):
                with self.assertRaises(TypeError):
                    self.datapackage.add_resource(df=pd.DataFrame(), dataset_name="dataset", dataset_description=input_type)

    def test_update_metadata_wrong_input_types(self):
        wrong_input_types = [0, pd.DataFrame(), False, object(), list()]

        for input_type in wrong_input_types:
            with self.subTest(msg="update_metadata: Wrong input parameter type for key parameter", _input=input_type):
                with self.assertRaises(TypeError):
                    self.datapackage.update_metadata(key=input_type, value="test_value")

        for input_type in wrong_input_types:
            with self.subTest(msg="update_metadata: Wrong input parameter type for value parameter", _input=input_type):
                with self.assertRaises(TypeError):
                    self.datapackage.update_metadata(key="test_key", value=input_type)

    def test_wrong_sql_connector_input(self):
        with self.assertRaises(TypeError):
            self.datapackage.read_sql(source='datalab', sql="SELECT * FROM test", connector="non_existant_connector")


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
