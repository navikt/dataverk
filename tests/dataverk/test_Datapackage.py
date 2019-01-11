# -*- coding: utf-8 -*-
# Import statements
# =================
import os
import json
import pandas as pd
from unittest import TestCase
from dataverk.datapackage import Datapackage
from pathlib import Path
from dataverk.utils import resource_discoverer

# Common input parameters
# =======================
metadata_file_template = {
  "updated": "today",
  "bucket_name": "nav-bucket123",
  "datapackage_name": "nav-datapakke123",
  "license": "Test license"
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

        with open('METADATA.json', 'w') as metadata_file:
            json.dump(metadata_file_template, metadata_file)

        self.files = resource_discoverer.search_for_files(start_path=Path(__file__).parent.joinpath("static"),
                                                          file_names=('settings.json', '.env'), levels=3)

        self.datapackage = Datapackage(resource_files=self.files)

    def tearDown(self):
        try:
            os.remove('METADATA.json')
        except OSError:
            pass


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """

    def test_class_instantiation_normal(self):
        datapackage = Datapackage(resource_files=self.files)
        self.assertEqual(datapackage.is_public, False)


    #def test_class_instantiation_without_settings_file(self):
        #with self.assertRaises(KeyError):
        #    Datapackage(resource_files={})


    # Input arguments outside constraints
    # ===================================
    def test_invalid_bucket_or_datapackage_names(self):
        invalid_names = ["_name", "-name", "name with spaces", "name_", "name-", "Name", "name_with_underscore"]

        for datapackage_name in invalid_names:
            with self.subTest(msg="Invalid data package name", _input=datapackage_name):
                with open('METADATA.json', 'r') as metadata_file:
                    metadata = json.load(metadata_file)
                with open('METADATA.json', 'w') as metadata_file:
                    metadata["datapackage_name"] = datapackage_name
                    json.dump(metadata, metadata_file)
                with self.assertRaises(NameError):
                    Datapackage(resource_files=self.files)

        for bucket_name in invalid_names:
            with self.subTest(msg="Invalid data package name", _input=bucket_name):
                with open('METADATA.json', 'r') as metadata_file:
                    metadata = json.load(metadata_file)
                with open('METADATA.json', 'w') as metadata_file:
                    metadata["bucket_name"] = bucket_name
                    json.dump(metadata, metadata_file)
                with self.assertRaises(NameError):
                    Datapackage(resource_files=self.files)


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
        # self.assertEqual(self.datapackage.datapackage_metadata['Datasett'][dataset_name], dataset_description)

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

        # wrong_dataset_desc_input_types = [0, pd.DataFrame(), False, object(), list()]
        # for input_type in wrong_dataset_desc_input_types:
        #     with self.subTest(msg="add_resource: Wrong input parameter type for dataset_description parameter", _input=input_type):
        #         with self.assertRaises(TypeError):
        #             self.datapackage.add_resource(df=pd.DataFrame(), dataset_name="dataset", dataset_description=input_type)

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
