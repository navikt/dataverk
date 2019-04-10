# -*- coding: utf-8 -*-
# Import statements
# =================
import copy
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
        self.datapackage = Datapackage(metadata_file_template)


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """

    def test_class_instantiation_normal(self):
        datapackage = Datapackage(metadata_file_template)
        self.assertIsNotNone(datapackage)

    # Input arguments outside constraints
    # ===================================
    def test_invalid_bucket_or_datapackage_names(self):
        invalid_names = ["_name", "-name", "name with spaces", "name_", "name-", "Name", "name_with_underscore"]

        for bucket_name in invalid_names:
            with self.subTest(msg="Invalid data package name", _input=bucket_name):
                metadata = copy.deepcopy(metadata_file_template)
                metadata["bucket_name"] = bucket_name
                with self.assertRaises(NameError):
                    Datapackage(metadata)


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
        self.assertEqual(self.datapackage.datapackage_metadata['datasets'][dataset_name], dataset_description)

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


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """

    def test_resources(self):
        dataset_name = "persons"
        df = pd.DataFrame.from_dict({"name": ["sondre"], "age": [10]})
        self.datapackage.add_resource(df=df,
                                      dataset_name=dataset_name,
                                      dataset_description="This is a description")
        resources = self.datapackage.resources
        self.assertIn(dataset_name, resources)
        self.assertIsInstance(resources[dataset_name], pd.DataFrame)

    def test_package_metadata(self):
        dataset_name = "persons"
        df = pd.DataFrame.from_dict({"name": ["sondre"], "age": [10]})
        self.datapackage.add_resource(df=df,
                                      dataset_name=dataset_name,
                                      dataset_description="This is a description")
        metadata = self.datapackage.datapackage_metadata
        expected = {'updated': '2019-04-10', 'bucket_name': 'nav-bucket123', 'datapackage_name': 'nav-datapakke123', 'license': None, 'version': '0.0.1', 'readme': None, 'views': [], 'resources': [{'name': 'persons', 'path': 'resources/persons.csv', 'format': 'csv', 'mediatype': 'text/csv', 'schema': {'fields': [{'name': 'name', 'description': '', 'type': 'string'}, {'name': 'age', 'description': '', 'type': 'number'}]}}], 'datasets': {'persons': 'This is a description'}}
        self.assertEquals(expected, metadata)