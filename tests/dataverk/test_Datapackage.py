# -*- coding: utf-8 -*-
# Import statements
# =================
import os
import json
import pandas as pd
from unittest import TestCase
from dataverk import Datapackage

# Common input parameters
# =======================
metadata_file_template = {
  "Sist oppdatert": "today",
  "Bucket_navn": "nav-bucket",
  "Datapakke_navn": "nav-datapakke",
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
        with open(os.path.abspath(os.path.join(os.pardir, 'LICENSE.md')), 'w+') as license_file:
            license_file.write("test license")
        with open(os.path.abspath(os.path.join(os.pardir, 'README.md')), 'w+') as readme_file:
            readme_file.write("test readme")
        with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'w+') as metadata_file:
            json.dump(metadata_file_template, metadata_file)

    def tearDown(self):
        os.remove(os.path.abspath(os.path.join(os.pardir, 'LICENSE.md')))
        os.remove(os.path.abspath(os.path.join(os.pardir, 'README.md')))
        os.remove(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')))


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """
    def test_class_instantiation_normal(self):
        datapackage = Datapackage(public=False)
        self.assertEqual(datapackage.is_public, False)

    # Input arguments wrong type
    # ==========================
    def test_class_instantiation_wrong_input_param_type(self):
        wrong_input_param_types = [0, "False", pd.DataFrame(), object(), list()]
        for input_type in wrong_input_param_types:
            with self.subTest(msg="Wrong input parameter type in Datapackage class instantiation", _input=input_type):
                with self.assertRaises(TypeError):
                    Datapackage(public=input_type)

    # Input arguments outside constraints
    # ===================================
    def test_invalid_bucket_or_datapackage_names(self):
        invalid_names = ["_name", "-name", "name with spaces", "name_", "name-", "Name", "name2", "name;"]

        for datapackage_name in invalid_names:
            with self.subTest(msg="Invalid data package name", _input=datapackage_name):
                with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'r') as metadata_file:
                    metadata = json.load(metadata_file)
                with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'w') as metadata_file:
                    metadata["Datapakke_navn"] = datapackage_name
                    json.dump(metadata, metadata_file)
                with self.assertRaises(NameError):
                    Datapackage(public=False)

        for bucket_name in invalid_names:
            with self.subTest(msg="Invalid data package name", _input=bucket_name):
                with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'r') as metadata_file:
                    metadata = json.load(metadata_file)
                with open(os.path.abspath(os.path.join(os.pardir, 'METADATA.json')), 'w') as metadata_file:
                    metadata["Bucket_navn"] = bucket_name
                    json.dump(metadata, metadata_file)
                with self.assertRaises(NameError):
                    Datapackage(public=False)


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
        datapackage = Datapackage(public=False)
        df = pd.DataFrame()
        dataset_name = "dataset"
        dataset_description = "dataset beskrivelse"

        datapackage.add_resource(df=df, dataset_name=dataset_name, dataset_description=dataset_description)
        self.assertIsInstance(datapackage.resources[dataset_name], pd.DataFrame)
        self.assertEqual(datapackage.datapackage_metadata['Datasett'][dataset_name], dataset_description)

    def test_update_metadata_normal(self):
        datapackage = Datapackage(public=False)
        datapackage.update_metadata("test_key", "test_value")
        self.assertEqual(datapackage.datapackage_metadata["test_key"], "test_value")

    # Test wrong input types
    def test_add_resource_wrong_input_types(self):
        datapackage = Datapackage(public=False)

        wrong_df_input_types = [0, "string", False, object(), list()]
        for input_type in wrong_df_input_types:
            with self.subTest(msg="add_resource: Wrong input parameter type for df parameter", _input=input_type):
                with self.assertRaises(TypeError):
                    datapackage.add_resource(df=input_type, dataset_name="dataset", dataset_description="")

        wrong_dataset_name_input_types = [0, pd.DataFrame(), False, object(), list()]
        for input_type in wrong_dataset_name_input_types:
            with self.subTest(msg="add_resource: Wrong input parameter type for dataset_name parameter", _input=input_type):
                with self.assertRaises(TypeError):
                    datapackage.add_resource(df=pd.DataFrame(), dataset_name=input_type, dataset_description="")

        wrong_dataset_desc_input_types = [0, pd.DataFrame(), False, object(), list()]
        for input_type in wrong_dataset_desc_input_types:
            with self.subTest(msg="add_resource: Wrong input parameter type for dataset_description parameter", _input=input_type):
                with self.assertRaises(TypeError):
                    datapackage.add_resource(df=pd.DataFrame(), dataset_name="dataset", dataset_description=input_type)

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
