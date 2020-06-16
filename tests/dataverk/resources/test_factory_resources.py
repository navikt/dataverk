import unittest
import pandas as pd

from dataverk.resources.factory_resources import get_resource_object, verify_resource_input_type
from dataverk.resources.dataframe_resource import DataFrameResource
from dataverk.resources.pdf_resource import PDFResource
from dataverk.resources.remote_resource import RemoteResource


pdf_resource_type = "pdf"
df_resource_type = "df"
remote_resource_type = "remote"
other_resource_type = "some_resource_type"


class TestMethodReturnValues(unittest.TestCase):

    def test_get_resource_object_df(self):
        resource_formatter = get_resource_object(resource_type=df_resource_type, resource="", resource_description="",
                                                 resource_name="", spec={}, datapackage_path="")
        self.assertIsInstance(resource_formatter, DataFrameResource)

    def test_get_resource_object_remote(self):
        resource_formatter = get_resource_object(resource_type=remote_resource_type, resource="",
                                                 resource_description="", resource_name="", spec={},
                                                 datapackage_path="")
        self.assertIsInstance(resource_formatter, RemoteResource)

    def test_get_resource_object_pdf(self):
        resource_formatter = get_resource_object(resource_type=pdf_resource_type, resource="", resource_description="",
                                                 resource_name="", spec={}, datapackage_path="")
        self.assertIsInstance(resource_formatter, PDFResource)

    def test_get_resource_object_other(self):
        with self.assertRaises(NotImplementedError):
            get_resource_object(resource_type=other_resource_type, resource="", resource_description="",
                                resource_name="", spec={}, datapackage_path="")

    def test_get_resource_object_invalid_input(self):
        pass

    def test_verify_resource_input_type_invalid_resource(self):
        with self.assertRaises(TypeError):
            verify_resource_input_type(1, "resource name", "resource description", {})

    def test_verify_resource_input_type_invalid_resource_name(self):
        with self.assertRaises(TypeError):
            verify_resource_input_type(pd.DataFrame(data={}), 1, "resource description", {})

    def test_verify_resource_input_type_invalid_resource_description(self):
        with self.assertRaises(TypeError):
            verify_resource_input_type(pd.DataFrame(data={}), "resource name", 1, {})

    def test_verify_resource_input_type_invalid_resource_spec(self):
        with self.assertRaises(TypeError):
            verify_resource_input_type(pd.DataFrame(data={}), "resource name", "resource description", 1)


