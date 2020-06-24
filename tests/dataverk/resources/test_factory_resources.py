import unittest
import pandas as pd

from dataverk.resources.factory_resources import get_resource_object
from dataverk.resources.dataframe_resource import DataFrameResource
from dataverk.resources.pdf_resource import PDFResource
from dataverk.resources.remote_resource import RemoteResource


pdf_resource_type = "pdf"
df_resource_type = "df"
remote_resource_type = "remote"
other_resource_type = "some_resource_type"
df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
remote_resource = "https://remote.resource.no/path/to/resource.csv"


class TestMethodReturnValues(unittest.TestCase):

    def test_get_resource_object_df(self):
        resource_formatter = get_resource_object(resource_type=df_resource_type, resource=df, resource_description="",
                                                 resource_name="", spec={}, datapackage_path="")
        self.assertIsInstance(resource_formatter, DataFrameResource)

    def test_get_resource_object_remote(self):
        resource_formatter = get_resource_object(resource_type=remote_resource_type, resource=remote_resource,
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