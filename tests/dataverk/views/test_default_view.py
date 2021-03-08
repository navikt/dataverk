import os
import unittest

from dataverk import Datapackage
from dataverk.views.default import DefaultView
from tests.dataverk.views.constants import PLOT_JSON

DP_NAME = "Test"
BUCKET_NAME = "bucket-name"
VISUALIZATION_NAME = "viz"


class TestDefaultView(unittest.TestCase):

    def setUp(self):
        os.environ["DATAVERK_BUCKET_ENDPOINT"] = "https://bucket-endpoint.no"
        os.environ["DATAVERK_API_ENDPOINT"] = "https://api-endpoint.no"
        os.environ["DATAVERK_BUCKET"] = BUCKET_NAME
        self.dp = Datapackage({
            "title": DP_NAME
        })

    def tearDown(self):
        del os.environ["DATAVERK_BUCKET_ENDPOINT"]
        del os.environ["DATAVERK_API_ENDPOINT"]
        del os.environ["DATAVERK_BUCKET"]

    def test_add_to_datapackage(self):
        default_view = DefaultView(
            spec_type='other',
            name=VISUALIZATION_NAME,
            resources=[],
            attribution="Kilde: NAV.",
            spec=PLOT_JSON
        )

        default_view.add_to_datapackage(self.dp)

        self.assertFalse(VISUALIZATION_NAME in self.dp.resources.keys())
        self.assertFalse("url" in self.dp.datapackage_metadata["views"][0]["spec"].keys())
        for spec_key in ["data", "layout"]:
            with self.subTest(msg="Testing spec keys", _input=spec_key):
                self.assertTrue(spec_key in self.dp.datapackage_metadata["views"][0]["spec"].keys())
