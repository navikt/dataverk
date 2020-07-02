import os
import unittest
import pandas as pd

from dataverk import Datapackage
from dataverk.views.json_spec import JsonSpecView


DP_NAME = "Test"
BUCKET_NAME = "bucket-name"
VISUALIZATION_NAME = "plotlyviz"

DATATABLE_VIZ_NAME = "thetable"
DATATABLE_SPEC = pd.DataFrame()


class TestRemoteSpecViews(unittest.TestCase):

    def setUp(self):
        os.environ["DATAVERK_BUCKET_ENDPOINT"] = "https://bucket-endpoint.no"
        os.environ["DATAVERK_API_ENDPOINT"] = "https://api-endpoint.no"
        self.dp = Datapackage({
            "title": DP_NAME,
            "store": "nais",
            "bucket": BUCKET_NAME
        })

    def tearDown(self):
        del os.environ["DATAVERK_BUCKET_ENDPOINT"]
        del os.environ["DATAVERK_API_ENDPOINT"]

    def test_add_to_datapackage_json_view(self):
        expt_resource_path = f"{os.environ['DATAVERK_API_ENDPOINT']}/{BUCKET_NAME}/resources/{VISUALIZATION_NAME}.json"
        remote_spec_view = JsonSpecView(
            spec_type='plotly',
            name=VISUALIZATION_NAME,
            resources=[],
            attribution="Kilde: NAV.",
            spec={}
        )

        remote_spec_view.add_to_datapackage(self.dp)

        self.assertTrue(VISUALIZATION_NAME in self.dp.resources.keys())
        self.assertTrue(self.dp.resources[VISUALIZATION_NAME]["path"], expt_resource_path)

        self.assertTrue(len(self.dp.datapackage_metadata["views"][0]["spec"].keys()) == 1)
        self.assertTrue(self.dp.datapackage_metadata["views"][0]["spec"]["url"], expt_resource_path)

    def test_add_to_datapackage_datatable_view(self):
        expt_resource_path = f"{os.environ['DATAVERK_API_ENDPOINT']}/{BUCKET_NAME}/resources/{VISUALIZATION_NAME}.json"
        remote_spec_view = JsonSpecView(
            spec_type='datatable',
            name=DATATABLE_VIZ_NAME,
            resources=[],
            attribution="Kilde: NAV.",
            spec=DATATABLE_SPEC
        )

        remote_spec_view.add_to_datapackage(self.dp)

        self.assertTrue(DATATABLE_VIZ_NAME in self.dp.resources.keys())
        self.assertTrue(self.dp.resources[DATATABLE_VIZ_NAME]["path"], expt_resource_path)

        self.assertTrue(len(self.dp.datapackage_metadata["views"][0]["spec"].keys()) == 1)
        self.assertTrue(self.dp.datapackage_metadata["views"][0]["spec"]["url"], expt_resource_path)
