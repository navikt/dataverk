import unittest

from dataverk.views.default import DefaultView
from dataverk.views.json_spec import JsonSpecView
from dataverk.views.remote_spec import RemoteSpecView
from dataverk.views.view_factory import RemoteSpecTypes, get_view_object


class TestViewFactory(unittest.TestCase):

    def test_get_view_object_plotly(self):
        view_object = get_view_object(
            spec_type='plotly',
            name='plotly visualization',
            resources=[],
            attribution="Kilde: NAV.",
            spec={}
        )
        self.assertIsInstance(view_object, JsonSpecView)

    def test_get_view_object_altair(self):
        view_object = get_view_object(
            spec_type="vega",
            title=f"altair visualization",
            name=f"altair visualization",
            description=f"altair visualization",
            spec={},
            resources=""
        )
        self.assertIsInstance(view_object, JsonSpecView)

    def test_get_view_object_default(self):
        view_object = get_view_object(
            spec_type="other",
            title=f"visualization",
            name=f"visualization",
            description=f"visualization",
            spec={},
            resources=""
        )
        self.assertIsInstance(view_object, DefaultView)
