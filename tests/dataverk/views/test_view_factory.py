import unittest

from dataverk.views.default import DefaultView
from dataverk.views.remote_spec import RemoteSpecView
from dataverk.views.view_factory import RemoteSpecTypes, get_view_object


class TestViewFactory(unittest.TestCase):

    def test_has_spec_type_true(self):
        remote_spec_types = ["vega", "plotly"]
        for spec_type in remote_spec_types:
            with self.subTest(msg="Testing spec type enum", _input=spec_type):
                self.assertTrue(RemoteSpecTypes.has_spec_type(spec_type))

    def test_has_spec_type_false(self):
        is_remote_spec_type = RemoteSpecTypes.has_spec_type("default")
        self.assertFalse(is_remote_spec_type)

    def test_get_view_object_plotly(self):
        view_object = get_view_object(
            spec_type='plotly',
            name='plotly visualization',
            resources=[],
            attribution="Kilde: NAV.",
            spec={}
        )
        self.assertIsInstance(view_object, RemoteSpecView)

    def test_get_view_object_altair(self):
        view_object = get_view_object(
            spec_type="vega",
            title=f"altair visualization",
            name=f"altair visualization",
            description=f"altair visualization",
            spec={},
            resources=""
        )
        self.assertIsInstance(view_object, RemoteSpecView)

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
