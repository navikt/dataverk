import unittest

from dataverk.dataverk_context import DataverkContext


class TestDataverkContext(unittest.TestCase):
    def setUp(self) -> None:
        self.context = DataverkContext({})

    def test__load_settings(self):
        settings = self.context._load_settings()
        expected_settings = {}
        self.assertEqual(expected_settings, settings)
