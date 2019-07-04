# -*- coding: utf-8 -*-
# Import statements
# =================
from unittest import TestCase
from dataverk.utils import mapping_util


class MethodsReturnValues(TestCase):

    def test_safe_get_nested_return_value(self):
        value = "value"
        my_mapping = {"key": {"nested_key": {"nested_nested_key": value}}}
        my_keys = ("key", "nested_key", "nested_nested_key")
        val = mapping_util.safe_get_nested(my_mapping, my_keys, default="default_value")
        self.assertEqual(value, val)

    def test_safe_get_nested_return_default(self):
        value = "value"
        default = "default_value"
        my_mapping = {"key": {"nested_key": {"nested_nested_key": value}}}
        my_keys = ("key2", "nested_key2", "nested_nested_key2")
        val = mapping_util.safe_get_nested(my_mapping, my_keys, default=default)
        self.assertEqual(default, val)
