# -*- coding: utf-8 -*-
# Import statements
# =================
from unittest import TestCase
from dataverk.utils import validators


class MethodsInput(TestCase):
    """
    Tests methods which take input parameters

    Tests include: passing invalid input, etc.
    """

    # Input arguments valid

    def test_validate_bucket_name_valid(self):
        valid_bucket_names = ["name", "name-with-separator"]

        for name in valid_bucket_names:
            with self.subTest(msg="Valid bucket name", _input=name):
                validators.validate_bucket_name(name)

    # Input arguments invalid

    def test_validate_bucket_name_invalid(self):
        invalid_bucket_names = ["_name", "-name", "name with spaces", "name_", "name-", "Name", "name_with_underscore"]

        for name in invalid_bucket_names:
            with self.subTest(msg="Invalid bucket name", _input=name):
                with self.assertRaises(NameError):
                    validators.validate_bucket_name(name)
