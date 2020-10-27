import unittest

from dataverk.utils.file_functions import url_encode


class TestFileFunctions(unittest.TestCase):

    def test_url_encode(self):
        test_cases = [
            {"input": "Title with spaces",
             "expected_output": "Title_with_spaces"},
            {"input": "Title with spaces and /slashes/",
             "expected_output": "Title_with_spaces_and__slashes_"},
            {"input": "Title with spaces and /slashes/ and comma, at the end.",
             "expected_output": "Title_with_spaces_and__slashes__and_comma__at_the_end."},
        ]

        for test_case in test_cases:
            output = url_encode(test_case["input"])
            self.assertEqual(test_case["expected_output"], output)
