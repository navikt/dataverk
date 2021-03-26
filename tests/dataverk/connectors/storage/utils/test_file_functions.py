import unittest

from dataverk.utils.file_functions import format_filename


class TestFileFunctions(unittest.TestCase):

    def test_format_filename(self):
        test_cases = [
            {"input": "Title with spaces",
             "expected_output": "Title_with_spaces"},
            {"input": "Title with spaces and /slashes/",
             "expected_output": "Title_with_spaces_and__slashes_"},
            {"input": "Title with spaces and /slashes/ and comma, at the end.",
             "expected_output": "Title_with_spaces_and__slashes__and_comma__at_the_end."},
        ]

        for test_case in test_cases:
            output = format_filename(test_case["input"])
            self.assertEqual(test_case["expected_output"], output)
