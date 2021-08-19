import unittest

from dataverk.utils.file_functions import remove_special_characters


class TestFileFunctions(unittest.TestCase):

    def test_remove_special_characters(self):
        test_cases = [
            {"input": "Title with spaces",
             "expected_output": "Title_with_spaces"},
            {"input": "Title with spaces and /slashes/",
             "expected_output": "Title_with_spaces_and_slashes"},
            {"input": "Title with spaces and /slashes/ and comma, at the end.",
             "expected_output": "Title_with_spaces_and_slashes_and_comma_at_the_end"},
            {"input": "Title with @£#$¤€%&{}[]() and 123456789.",
             "expected_output": "Title_with__and_123456789"}
        ]

        for test_case in test_cases:
            output = remove_special_characters(test_case["input"])
            self.assertEqual(test_case["expected_output"], output)
