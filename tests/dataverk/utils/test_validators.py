import unittest

from dataverk.utils import validators


class TestValidators(unittest.TestCase):

    def test_validate_bucket_name_ok(self):
        valid_bucket_names = ['my-bucket', 'bucket-something', 'something-bucket-2', '1-something-2-bucket-3']
        for bucket_name in valid_bucket_names:
            validators.validate_bucket_name(bucket_name)

    def test_validate_bucket_name_not_ok(self):
        invalid_bucket_names = ['Bucket', '-bucket', '_bucket', 'bucket_', 'bucket-']
        for bucket_name in invalid_bucket_names:
            with self.assertRaises(NameError):
                validators.validate_bucket_name(bucket_name)
