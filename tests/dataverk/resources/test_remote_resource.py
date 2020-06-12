import unittest


from dataverk.resources.remote_resource import RemoteResource

resource_name_in = "resource"

resource_fmt = "csv"
resource_fmt_zipped = "csv.gz"

resource_url = f"https://remote.storage.location.com/bucket/" \
               f"datapackage/resources/{resource_name_in}.{resource_fmt}"

resource_url_zipped = f"https://remote.storage.location.com/bucket/" \
                      f"datapackage/resources/{resource_name_in}.{resource_fmt_zipped}"

resource_url_invalid = f"/not/a/web/url/bucket/datapackage/resources/{resource_name_in}.csv.gz"

path = "https://some.bucket.storage.com"
resource_description = "A remote resource"
spec = None
compress = False

expected_schema = {
            'name': resource_name_in,
            'description': resource_description,
            'path': resource_url,
            'format': resource_fmt,
            'spec': spec
        }


class TestMethodReturnValues(unittest.TestCase):

    def setUp(self):
        self.resource_formatter = RemoteResource(resource=resource_url,
                                                 resource_description=resource_description, datapackage_path=path,
                                                 fmt="", compress=compress, spec=spec)

    def test__get_schema(self):
        actual_schema = self.resource_formatter.get_schema()
        self.assertEqual(expected_schema, actual_schema)

    def test__resource_name_and_type_from_url(self):
        actual_resource_name, actual_resource_fmt = self.resource_formatter. \
            _resource_name_and_type_from_url(resource_url)

        self.assertEqual(resource_name_in, actual_resource_name)
        self.assertEqual(resource_fmt, actual_resource_fmt)

    def test__resource_name_and_type_from_url_zipped(self):
        actual_resource_name, actual_resource_fmt = self.resource_formatter.\
            _resource_name_and_type_from_url(resource_url_zipped)

        self.assertEqual(resource_name_in, actual_resource_name)
        self.assertEqual(resource_fmt_zipped, actual_resource_fmt)

    def test__resource_name_and_type_from_url_invalid(self):
        with self.assertRaises(ValueError):
            self.resource_formatter._resource_name_and_type_from_url(resource_url_invalid)
