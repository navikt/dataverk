import unittest


from dataverk.resources.pdf_resource import PDFResource

resource = "A pdf binary string"
resource_name_in = "pdf resource"
resource_description = "A pdf resource"
resource_fmt = "pdf"
path = "https://some.bucket.storage.com"
media_type = "application/pdf"
spec = None
compress = False

expected_resource_name = resource_name_in.replace(" ", "_")
expected_schema = {
            'name': expected_resource_name,
            'description': resource_description,
            'path': f'{path}/resources/{expected_resource_name}.{resource_fmt}',
            'format': resource_fmt,
            'mediatype': media_type,
            'spec': spec
        }


class TestMethodReturnValues(unittest.TestCase):

    def setUp(self):
        self.resource_formatter = PDFResource(resource=resource, resource_name=resource_name_in,
                                              resource_description=resource_description, datapackage_path=path,
                                              fmt="pdf", compress=compress, spec=spec)

    def test__resource_path(self):
        actual_path = self.resource_formatter._resource_path()
        expected_path = expected_schema.get('path')
        self.assertEqual(expected_path, actual_path)

    def test__formatted_resource_name(self):
        actual_formatted_name = self.resource_formatter.formatted_resource_name()
        expected_formatted_name = expected_schema.get('name')
        self.assertEqual(expected_formatted_name, actual_formatted_name)

    def test__get_schema(self):
        actual_schema = self.resource_formatter.get_schema()
        self.assertEqual(expected_schema, actual_schema)
