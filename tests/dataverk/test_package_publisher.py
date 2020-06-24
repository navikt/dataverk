import gzip
from unittest import TestCase
import io

from dataverk.utils import file_functions

from dataverk import Datapackage
from dataverk.package_publisher import PackagePublisher
from tests.testing_resources import MOCK_SETTINGS, MOCK_METADATA


MOCK_ENVSTORE = {}


class TestPackagePublisher(TestCase):

    def test_init(self):
        pp = PackagePublisher(settings_store=MOCK_SETTINGS, dp=Datapackage(MOCK_METADATA), env_store=MOCK_ENVSTORE)

    def test__is_publish_set(self):
        pp = PackagePublisher(settings_store=MOCK_SETTINGS, dp=Datapackage(MOCK_METADATA), env_store=MOCK_ENVSTORE)

    def test__compress_content(self):
        orig_data = "col1;col2;col3 value;1;2.3"
        data_buff = io.StringIO()
        data_buff.write(orig_data)
        gz_buff = file_functions.compress_content(data_buff)
        data = gzip.decompress(gz_buff).decode("utf-8-sig")
        self.assertEqual(data, orig_data)
