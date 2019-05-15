from unittest import TestCase

from dataverk.package_publisher import PackagePublisher
from tests.testing_resources import MOCK_SETTINGS, MOCK_METADATA


MOCK_ENVSTORE = {}


class TestPackagePublisher(TestCase):

    def test_init(self):
        pp = PackagePublisher(settings_store=MOCK_SETTINGS, datapackage_metadata=MOCK_METADATA, env_store=MOCK_ENVSTORE)

    def test__is_publish_set(self):
        pp = PackagePublisher(settings_store=MOCK_SETTINGS, datapackage_metadata=MOCK_METADATA, env_store=MOCK_ENVSTORE)


    def test_publish(self):
        pp = PackagePublisher(settings_store=MOCK_SETTINGS, datapackage_metadata=MOCK_METADATA, env_store=MOCK_ENVSTORE)
        pp.publish("Resource")

    def test_upload_to_storage_bucket(self):
        self.fail()
