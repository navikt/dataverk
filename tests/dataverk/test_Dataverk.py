import unittest

from dataverk.dataverk import Dataverk
from dataverk.datapackage import Datapackage
from os import environ

class DataverkTest(unittest.TestCase):

    def setUp(self):
        environ["DATAVERK_NO_SETTINGS_SECRETS"] = "true"

    def test_init(self):
        dv = Dataverk("/Users/sondre/Development/dataverk/tests/dataverk/")

    def test_publish(self):
        dv = Dataverk("/Users/sondre/Development/dataverk/tests/dataverk/")
        datapackage = Datapackage({"meta": "data"})
        dv.publish(datapackage=datapackage)

