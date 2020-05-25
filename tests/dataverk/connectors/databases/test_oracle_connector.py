# -*- coding: utf-8 -*-
# Import statements
# =================
from unittest import TestCase
from dataverk.connectors import OracleConnector

settings = {
    "db_connection_strings": {
        "oracle": "oracle://user:password@host.no:1234/db"
    }
}


expected_conn_string = "oracle://user:password@host.no:1234/?service_name=db"


class MethodsReturnValues(TestCase):

    def test__format_connection_string(self):
        con = OracleConnector(settings, "oracle")
        conn_string = con._format_connection_string(settings["db_connection_strings"]["oracle"])
        self.assertEqual(conn_string, expected_conn_string)

