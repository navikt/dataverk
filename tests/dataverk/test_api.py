# -*- coding: utf-8 -*-
# Import statements
# =================
import unittest
import dataverk
from dataverk.connectors.oracle import OracleConnector
from dataverk.connectors.postgres import PostgresConnector
from dataverk.connectors.sqlite import SQLiteConnector

# Common input parameters
# =======================
SETTINGS = {
    "db_connection_strings": {
        "oracle": "oracle://user:password@host:8080/service",
        "postgres": "postgres://user:password@host:8080/service"
    }
}


# Base classes
# ============
class Base(unittest.TestCase):
    """
    Base class for tests

    This class defines a common `setUp` method that defines attributes which are used in the various tests.
    """
    def setUp(self):
        pass


# Test classes
# ============
class Instantiation(Base):
    """
    Tests all aspects of instantiation

    Tests include: instantiation with args of wrong type, instantiation with input values outside constraints, etc.
    """
    pass

    # Input arguments wrong type
    # ==========================

    # Input arguments outside constraints
    # ===================================


class Set(Base):
    """
    Tests all aspects of setting attributes

    Tests include: setting attributes of wrong type, setting attributes outside their constraints, etc.
    """
    pass

    # Set attribute wrong type
    # ========================

    # Set attribute outside constraint
    # ================================


class MethodsInput(Base):
    """
    Tests methods which take input parameters

    Tests include: passing invalid input, etc.
    """
    pass


class MethodsReturnType(Base):
    """
    Tests methods' output types
    """

    def test__is_sql_file(self):
        sql_path = "./myquery.sql"
        sql_query = "SELECT * FROM mytable"
        self.assertEqual(dataverk.api._is_sql_file(source=sql_path), True)
        self.assertEqual(dataverk.api._is_sql_file(source=sql_query), False)

    def test__get_db_connector_valid(self):
        connector_types = [("Oracle", "oracle", OracleConnector), ("Postgres", "postgres", PostgresConnector), ("Sqllite", ":memory:", SQLiteConnector)]

        for connector_type in connector_types:
            with self.subTest(msg="Testing sql connector type factory method", _input=connector_type):
                self.assertIsInstance(dataverk.api._get_db_connector(settings_store=SETTINGS, connector=connector_type[0], source=connector_type[1]), connector_type[2])

    def test__get_db_connector_invalid(self):
        invalid_connector_type = "InvalidSqlConnector"

        with self.assertRaises(NotImplementedError):
            dataverk.api._get_db_connector(settings_store=SETTINGS, connector=invalid_connector_type, source="connection")


class MethodsReturnUnits(Base):
    """
    Tests methods' output units where applicable
    """
    pass


class MethodsReturnValues(Base):
    """
    Tests values of methods against known values
    """
    pass