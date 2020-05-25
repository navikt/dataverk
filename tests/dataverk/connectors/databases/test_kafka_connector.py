# -*- coding: utf-8 -*-
# Import statements
# =================
import math
import avro
from datetime import datetime
from unittest import TestCase
from dataverk.connectors import KafkaConnector

MOCK_SCHEMA = "{\"type\":\"record\",\"name\":\"testRecord\",\"namespace\":\"no.navikt\",\"doc\":\"Schematest\",\"fields\":[{\"name\":\"RandomNumber\",\"type\":\"int\",\"doc\":\"doc\"},{\"name\":\"RandomNumber2\",\"type\":\"long\",\"doc\":\"doc\"}]}"
MOCK_INVALID_SHCEMA = ""


class MockKafkaConsumer:

    def __init__(self):
        pass


class MockMessage:

    def __init__(self):
        self._timestamp = int(datetime.now().timestamp() * 1000)
        self._value = b'\x00\x00\x00\x00\x05\xf4;\x80\xc6\xeb\xb3\xd5P'

    @property
    def timestamp(self):
        return self._timestamp

    @property
    def value(self):
        return self._value


class Base(TestCase):
    def setUp(self):
        self.settings = {}
        self.topics = []
        self.conn = KafkaConnector(consumer=MockKafkaConsumer(), settings=self.settings, topics=self.topics, fetch_mode="from_beginning")

    def tearDown(self):
        pass


class Instanciation(TestCase):

    def test_instanciation_valid(self):
        settings = {}
        topics = []
        conn = KafkaConnector(consumer=MockKafkaConsumer(), settings=settings, topics=topics, fetch_mode="from_beginning")
        self.assertIsInstance(conn, KafkaConnector)


class MethodsReturnValues(Base):

    def test__is_requested_messages_read(self):
        message = MockMessage()
        res = self.conn._is_requested_messages_read(message=message, max_mesgs=math.inf, mesgs_read=1)
        self.assertTrue(res)

        res = self.conn._is_requested_messages_read(message=message, max_mesgs=10, mesgs_read=10)
        self.assertTrue(res)

        self.conn._read_until_timestamp = self.conn._get_current_timestamp_in_ms() + 1
        res = self.conn._is_requested_messages_read(message=message, max_mesgs=math.inf, mesgs_read=1)
        self.assertFalse(res)

    def test__decode_avro_message(self):
        message = MockMessage()
        mesg = self.conn._decode_avro_message(MOCK_SCHEMA, message)

        self.assertTrue("RandomNumber" in mesg.keys())
        self.assertTrue("RandomNumber2" in mesg.keys())
        self.assertEqual(mesg["RandomNumber"], 3834)
        self.assertEqual(mesg["RandomNumber2"], 1385852400000)

    def test__decode_avro_message_invalid_schema(self):
        message = MockMessage()
        with self.assertRaises(avro.schema.SchemaParseException):
            mesg = self.conn._decode_avro_message(MOCK_INVALID_SHCEMA, message)

    def test__extract_requested_fields(self):
        mesg = MockMessage()
        decoded_mesg = self.conn._decode_avro_message(MOCK_SCHEMA, mesg)
        fields = ["RandomNumber", "RandomNumber2"]
        res = self.conn._extract_requested_fields(mesg=decoded_mesg, fields=fields)
        self.assertTrue(None not in res)

    def test__extract_requested_fields_not_found(self):
        mesg = MockMessage()
        decoded_mesg = self.conn._decode_avro_message(MOCK_SCHEMA, mesg)
        fields = ["NoneExistantField"]
        res = self.conn._extract_requested_fields(decoded_mesg, fields)
        self.assertTrue(None in res)
