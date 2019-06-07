# -*- coding: utf-8 -*-
# Import statements
# =================
import math
from datetime import datetime
from unittest import TestCase
from dataverk.connectors import KafkaConnector


class MockKafkaConsumer:

    def __init__(self):
        pass


class MockMessage:

    def __init__(self):
        self._timestamp = int(datetime.now().timestamp() * 1000)

    @property
    def timestamp(self):
        return self._timestamp


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
