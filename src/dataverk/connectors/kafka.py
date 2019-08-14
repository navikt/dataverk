import math

import pandas as pd
import json
import struct
import requests
import avro
import avro.schema
import avro.io
import io
import time

from dataverk.utils import mapping_util
from kafka import KafkaConsumer
from collections.abc import Mapping, Sequence
from enum import Enum
from datetime import datetime
from dataverk.connectors import BaseConnector
import streamz


class KafkaFetchMode(Enum):
    FROM_BEGINNING = "from_beginning"
    LAST_COMMITED_OFFSET = "last_commited_offset"


class KafkaConnector(BaseConnector):

    def __init__(self, consumer: KafkaConsumer, settings: Mapping, topics: Sequence, fetch_mode: str):
        """ Dataverk Kafka consumer class

        :param settings: Mapping object containing project settings
        :param topics: Sequence of topics to subscribe to
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset)
        """
        super().__init__()
        self._topics = topics
        self._fetch_mode = fetch_mode
        self._schema_cache = {}
        self._consumer = consumer
        self.log(f"KafkaConsumer created with fetch mode set to '{fetch_mode}'")
        self._read_until_timestamp = self._get_current_timestamp_in_ms()
        self._schema_registry_url = mapping_util.safe_get_nested(settings, keys=("kafka", "schema_registry"), default="http://localhost:8081")

    def get_pandas_df(self, strategy=None, fields=None, max_mesgs=math.inf):
        """ Read kafka topics, commits offset and returns result as pandas dataframe

        :return: pd.Dataframe containing kafka messages read. NB! Commits offset
        """
        if strategy is None:
            records = self._read_kafka_raw(max_mesgs, fields)
        else:
            records = self._read_kafka_accumulated(max_mesgs, strategy)
        try:
            df = pd.DataFrame.from_records(records)
        except ValueError:
            df = pd.DataFrame.from_records(records, index=[0])
        self._commit_offsets()

        return df

    def get_message_fields(self):
        """ Read single kafka message from topic and return message fields

        :return: list: message fields
        """
        for message in self._consumer:
            try:
                schema_res = self._get_schema_from_registry(message=message)
                schema = schema_res.json()["schema"]
            except (AttributeError, KeyError):
                mesg = json.loads(message.value.decode('utf8'))
            else:
                mesg = self._decode_avro_message(schema=schema, message=message)

            return mesg.keys()

    def _read_kafka_raw(self, max_mesgs, fields):
        start_time = time.time()

        self.log(f"Reading kafka stream {self._topics}. Fetch mode {self._fetch_mode}")

        data = list()

        for message in self._consumer:
            mesg = self._parse_kafka_message(message)
            data.append(self._extract_requested_fields(mesg, fields))
            if self._is_requested_messages_read(message, max_mesgs, len(data)):
                break

        self.log(f"({len(data)} messages read from kafka stream {self._topics} in {time.time() - start_time} sec. Fetch mode {self._fetch_mode}")

        return data

    def _read_kafka_accumulated(self, max_mesgs, strategy):
        data = {}
        mesg_count = 0
        stream = streamz.Stream()
        acc = stream.accumulate(strategy, start=data)

        for message in self._consumer:
            mesg = self._parse_kafka_message(message)
            mesg_count += 1
            stream.emit(mesg)
            if self._is_requested_messages_read(message, max_mesgs, mesg_count):
                break
        return data

    def _parse_kafka_message(self, message):
        try:
            schema_res = self._get_schema_from_registry(message=message)
            schema = schema_res.json()["schema"]
        except (AttributeError, KeyError):
            return json.loads(message.value.decode('utf8'))
        else:
            return self._decode_avro_message(schema=schema, message=message)

    def _get_schema_from_registry(self, message):
        schema_id = struct.unpack(">L", message.value[1:5])[0]
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]
        else:
            schema = requests.get(self._schema_registry_url + '/schemas/ids/' + str(schema_id))
            self._schema_cache[schema_id] = schema
            return schema

    @staticmethod
    def _decode_avro_message(schema, message):
        schema = avro.schema.Parse(schema)
        bytes_reader = io.BytesIO(message.value[5:])
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        return reader.read(decoder)

    @staticmethod
    def _get_current_timestamp_in_ms():
        return int(datetime.now().timestamp() * 1000)

    def _is_requested_messages_read(self, message, max_mesgs, mesgs_read):
        if message.timestamp >= self._read_until_timestamp:
            return True
        elif mesgs_read >= max_mesgs:
            return True
        return False

    def _commit_offsets(self):
        """ Commits the offsets to kafka when the KafkaConsumer object is configured with group_id
        """
        if self._consumer.config["group_id"] is not None:
            self._consumer.commit()

    def _extract_requested_fields(self, mesg, fields):
        if fields is not None:
            return [self._extract_field(mesg, field) for field in fields]
        else:
            return mesg

    @staticmethod
    def _extract_field(mesg, field):
        try:
            return mesg[field]
        except KeyError:
            return None


def get_kafka_consumer(settings: Mapping, topics: Sequence, fetch_mode: str) -> KafkaConsumer:
    """ Factory method returning a KafkaConsumer object with desired configuration

    :param settings: Mapping object containing project settings
    :param topics: Sequence of topics to subscribe
    :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset)
    :return: KafkaConsumer object with desired configuration
    """
    if KafkaFetchMode(fetch_mode) is KafkaFetchMode.FROM_BEGINNING:
        group_id = None
    elif KafkaFetchMode(fetch_mode) is KafkaFetchMode.LAST_COMMITED_OFFSET:
        group_id = settings["kafka"].get("group_id", None)
    else:
        raise ValueError(f"{fetch_mode} is not a valid KafkaFetchMode. Valid fetch_modes are: "
                         f"'{KafkaFetchMode.FROM_BEGINNING}' and '{KafkaFetchMode.LAST_COMMITED_OFFSET}'")

    return KafkaConsumer(*topics,
                         group_id=group_id,
                         bootstrap_servers=mapping_util.safe_get_nested(settings, keys=("kafka", "brokers"), default="localhost:9092"),
                         security_protocol=mapping_util.safe_get_nested(settings, keys=("kafka", "security_protocol"), default="PLAINTEXT"),
                         sasl_mechanism=mapping_util.safe_get_nested(settings, keys=("kafka", "sasl_mechanism"), default=None),
                         sasl_plain_username=mapping_util.safe_get_nested(settings, keys=("kafka", "sasl_plain_username"), default=None),
                         sasl_plain_password=mapping_util.safe_get_nested(settings, keys=("kafka", "sasl_plain_password"), default=None),
                         ssl_cafile=mapping_util.safe_get_nested(settings, keys=("kafka", "ssl_cafile"), default=None),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         consumer_timeout_ms=15000)
