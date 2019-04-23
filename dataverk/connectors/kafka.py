import pandas as pd
import json
import struct
import requests
import avro
import avro.io
import io
import time
from kafka import KafkaConsumer
from collections import Mapping, Sequence
from enum import Enum
from datetime import datetime
from dataverk.connectors import BaseConnector


class KafkaFetchMode(Enum):
    FROM_BEGINNING = "from_beginning"
    LAST_COMMITED_OFFSET = "last_commited_offset"


class KafkaConnector(BaseConnector):

    def __init__(self, settings: Mapping, topics: Sequence, fetch_mode: str):
        """ Dataverk Kafka consumer class

        :param settings: Mapping object containing project settings
        :param topics: Sequence of topics to subscribe to
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset)
        """
        super().__init__()
        self._topics = topics
        self._fetch_mode = fetch_mode
        self._schema_cache = {}
        self._consumer = self._get_kafka_consumer(settings=settings, topics=topics, fetch_mode=fetch_mode)
        self.log(f"KafkaConsumer created with fetch mode set to '{fetch_mode}'")
        self._read_until_timestamp = self._get_current_timestamp_in_ms()
        self._schema_registry_url = self._safe_get_nested(settings=settings, keys=("kafka", "schema_registry"), default="http://localhost:8081")

    def get_pandas_df(self, numb_of_msgs=None):
        """ Read kafka topics, commits offset and returns result as pandas dataframe

        :return: pd.Dataframe containing kafka messages read. NB! Commits offset
        """
        df = pd.DataFrame.from_dict(self._read_kafka())
        self._commit_offsets()

        return df
        
    def _read_kafka(self):

        start_time = time.time()

        self.log(f"Reading kafka stream {self._topics}. Fetch mode {self._fetch_mode}")

        data = list()

        for message in self._consumer:
            try:
                schema_res = self._get_schema_from_registry(message=message)
                schema = schema_res.json()["schema"]
            except (AttributeError, KeyError):
                mesg = json.loads(message.value.decode('utf8'))
            else:
                mesg = self._decode_avro_message(schema=schema, message=message)

            data.append(mesg)
            if self._is_requested_messages_read(message):
                break

        self.log(f"({len(data)} messages read from kafka stream {self._topics} in {time.time() - start_time} sec. Fetch mode {self._fetch_mode}")

        return data

    def _get_schema_from_registry(self, message):
        schema_id = struct.unpack(">L", message.value[1:5])[0]
        if schema_id in self._schema_cache:
            return self._schema_cache[schema_id]
        else:
            schema = requests.get(self._schema_registry_url + '/schemas/ids/' + str(schema_id))
            self._schema_cache[schema_id] = schema
            return schema

    def _decode_avro_message(self, schema, message):
        schema = avro.schema.Parse(schema)
        bytes_reader = io.BytesIO(message.value[5:])
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(schema)
        return reader.read(decoder)

    def _get_kafka_consumer(self, settings: Mapping, topics: Sequence, fetch_mode: str) -> KafkaConsumer:
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
                             bootstrap_servers=self._safe_get_nested(settings=settings, keys=("kafka", "brokers"), default="localhost:9092"),
                             security_protocol=self._safe_get_nested(settings=settings, keys=("kafka", "security_protocol"), default="PLAINTEXT"),
                             sasl_mechanism=self._safe_get_nested(settings=settings, keys=("kafka", "sasl_mechanism"), default=None),
                             sasl_plain_username=self._safe_get_nested(settings=settings, keys=("kafka", "sasl_plain_username"), default=None),
                             sasl_plain_password=self._safe_get_nested(settings=settings, keys=("kafka", "sasl_plain_password"), default=None),
                             ssl_cafile=self._safe_get_nested(settings=settings, keys=("kafka", "ssl_cafile"), default=None),
                             auto_offset_reset='earliest',
                             enable_auto_commit=False,
                             consumer_timeout_ms=15000)

    def _safe_get_nested(self, settings: Mapping, keys: tuple, default):
        for key in keys:
            if key not in settings:
                return default
            else:
                settings = settings[key]
        return settings

    def _get_current_timestamp_in_ms(self):
        return int(datetime.now().timestamp() * 1000)

    def _append_to_df(self, df: pd.DataFrame, message_value):
        return pd.concat([df, pd.DataFrame(json.loads(message_value), index=[0])], ignore_index=True)

    def _is_requested_messages_read(self, message):
        return message.timestamp >= self._read_until_timestamp

    def _commit_offsets(self):
        """ Commits the offsets to kafka when the KafkaConsumer object is configured with group_id
        """
        if self._consumer.config["group_id"] is not None:
            self._consumer.commit()
