import pandas as pd
import json
from kafka import KafkaConsumer
from collections import Mapping, Sequence
from enum import Enum
from datetime import datetime


class KafkaFetchMode(Enum):
    FROM_BEGINNING = "from_beginning"
    LAST_COMMITED_OFFSET = "last_commited_offset"


class DVKafkaConsumer:

    def __init__(self, settings: Mapping, topics: Sequence, fetch_mode: str):
        """ Dataverk Kafka consumer class

        :param settings: Mapping object containing project settings
        :param topics: Sequence of topics to subscribe to
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset)
        """
        self._consumer = self._get_kafka_consumer(settings=settings, topics=topics, fetch_mode=fetch_mode)
        self._read_until_timestamp = self._get_current_timestamp_in_ms()

    def get_pandas_df(self):
        """ Read kafka topics and return result as pandas dataframe

        :return: pd.Dataframe containing kafka messages read
        """
        df = pd.DataFrame()

        for message in self._consumer:
            df = self._append_to_df(df=df, message_value=message.value.decode("utf-8"))
            if self._is_requested_messages_read(message):
                break

        self._commit_offsets()

        return df

    def _get_kafka_consumer(self, settings: Mapping, topics: Sequence, fetch_mode: str) -> KafkaConsumer:
        """ Factory method returning a KafkaConsumer object with desired configuration

        :param settings: Mapping object containing project settings
        :param topics: Sequence of topics to subscribe
        :param fetch_mode: str describing fetch mode (from_beginning, last_committed_offset)
        :return:
        """
        if KafkaFetchMode(fetch_mode) is KafkaFetchMode.FROM_BEGINNING:
            group_id = None
        elif KafkaFetchMode(fetch_mode) is KafkaFetchMode.LAST_COMMITED_OFFSET:
            group_id = settings["kafka"].get("group_id", None)
        else:
            raise ValueError(f'{fetch_mode} is not a valid KafkaFetchMode. Valid fetch_modes are: '
                             f'"{KafkaFetchMode.FROM_BEGINNING}" and "{KafkaFetchMode.LAST_COMMITED_OFFSET}"')

        return KafkaConsumer(*topics,
                             group_id=group_id,
                             bootstrap_servers=settings["kafka"].get("brokers", "localhost:9092"),
                             security_protocol=settings["kafka"].get("security_protocol", "PLAINTEXT"),
                             sasl_mechanism=settings["kafka"].get("sasl_mechanism", None),
                             sasl_plain_username=settings["kafka"].get("sasl_plain_username", None),
                             sasl_plain_password=settings["kafka"].get("sasl_plain_password", None),
                             ssl_cafile=settings["kafka"].get("ssl_cafile", None),
                             auto_offset_reset='earliest',
                             enable_auto_commit=False,
                             consumer_timeout_ms=1000)

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
