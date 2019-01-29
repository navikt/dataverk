from kafka import KafkaConsumer
from dataverk.connectors.base import BaseConnector
from collections.abc import Sequence


class KafkaConnector(BaseConnector):

    def __init__(self, topic: str, bootstrap_servers: Sequence, group_id: str=None):
        super().__init__()

        self._consumer = KafkaConsumer(topic,
                                       group_id=group_id,
                                       bootstrap_servers=bootstrap_servers,
                                       consumer_timeout_ms=1000)

    def read_all_messages(self) -> list:
        messages = list()

        for message in self._consumer:
            messages.append(message)

        return messages
