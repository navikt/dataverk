from dataverk.connectors.jsonstat import JSONStatConnector
from dataverk.connectors.storage.google_storage import GoogleStorageConnector
from dataverk.connectors.databases.db2 import Db2Connector
from dataverk.connectors.databases.oracle import OracleConnector
from dataverk.connectors.databases.postgres import PostgresConnector
from dataverk.connectors.databases.sqlite import SqliteConnector
from dataverk.connectors.kafka import KafkaConnector
from dataverk.connectors.storage.nais import NaisS3Connector


__all__ = [
            'JSONStatConnector',
            'NaisS3Connector',
            'GoogleStorageConnector',
            'OracleConnector',
            'KafkaConnector',
            'PostgresConnector',
            'Db2Connector',
            'SqliteConnector'
           ]
