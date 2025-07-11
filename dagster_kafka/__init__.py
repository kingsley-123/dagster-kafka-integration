"""Dagster Kafka Integration Package

Provides Kafka integration for Dagster data pipelines with support for:
- JSON message consumption
- Avro message consumption with Schema Registry support
- Configurable consumer groups and connection settings
"""

from .resources import KafkaResource
from .io_manager import KafkaIOManager
from .avro_io_manager import AvroKafkaIOManager, avro_kafka_io_manager

__version__ = "0.2.0"

__all__ = [
    "KafkaResource",
    "KafkaIOManager",
    "AvroKafkaIOManager",
    "avro_kafka_io_manager",
]