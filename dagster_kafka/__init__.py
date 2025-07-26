"""Dagster Kafka Integration Package

Provides Kafka integration for Dagster data pipelines with support for:
- JSON message consumption
- Avro message consumption with Schema Registry support
- Protobuf message consumption with Schema Registry support
- Schema evolution validation and compatibility checking
- Production-grade error handling and recovery
- Comprehensive monitoring and alerting system
- High-performance caching, batching, and connection pooling
- Configurable consumer groups and connection settings
"""

from .resources import KafkaResource
from .resources import SecurityProtocol, SaslMechanism
from .io_manager import KafkaIOManager
from .avro_io_manager import AvroKafkaIOManager, avro_kafka_io_manager
from .protobuf_io_manager import ProtobufKafkaIOManager, protobuf_kafka_io_manager, ProtobufSchemaManager
from .schema_evolution import SchemaEvolutionValidator, CompatibilityLevel
from .production_utils import (
    ProductionSchemaEvolutionManager, 
    RecoveryStrategy, 
    SchemaEvolutionMetrics,
    with_schema_evolution_monitoring
)
from .monitoring import (
    SchemaEvolutionMonitor,
    AlertSeverity,
    MetricType,
    Alert,
    Metric,
    slack_alert_handler,
    email_alert_handler
)
from .performance import (
    PerformanceOptimizer,
    HighPerformanceCache,
    BatchProcessor,
    ConnectionPool,
    CacheStrategy,
    BatchStrategy,
    PerformanceMetrics
)

__version__ = "0.8.0"

__all__ = [
    "KafkaResource",
    "KafkaIOManager", 
    "AvroKafkaIOManager",
    "avro_kafka_io_manager",
    "ProtobufKafkaIOManager",
    "protobuf_kafka_io_manager",
    "ProtobufSchemaManager",
    "SchemaEvolutionValidator",
    "CompatibilityLevel",
    "ProductionSchemaEvolutionManager",
    "RecoveryStrategy",
    "SchemaEvolutionMetrics",
    "with_schema_evolution_monitoring",
    "SchemaEvolutionMonitor",
    "AlertSeverity",
    "MetricType",
    "Alert",
    "Metric",
    "slack_alert_handler",
    "email_alert_handler",
    "PerformanceOptimizer",
    "HighPerformanceCache",
    "BatchProcessor",
    "ConnectionPool",
    "CacheStrategy",
    "BatchStrategy",
    "PerformanceMetrics",
]