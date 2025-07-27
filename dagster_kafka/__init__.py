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
- Dead Letter Queue (DLQ) support for enterprise error handling
"""

from .resources import KafkaResource
from .resources import SecurityProtocol, SaslMechanism
from .io_manager import KafkaIOManager
from .avro_io_manager import AvroKafkaIOManager, avro_kafka_io_manager
from .protobuf_io_manager import ProtobufKafkaIOManager, protobuf_kafka_io_manager, ProtobufSchemaManager, create_protobuf_kafka_io_manager
from .schema_evolution import SchemaEvolutionValidator, CompatibilityLevel
from .dlq import DLQStrategy, ErrorType, DLQConfiguration, DLQManager, create_dlq_manager, CircuitBreakerState
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

__version__ = "0.9.0"

__all__ = [
    "KafkaResource",
    "SecurityProtocol",
    "SaslMechanism",
    "KafkaIOManager", 
    "AvroKafkaIOManager",
    "avro_kafka_io_manager",
    "ProtobufKafkaIOManager",
    "protobuf_kafka_io_manager",
    "ProtobufSchemaManager",
    "create_protobuf_kafka_io_manager",
    "SchemaEvolutionValidator",
    "CompatibilityLevel",
    "DLQStrategy",
    "ErrorType",
    "DLQConfiguration",
    "DLQManager",
    "create_dlq_manager",
    "CircuitBreakerState",
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