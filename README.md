# Dagster Kafka Integration

The first and most comprehensive Kafka integration for Dagster with complete enterprise-grade features supporting all three major serialization formats.

## Complete Enterprise Solution

**Version 0.7.0** - The definitive Kafka integration with full serialization support:

- **JSON Support**: Native JSON message consumption from Kafka topics
- **Avro Support**: Full Avro message support with Schema Registry integration  
- **Protobuf Support**: Complete Protocol Buffers integration with schema management
- **Schema Evolution**: Comprehensive validation with breaking change detection across all formats
- **Production Monitoring**: Real-time alerting with Slack/Email integration
- **High Performance**: Advanced caching, batching, and connection pooling
- **Error Recovery**: Multiple recovery strategies for production resilience
- **Enterprise Ready**: Complete observability and production-grade error handling

**81 comprehensive tests passing** - Full test coverage across all serialization formats and enterprise features.

## Three Serialization Formats Supported

### JSON Support
Perfect for APIs and simple data structures.

### Avro Support 
Schema Registry integration with evolution validation.

### Protobuf Support
High-performance binary serialization with comprehensive tooling.

## Installation

```bash
pip install git+https://github.com/kingsley-123/dagster-kafka-integration.git
```

## Quick Start

### JSON Usage

```python
from dagster import asset, Definitions
from dagster_kafka import KafkaResource, KafkaIOManager

@asset
def api_events():
    """Consume JSON messages from Kafka topic."""
    pass

defs = Definitions(
    assets=[api_events],
    resources={
        "kafka": KafkaResource(bootstrap_servers="localhost:9092"),
        "io_manager": KafkaIOManager(
            kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
            consumer_group_id="my-dagster-pipeline"
        )
    }
)
```

### Avro Usage with Schema Registry

```python
from dagster import asset, Config
from dagster_kafka import KafkaResource, avro_kafka_io_manager

class UserEventsConfig(Config):
    schema_file: str = "schemas/user.avsc"
    max_messages: int = 100

@asset(io_manager_key="avro_kafka_io_manager")
def user_data(context, config: UserEventsConfig):
    """Load user events using Avro schema with validation."""
    io_manager = context.resources.avro_kafka_io_manager
    return io_manager.load_input(
        context,
        topic="user-events",
        schema_file=config.schema_file,
        max_messages=config.max_messages,
        validate_evolution=True
    )
```

### Protobuf Usage

```python
from dagster import asset, Definitions
from dagster_kafka import KafkaResource
from dagster_kafka.protobuf_io_manager import create_protobuf_kafka_io_manager

@asset(io_manager_key="protobuf_kafka_io_manager")
def user_events():
    """Consume Protobuf messages from Kafka topic."""
    pass

@asset
def processed_data(user_events):
    """Process Protobuf user events."""
    print(f"Processing {len(user_events)} Protobuf events")
    return {"processed_count": len(user_events)}

defs = Definitions(
    assets=[user_events, processed_data],
    resources={
        "protobuf_kafka_io_manager": create_protobuf_kafka_io_manager(
            kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
            schema_registry_url="http://localhost:8081",  # Optional
            consumer_group_id="dagster-protobuf-pipeline"
        )
    }
)
```

### Advanced Protobuf with Schema Registry

```python
from dagster_kafka.protobuf_io_manager import ProtobufKafkaIOManager

protobuf_manager = ProtobufKafkaIOManager(
    kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
    schema_registry_url="http://localhost:8081",
    enable_schema_validation=True,
    compatibility_level="BACKWARD",
    consumer_group_id="enterprise-protobuf-consumer"
)
```

## All Three Formats in One Pipeline

```python
from dagster import Definitions
from dagster_kafka import KafkaResource, KafkaIOManager, avro_kafka_io_manager
from dagster_kafka.protobuf_io_manager import create_protobuf_kafka_io_manager

defs = Definitions(
    assets=[json_events, avro_events, protobuf_events, unified_processing],
    resources={
        "json_io_manager": KafkaIOManager(
            kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
            consumer_group_id="json-consumer"
        ),
        "avro_io_manager": avro_kafka_io_manager.configured({
            "schema_registry_url": "http://localhost:8081",
            "enable_schema_validation": True
        }),
        "protobuf_io_manager": create_protobuf_kafka_io_manager(
            kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
            schema_registry_url="http://localhost:8081"
        )
    }
)
```

## Schema Examples

### Avro Schema

```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example.users",
  "fields": [
    {"name": "id", "type": "int"},
    {"name": "name", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "created_at", "type": "long"},
    {"name": "is_active", "type": "boolean"}
  ]
}
```

### Protobuf Schema

```protobuf
syntax = "proto3";

package examples;

message User {
  int32 id = 1;
  string name = 2;
  string email = 3;
  int32 age = 4;
  bool is_active = 5;
  repeated string tags = 6;
  Address address = 7;
  int64 created_at = 8;
  int64 updated_at = 9;
}

message Address {
  string street = 1;
  string city = 2;
  string state = 3;
  string postal_code = 4;
  string country = 5;
}

enum EventType {
  USER_CREATED = 0;
  USER_UPDATED = 1;
  USER_DELETED = 2;
  USER_LOGIN = 3;
  USER_LOGOUT = 4;
}

message UserEvent {
  EventType event_type = 1;
  User user = 2;
  int64 timestamp = 3;
  string source_system = 4;
  map<string, string> metadata = 5;
}
```

## Schema Evolution Management

### Compatibility Levels

Support for all major compatibility levels across JSON, Avro, and Protobuf:

- **BACKWARD**: New schema can read old data
- **FORWARD**: Old schema can read new data  
- **FULL**: Both backward and forward compatible
- **BACKWARD_TRANSITIVE**: Compatible with all previous versions
- **FORWARD_TRANSITIVE**: Compatible with all future versions
- **FULL_TRANSITIVE**: Both backward and forward transitive
- **NONE**: No compatibility checking

### Breaking Change Detection

```python
from dagster_kafka import SchemaEvolutionValidator

validator = SchemaEvolutionValidator(schema_registry_client)

result = validator.validate_schema_compatibility(
    "user-events-value",
    new_schema,
    CompatibilityLevel.BACKWARD
)

if not result["compatible"]:
    print(f"Breaking changes detected: {result['reason']}")
```

## Production Monitoring and Alerting

### Real-time Monitoring

```python
from dagster_kafka import SchemaEvolutionMonitor, slack_alert_handler

monitor = SchemaEvolutionMonitor()
monitor.add_alert_callback(slack_alert_handler("https://hooks.slack.com/your-webhook"))

monitor.record_validation_attempt(
    subject="user-events",
    success=True,
    duration=2.5,
    breaking_changes_count=0
)
```

### Performance Optimization

```python
from dagster_kafka import PerformanceOptimizer, CacheStrategy, BatchStrategy

optimizer = PerformanceOptimizer(
    cache_config={
        "max_size": 10000,
        "strategy": CacheStrategy.LRU,
        "ttl_seconds": 300
    },
    batch_config={
        "strategy": BatchStrategy.ADAPTIVE,
        "max_batch_size": 1000
    },
    pool_config={
        "max_connections": 20
    }
)
```

## Configuration Options

### KafkaResource

```python
KafkaResource(
    bootstrap_servers="localhost:9092",  # Required: Kafka cluster endpoints
)
```

### Advanced AvroKafkaIOManager Configuration

```python
avro_kafka_io_manager.configured({
    "schema_registry_url": "http://localhost:8081",
    "enable_schema_validation": True,
    "compatibility_level": "BACKWARD",
    "enable_caching": True,
    "cache_ttl": 300,
    "max_retries": 3,
    "retry_backoff": 1.0
})
```

### Protobuf Configuration Options

```python
# Simple Protobuf usage
simple_manager = create_protobuf_kafka_io_manager(
    kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
    consumer_group_id="my-protobuf-consumer"
)

# Advanced Protobuf with Schema Registry
advanced_manager = ProtobufKafkaIOManager(
    kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
    schema_registry_url="http://localhost:8081",
    enable_schema_validation=True,
    compatibility_level="BACKWARD",
    consumer_group_id="enterprise-protobuf"
)
```

## Examples Directory Structure

```
examples/
├── json_examples/              # JSON message examples
│   ├── simple_json_test.py
│   └── README.md
├── avro_examples/              # Avro schema examples
│   ├── simple_avro_test.py
│   ├── production_schema_migration.py
│   ├── schemas/
│   └── README.md
├── protobuf_examples/          # Protobuf examples
│   ├── simple_protobuf_example.py
│   ├── advanced_protobuf_example.py
│   ├── schemas/
│   │   ├── user.proto
│   │   └── product.proto
│   └── README.md
├── performance_examples/       # Performance optimization
├── production_examples/        # Enterprise deployment patterns
└── docker-compose.yml         # Local testing setup
```

## Serialization Format Comparison

| Feature | JSON | Avro | Protobuf |
|---------|------|------|----------|
| **Schema Evolution** | Basic | Advanced | Advanced |
| **Performance** | Good | Better | Best |
| **Schema Registry** | No | Yes | Yes |
| **Backward Compatibility** | Manual | Automatic | Automatic |
| **Binary Format** | No | Yes | Yes |
| **Human Readable** | Yes | No | No |
| **Cross-Language** | Yes | Yes | Yes |
| **Use Case** | APIs, Logging | Analytics, ETL | High-perf, gRPC |

## Development & Testing

### Local Development Setup

```bash
git clone https://github.com/kingsley-123/dagster-kafka-integration.git
cd dagster-kafka-integration

# Install dependencies (includes Protobuf support)
pip install -r requirements.txt

# Install in development mode
pip install -e .
```

### Comprehensive Testing

```bash
# Start Kafka and Schema Registry
docker-compose up -d

# Run all 81 tests across all formats
python -m pytest tests/ -v

# Test specific serialization formats
python -m pytest tests/test_avro_io_manager.py -v          # Avro tests
python -m pytest tests/test_protobuf_io_manager.py -v      # Protobuf tests
python -m pytest tests/test_schema_evolution.py -v        # Schema evolution
python -m pytest tests/test_monitoring.py -v              # Monitoring
python -m pytest tests/test_performance.py -v             # Performance
```

### Running Examples

```bash
# JSON examples
python examples/json_examples/simple_json_test.py

# Avro examples
python examples/avro_examples/simple_avro_test.py
python examples/avro_examples/production_schema_migration.py

# Protobuf examples
python examples/protobuf_examples/simple_protobuf_example.py
python examples/protobuf_examples/advanced_protobuf_example.py

# Performance examples
python examples/performance_examples/high_throughput_pipeline.py
```

## Schema Registry Support

Supports multiple Schema Registry providers across Avro and Protobuf:

- **Confluent Schema Registry** (most common)
- **AWS Glue Schema Registry**  
- **Azure Schema Registry**
- **Custom implementations**

## Error Handling and Recovery

The integration includes comprehensive error handling for all serialization formats:

- **Connection failures**: Graceful timeouts and retries
- **Schema errors**: Clear error messages for missing/invalid schemas  
- **Deserialization errors**: Skip malformed messages with logging
- **Schema evolution failures**: Multiple recovery strategies
- **Performance degradation**: Automatic optimization recommendations
- **Authentication**: Support for SASL, SSL, and other auth methods

## Production Features

### Error Recovery Strategies

- **Fail Fast**: Immediate failure on errors
- **Fallback Schema**: Automatic fallback to previous schema versions
- **Skip Validation**: Continue processing with validation disabled
- **Graceful Degradation**: Accept minor breaking changes
- **Retry with Backoff**: Exponential backoff retry logic

### Performance Optimization

- **High-Performance Caching**: LRU, TTL, and write-through strategies
- **Adaptive Batching**: Dynamic batch size optimization
- **Connection Pooling**: Efficient resource management
- **Metrics Collection**: Comprehensive performance monitoring

### Monitoring and Alerting

- **Real-time Metrics**: Validation attempts, cache hit rates, throughput
- **Alert Integration**: Slack, email, and custom webhooks
- **Threshold Management**: Configurable alert thresholds
- **Historical Analysis**: Performance trends and optimization insights

## Roadmap

### Completed Features

- **JSON Support** - Complete native integration
- **Avro Support** - Full Schema Registry + evolution validation
- **Protobuf Support** - Complete Protocol Buffers integration
- **Schema Evolution** - All compatibility levels across formats
- **Production Monitoring** - Real-time alerting and metrics
- **High-Performance Optimization** - Caching, batching, pooling
- **Comprehensive Testing** - 81 tests across all features

### Upcoming Features

- **Enhanced Security** - SASL/SSL for production clusters
- **Dead Letter Queues** - Advanced error handling
- **PyPI Distribution** - Official package release
- **Confluent Connect** - Native connector integration
- **Kafka Streams** - Stream processing integration

### Future Enhancements

- **Additional Formats** - JSON Schema, MessagePack
- **Advanced Consumers** - Custom partition assignment
- **Cloud Integrations** - AWS MSK, Confluent Cloud
- **Official Dagster Integration** - Potential core inclusion

## Why Choose This Integration

### Complete Solution

- **Only integration supporting all 3 major formats** (JSON, Avro, Protobuf)
- **Enterprise-grade features** out of the box
- **Production-ready** with comprehensive monitoring

### Developer Experience

- **Familiar Dagster patterns** - feels native to the platform
- **Comprehensive examples** for all use cases
- **Extensive documentation** and testing

### Production Ready

- **81 comprehensive tests** covering all scenarios
- **Real-world deployment** patterns and examples
- **Performance optimization** tools and monitoring

### Community Driven

- **Active development** based on user feedback
- **Open source** with transparent roadmap
- **Enterprise support** options available

## Contributing

Contributions are welcome! This project aims to be the definitive Kafka integration for Dagster.

Ways to contribute:

- **Report issues** - Found a bug? Let us know!  
- **Feature requests** - What would make this more useful?  
- **Documentation** - Help improve examples and guides  
- **Code contributions** - PRs welcome for any improvements  

## License

Apache 2.0 - see [LICENSE](LICENSE) file for details.

## Community & Support

- **GitHub Issues**: [Report bugs and request features](https://github.com/kingsley-123/dagster-kafka-integration/issues)
- **GitHub Discussions**: [Share use cases and get help](https://github.com/kingsley-123/dagster-kafka-integration/discussions)
- **Star the repo**: If this helped your project!

## Acknowledgments

- **Dagster Community**: For the initial feature request and continued feedback
- **Contributors**: Thanks to all who provided feedback, testing, and code contributions
- **Enterprise Users**: Built in response to real production deployment needs
- **Slack Community**: Special thanks for validation and feature suggestions

---

## The Complete Enterprise Solution

**The first and most comprehensive Kafka integration for Dagster** - supporting all three major serialization formats (JSON, Avro, Protobuf) with enterprise-grade production features.

*Version 0.7.0 - Built by Kingsley Okonkwo*

*Solving real data engineering problems with comprehensive open source solutions.*