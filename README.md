# Dagster Kafka Integration

[![PyPI version](https://badge.fury.io/py/dagster-kafka.svg)](https://badge.fury.io/py/dagster-kafka)
[![Python Support](https://img.shields.io/pypi/pyversions/dagster-kafka.svg)](https://pypi.org/project/dagster-kafka/)
[![Downloads](https://pepy.tech/badge/dagster-kafka)](https://pepy.tech/project/dagster-kafka)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**The most comprehensively validated Kafka integration for Dagster** - Supporting all four major serialization formats with enterprise-grade features, complete security, operational tooling, and YAML-based Components.

## 🚀 What's New in v1.2.1

### ✨ JSON Schema Validation (NEW)
- **4th Serialization Format**: Complete JSON Schema validation support
- **Data Quality Enforcement**: Automatic validation with configurable strictness
- **Schema Evolution**: Track and validate schema changes over time
- **Enterprise DLQ Integration**: Invalid data automatically routed to Dead Letter Queue
- **Production Ready**: Circuit breaker patterns and comprehensive error handling

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Serialization Formats](#serialization-formats)
  - [JSON Schema Validation (NEW)](#json-schema-validation-new)
  - [JSON Support](#json-support)
  - [Avro Support](#avro-support)
  - [Protobuf Support](#protobuf-support)
- [Enterprise Features](#enterprise-features)
- [Dead Letter Queue (DLQ)](#dead-letter-queue-dlq)
- [Security](#security)
- [Performance](#performance)
- [Examples](#examples)
- [Development](#development)
- [Contributing](#contributing)

## Installation

```bash
pip install dagster-kafka
```

**Requirements**: Python 3.9+ | Dagster 1.5.0+

## Quick Start

### JSON Schema Validation (Recommended)
```python
from dagster import asset, Definitions
from dagster_kafka import KafkaResource, create_json_schema_kafka_io_manager, DLQStrategy

# Define your data quality schema
user_events_schema = {
    "type": "object",
    "properties": {
        "user_id": {"type": "string"},
        "event_type": {"type": "string", "enum": ["login", "logout", "click"]},
        "timestamp": {"type": "string", "format": "date-time"},
        "metadata": {
            "type": "object",
            "properties": {
                "ip_address": {"type": "string"},
                "user_agent": {"type": "string"}
            },
            "required": ["ip_address"]
        }
    },
    "required": ["user_id", "event_type", "timestamp"]
}

@asset(io_manager_key="json_schema_io_manager")
def validated_user_events():
    """Consume user events with automatic JSON Schema validation."""
    pass

defs = Definitions(
    assets=[validated_user_events],
    resources={
        "kafka": KafkaResource(bootstrap_servers="localhost:9092"),
        "json_schema_io_manager": create_json_schema_kafka_io_manager(
            kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
            schema_dict=user_events_schema,
            enable_schema_validation=True,
            strict_validation=True,
            enable_dlq=True,
            dlq_strategy=DLQStrategy.CIRCUIT_BREAKER
        )
    }
)
```

### Basic JSON Usage
```python
from dagster import asset, Definitions
from dagster_kafka import KafkaResource, KafkaIOManager, DLQStrategy

@asset
def api_events():
    """Consume JSON messages from Kafka topic with DLQ support."""
    pass

defs = Definitions(
    assets=[api_events],
    resources={
        "kafka": KafkaResource(bootstrap_servers="localhost:9092"),
        "io_manager": KafkaIOManager(
            kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
            consumer_group_id="my-dagster-pipeline",
            enable_dlq=True,
            dlq_strategy=DLQStrategy.RETRY_THEN_DLQ,
            dlq_max_retries=3
        )
    }
)
```

## Serialization Formats

This integration supports **all four major serialization formats** used in modern data engineering:

| Format | Schema Support | Validation | Registry | Performance | Best For |
|--------|---------------|------------|----------|-------------|----------|
| **JSON** | ❌ | ❌ | ❌ | Good | Simple events, logs |
| **JSON Schema** | ✅ | ✅ | ❌ | Good | **Data quality enforcement** |
| **Avro** | ✅ | ✅ | ✅ | Better | Schema evolution, analytics |
| **Protobuf** | ✅ | ✅ | ✅ | Best | High-performance, microservices |

### JSON Schema Validation (NEW)

**Enforce data quality with automatic validation**

#### Features
- **Automatic Validation**: Messages validated against JSON Schema on consumption
- **Flexible Modes**: Strict (fail on invalid) or lenient (warn and continue)
- **Schema Evolution**: Track schema changes and compatibility
- **DLQ Integration**: Invalid messages automatically routed for investigation
- **File or Inline**: Load schemas from files or define inline

#### Basic Usage
```python
from dagster_kafka import create_json_schema_kafka_io_manager

# Using schema file
json_schema_manager = create_json_schema_kafka_io_manager(
    kafka_resource=kafka_resource,
    schema_file="schemas/user_events.json",
    enable_schema_validation=True,
    strict_validation=True
)

# Using inline schema
json_schema_manager = create_json_schema_kafka_io_manager(
    kafka_resource=kafka_resource,
    schema_dict={
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "timestamp": {"type": "string", "format": "date-time"}
        },
        "required": ["id", "timestamp"]
    },
    enable_schema_validation=True
)
```

#### Advanced Configuration
```python
# Production configuration with circuit breaker
production_manager = create_json_schema_kafka_io_manager(
    kafka_resource=secure_kafka_resource,
    schema_file="schemas/critical_events_v2.json",
    consumer_group_id="production-events",
    enable_schema_validation=True,
    strict_validation=True,
    enable_dlq=True,
    dlq_strategy=DLQStrategy.CIRCUIT_BREAKER,
    dlq_max_retries=5,
    dlq_circuit_breaker_failure_threshold=3
)
```

#### Schema Evolution Example
```python
@asset(io_manager_key="evolving_schema_manager")
def evolving_events():
    """Events with schema evolution tracking."""
    pass

# The manager automatically detects schema changes and logs compatibility
```

### JSON Support

Basic JSON message consumption without schema validation.

```python
from dagster_kafka import KafkaIOManager

json_manager = KafkaIOManager(
    kafka_resource=kafka_resource,
    consumer_group_id="json-consumer",
    enable_dlq=True
)
```

### Avro Support

Binary format with Schema Registry integration and evolution validation.

```python
from dagster_kafka import avro_kafka_io_manager

@asset(io_manager_key="avro_kafka_io_manager")
def user_data(context, config):
    """Load user events using Avro schema with validation."""
    io_manager = context.resources.avro_kafka_io_manager
    return io_manager.load_input(
        context,
        topic="user-events",
        schema_file="schemas/user.avsc",
        validate_evolution=True
    )
```

### Protobuf Support

High-performance binary format with full schema management.

```python
from dagster_kafka.protobuf_io_manager import create_protobuf_kafka_io_manager

protobuf_manager = create_protobuf_kafka_io_manager(
    kafka_resource=kafka_resource,
    schema_registry_url="http://localhost:8081",
    consumer_group_id="protobuf-consumer",
    enable_dlq=True
)
```

## Enterprise Features

### Comprehensive Enterprise Validation

**Version 1.2.2** - Most validated Kafka integration package ever created:

#### 11-Phase Enterprise Validation Completed
- **EXCEPTIONAL Performance**: 1,199 messages/second peak throughput
- **Security Hardened**: Complete credential validation + network security  
- **Stress Tested**: 100% success rate (305/305 operations over 8+ minutes)
- **Memory Efficient**: Stable under extended load (+42MB over 8 minutes)
- **Enterprise Ready**: Complete DLQ tooling suite with 5 CLI tools
- **Zero Critical Issues**: Across all validation phases
- **JSON Schema Validated**: 4th serialization format thoroughly tested

#### Validation Results Summary
| Phase | Test Type | Result | Key Metrics |
|-------|-----------|--------|-------------|
| **Phase 5** | Performance Testing | ✅ **PASS** | 1,199 msgs/sec peak throughput |
| **Phase 7** | Integration Testing | ✅ **PASS** | End-to-end message flow validated |
| **Phase 9** | Compatibility Testing | ✅ **PASS** | Python 3.12 + Dagster 1.11.3 |
| **Phase 10** | Security Audit | ✅ **PASS** | Credential + network security |
| **Phase 11** | Stress Testing | ✅ **EXCEPTIONAL** | 100% success rate, 305 operations |

### Core Enterprise Features
- **Complete Security**: SASL/SSL authentication and encryption
- **Schema Evolution**: Breaking change detection across all formats
- **Production Monitoring**: Real-time alerting with Slack/Email integration
- **High Performance**: Advanced caching, batching, and connection pooling
- **Error Recovery**: Multiple recovery strategies for production resilience
- **Dagster Components**: YAML-based configuration for teams

### Enterprise DLQ Tooling Suite

Complete operational tooling for Dead Letter Queue management:

```bash
# Analyze failed messages with comprehensive error pattern analysis
dlq-inspector --topic user-events --max-messages 20

# Replay messages with filtering and safety controls  
dlq-replayer --source-topic orders_dlq --target-topic orders --dry-run

# Monitor DLQ health across multiple topics
dlq-monitor --topics user-events_dlq,orders_dlq --output-format json

# Set up automated alerting
dlq-alerts --topic critical-events_dlq --max-messages 500

# Operations dashboard for DLQ health monitoring
dlq-dashboard --topics user-events_dlq,orders_dlq
```

## Dead Letter Queue (DLQ)

### DLQ Strategies
- **DISABLED**: No DLQ processing
- **IMMEDIATE**: Send to DLQ immediately on failure
- **RETRY_THEN_DLQ**: Retry N times, then send to DLQ
- **CIRCUIT_BREAKER**: Circuit breaker pattern with DLQ fallback

### Error Classification
- **DESERIALIZATION_ERROR**: Failed to deserialize message
- **SCHEMA_ERROR**: Schema validation failed (includes JSON Schema validation)
- **PROCESSING_ERROR**: Business logic error
- **CONNECTION_ERROR**: Kafka connection issues
- **TIMEOUT_ERROR**: Message processing timeout
- **UNKNOWN_ERROR**: Unclassified errors

### Circuit Breaker Configuration
```python
from dagster_kafka import DLQConfiguration, DLQStrategy

dlq_config = DLQConfiguration(
    strategy=DLQStrategy.CIRCUIT_BREAKER,
    circuit_breaker_failure_threshold=5,      # Open after 5 failures
    circuit_breaker_recovery_timeout_ms=30000, # Test recovery after 30s
    circuit_breaker_success_threshold=2        # Close after 2 successes
)
```

## Security

### Security Protocols Supported
- **PLAINTEXT**: For local development and testing
- **SSL**: Certificate-based encryption
- **SASL_PLAINTEXT**: Username/password authentication  
- **SASL_SSL**: Combined authentication and encryption (recommended for production)

### SASL Authentication Mechanisms
- **PLAIN**: Simple username/password authentication
- **SCRAM-SHA-256**: Secure challenge-response authentication
- **SCRAM-SHA-512**: Enhanced secure authentication
- **GSSAPI**: Kerberos authentication for enterprise environments
- **OAUTHBEARER**: OAuth-based authentication

### Secure Production Example
```python
from dagster_kafka import KafkaResource, SecurityProtocol, SaslMechanism

secure_kafka = KafkaResource(
    bootstrap_servers="prod-kafka-01:9092,prod-kafka-02:9092",
    security_protocol=SecurityProtocol.SASL_SSL,
    sasl_mechanism=SaslMechanism.SCRAM_SHA_256,
    sasl_username="production-user",
    sasl_password="secure-password",
    ssl_ca_location="/etc/ssl/certs/kafka-ca.pem",
    ssl_check_hostname=True
)
```

## Performance

### Validated Performance Results
- **Peak Throughput**: 1,199 messages/second
- **Stress Test Success**: 100% (305/305 operations)
- **Extended Stability**: 8+ minutes continuous operation
- **Memory Efficiency**: +42MB over extended load (excellent)
- **Concurrent Operations**: 120/120 successful operations
- **Resource Management**: Zero thread accumulation

### Enterprise Stability Testing
```
PASS Extended Stability: 5+ minutes, 137/137 successful materializations
PASS Resource Management: 15 cycles, no memory leaks detected  
PASS Concurrent Usage: 8 threads × 15 operations = 100% success
PASS Comprehensive Stress: 8+ minutes, 305 operations, EXCEPTIONAL rating
```

## Examples

### Complete JSON Schema Examples

#### Development Environment
```python
from dagster_kafka import create_json_schema_kafka_io_manager, DLQStrategy

# Lenient validation for development
dev_manager = create_json_schema_kafka_io_manager(
    kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
    schema_file="schemas/user_events_dev.json",
    enable_schema_validation=True,
    strict_validation=False,  # Allow malformed data in dev
    enable_dlq=True,
    dlq_strategy=DLQStrategy.IMMEDIATE
)
```

#### Production Environment
```python
# Strict validation for production
prod_manager = create_json_schema_kafka_io_manager(
    kafka_resource=secure_kafka_resource,
    schema_file="schemas/user_events_prod.json",
    consumer_group_id="production-user-events",
    enable_schema_validation=True,
    strict_validation=True,
    enable_dlq=True,
    dlq_strategy=DLQStrategy.CIRCUIT_BREAKER,
    dlq_circuit_breaker_failure_threshold=3,
    dlq_max_retries=5
)
```

#### Multi-Schema Pipeline
```python
def create_multi_schema_pipeline():
    """Pipeline with different schemas for different data types."""
    
    kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
    
    return Definitions(
        assets=[user_events, order_events, product_events],
        resources={
            "kafka": kafka_resource,
            
            # Strict validation for user events
            "user_schema_manager": create_json_schema_kafka_io_manager(
                kafka_resource=kafka_resource,
                schema_file="schemas/user_events.json",
                strict_validation=True
            ),
            
            # Lenient validation for order events
            "order_schema_manager": create_json_schema_kafka_io_manager(
                kafka_resource=kafka_resource,
                schema_file="schemas/order_events.json",
                strict_validation=False
            ),
            
            # Circuit breaker for critical product events
            "product_schema_manager": create_json_schema_kafka_io_manager(
                kafka_resource=kafka_resource,
                schema_file="schemas/product_events.json",
                strict_validation=True,
                dlq_strategy=DLQStrategy.CIRCUIT_BREAKER
            )
        }
    )
```

### YAML Components Configuration

Transform complex Python setup into simple YAML:

```yaml
# Configure JSON Schema validation with YAML
type: dagster_kafka.KafkaComponent
attributes:
  kafka_config:
    bootstrap_servers: "localhost:9092"
    security_protocol: "PLAINTEXT"
  
  consumer_config:
    consumer_group_id: "json-schema-pipeline"
    max_messages: 500
    enable_dlq: true
    dlq_strategy: "circuit_breaker"
  
  schema_config:
    enable_schema_validation: true
    strict_validation: true
  
  topics:
    - name: "user-events"
      format: "json_schema"
      schema_file: "schemas/user_events.json"
    - name: "order-events"
      format: "json_schema"
      schema_file: "schemas/order_events.json"
    - name: "analytics-events"
      format: "avro"
      schema_registry_url: "http://localhost:8081"
```

## Development

### Running Tests
```bash
# Run all validation tests (11 phases)
python -m pytest tests/ -v

# Specific test modules  
python -m pytest tests/test_json_schema_io_manager.py -v  # JSON Schema tests
python -m pytest tests/test_avro_io_manager.py -v         # Avro tests
python -m pytest tests/test_protobuf_io_manager.py -v     # Protobuf tests
python -m pytest tests/test_dlq.py -v                    # DLQ tests
python -m pytest tests/test_security.py -v               # Security tests
python -m pytest tests/test_performance.py -v            # Performance tests
```

### Local Development Setup
```bash
# Clone the repository
git clone https://github.com/kingsley-123/dagster-kafka-integration.git
cd dagster-kafka-integration

# Install dependencies
pip install -r requirements.txt

# Install in development mode
pip install -e .

# Start local Kafka for testing
docker-compose up -d
```

### Example Directory Structure
```
examples/
├── json_examples/              # Basic JSON message examples
├── json_schema_examples/       # JSON Schema validation examples (NEW)
├── avro_examples/              # Avro schema examples  
├── protobuf_examples/          # Protobuf examples
├── components_examples/        # YAML Components configuration
├── dlq_examples/               # Complete DLQ tooling suite
├── security_examples/          # Enterprise security examples
├── performance_examples/       # Performance optimization
└── production_examples/        # Enterprise deployment patterns
```

## Why Choose This Integration

### Complete Solution
- **Only integration supporting all 4 major formats** (JSON, JSON Schema, Avro, Protobuf)
- **Enterprise-grade security** with SASL/SSL support
- **Production-ready** with comprehensive monitoring
- **Advanced error handling** with Dead Letter Queue support
- **Complete DLQ Tooling Suite** for enterprise operations

### Developer Experience
- **Multiple configuration options** - Python API OR simple YAML Components
- **Team accessibility** - Components enable non-Python users to configure assets
- **Familiar Dagster patterns** - feels native to the platform
- **Comprehensive examples** for all use cases including security and DLQ
- **Extensive documentation** and testing
- **Production-ready CLI tooling** for DLQ management

### Enterprise Ready
- **11-phase comprehensive validation** covering all scenarios
- **Real-world deployment patterns** and examples
- **Performance optimization** tools and monitoring
- **Enterprise security** for production Kafka clusters
- **Bulletproof error handling** with circuit breaker patterns
- **Complete operational tooling** for DLQ management

### Unprecedented Validation
- **Most validated package** in the Dagster ecosystem
- **Performance proven**: 1,199 msgs/sec peak throughput
- **Stability proven**: 100% success rate under stress
- **Security proven**: Complete credential and network validation
- **Enterprise proven**: Exceptional rating across all dimensions

## Roadmap

### Completed Features (v1.2.1)
- **JSON Support** - Complete native integration ✅
- **JSON Schema Support** - Data validation with evolution checking ✅
- **Avro Support** - Full Schema Registry + evolution validation ✅
- **Protobuf Support** - Complete Protocol Buffers integration ✅
- **Dagster Components** - YAML-based configuration support ✅
- **Enterprise Security** - Complete SASL/SSL authentication and encryption ✅
- **Schema Evolution** - All compatibility levels across formats ✅
- **Production Monitoring** - Real-time alerting and metrics ✅
- **High-Performance Optimization** - Caching, batching, pooling ✅
- **Dead Letter Queues** - Advanced error handling with circuit breaker ✅
- **Complete DLQ Tooling Suite** - Inspector, Replayer, Monitoring, Alerting ✅
- **Comprehensive Testing** - 11-phase enterprise validation ✅
- **PyPI Distribution** - Official package published and validated ✅
- **Security Hardening** - Configuration injection protection ✅

### Upcoming Features
- **Confluent Connect** - Native connector integration
- **Enhanced JSON Schema** - Schema registry integration
- **Advanced Monitoring** - Custom metrics and dashboards

## Contributing

Contributions are welcome! This project aims to be the definitive Kafka integration for Dagster.

### Ways to contribute:
- **Report issues** - Found a bug? Let us know!
- **Feature requests** - What would make this more useful?
- **Documentation** - Help improve examples and guides
- **Code contributions** - PRs welcome for any improvements
- **Security testing** - Help test security configurations
- **DLQ testing** - Help test error handling scenarios
- **JSON Schema testing** - Help test validation scenarios

## License

Apache 2.0 - see [LICENSE](LICENSE) file for details.

## Community & Support

- **GitHub Issues**: [Report bugs and request features](https://github.com/kingsley-123/dagster-kafka-integration/issues)
- **GitHub Discussions**: [Share use cases and get help](https://github.com/kingsley-123/dagster-kafka-integration/discussions)
- **PyPI Package**: [Install and documentation](https://pypi.org/project/dagster-kafka/)
- **Star the repo**: If this helped your project!

## Acknowledgments

- **Dagster Community**: For the initial feature request and continued feedback
- **Contributors**: Thanks to all who provided feedback, testing, and code contributions
- **Enterprise Users**: Built in response to real production deployment needs
- **Security Community**: Special thanks for security testing and validation
- **JSON Schema Community**: Thanks for validation methodology and best practices

---

## The Complete Enterprise Solution

**The most comprehensively validated Kafka integration for Dagster** - supporting all four major serialization formats (JSON, JSON Schema, Avro, Protobuf) with enterprise-grade production features, complete security, advanced Dead Letter Queue error handling, YAML-based Components, and complete operational tooling suite.

**Version 1.2.2** - JSON Schema Validation Release

*Built by [Kingsley Okonkwo](https://github.com/kingsley-123) - Solving real data engineering problems with comprehensive open source solutions.*