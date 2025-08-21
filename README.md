<artifacts>
<artifact type="text/markdown" title="Updated dagster-kafka README.md with Confluent Connect Integration">

##  Dagster Kafka Integration

[![PyPI version](https://badge.fury.io/py/dagster-kafka.svg)](https://badge.fury.io/py/dagster-kafka)
[![Python Support](https://img.shields.io/pypi/pyversions/dagster-kafka.svg)](https://pypi.org/project/dagster-kafka/)
[![Downloads](https://pepy.tech/badge/dagster-kafka)](https://pepy.tech/project/dagster-kafka)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**The most comprehensively validated Kafka integration for Dagster** - Supporting all four major serialization formats with enterprise-grade features, complete security, operational tooling, and YAML-based Components.

##  What's New in v1.3.0

### Confluent Connect Integration (NEW)
- **Complete Connect Management**: Native REST API integration for Kafka Connect
- **Connector Assets**: Define Kafka connectors as Dagster Software-Defined Assets
- **Automated Health Monitoring**: Intelligent connector monitoring with auto-recovery
- **Enterprise Operations**: CLI tools for connector management and monitoring
- **Production Ready**: Thoroughly tested with race condition handling and load testing

### JSON Schema Validation
- **4th Serialization Format**: Complete JSON Schema validation support
- **Data Quality Enforcement**: Automatic validation with configurable strictness
- **Schema Evolution**: Track and validate schema changes over time
- **Enterprise DLQ Integration**: Invalid data automatically routed to Dead Letter Queue
- **Production Ready**: Circuit breaker patterns and comprehensive error handling

## 📋 Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Serialization Formats](#serialization-formats)
  - [JSON Schema Validation](#json-schema-validation)
  - [JSON Support](#json-support)
  - [Avro Support](#avro-support)
  - [Protobuf Support](#protobuf-support)
- [Confluent Connect Integration](#confluent-connect-integration)
  - [Connect Management](#connect-management)
  - [Connector Assets](#connector-assets)
  - [Health Monitoring](#health-monitoring)
  - [Recovery Patterns](#recovery-patterns)
  - [CLI Tools](#cli-tools)
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

### Confluent Connect Integration (New)

```python
from dagster import Definitions
from dagster_kafka.connect import ConfluentConnectResource, create_connector_asset

# Define a connector as a Dagster asset
mirror_connector = create_connector_asset(
    group_name="kafka_connect",
)

# Define your Dagster job with the connector asset
defs = Definitions(
    assets=[mirror_connector],
    resources={
        "connect": ConfluentConnectResource(
            connect_url="http://localhost:8083",
        )
    },
)

# Use with configuration
"""
ops:
  connector_asset:
    config:
      name: "my-source-connector"
      connector_class: "org.apache.kafka.connect.file.FileStreamSourceConnector"
      config:
        tasks.max: "1"
        file: "/tmp/test-source.txt"
        topic: "test-topic"
"""
```

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

## Serialization Formats

This integration supports **all four major serialization formats** used in modern data engineering:

| Format | Schema Support | Validation | Registry | Performance | Best For |
|--------|---------------|------------|----------|-------------|----------|
| **JSON** | ❌ | ❌ | ❌ | Good | Simple events, logs |
| **JSON Schema** | ✅ | ✅ | ❌ | Good | **Data quality enforcement** |
| **Avro** | ✅ | ✅ | ✅ | Better | Schema evolution, analytics |
| **Protobuf** | ✅ | ✅ | ✅ | Best | High-performance, microservices |

### JSON Schema Validation

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

## Confluent Connect Integration

The Confluent Connect integration provides a complete solution for managing Kafka Connect connectors within your Dagster environment.

### Connect Management

**REST API Client for Kafka Connect**

```python
from dagster_kafka.connect import ConfluentConnectClient

# Create a client to interact with the Connect REST API
client = ConfluentConnectClient(base_url="http://localhost:8083")

# List available connectors
connectors = client.list_connectors()

# Get connector status
status = client.get_connector_status("my-connector")

# Create a new connector
connector_config = {
    "name": "file-source-connector",
    "config": {
        "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
        "tasks.max": "1",
        "file": "/tmp/test-source.txt",
        "topic": "test-topic"
    }
}
client.create_connector(connector_config)

# Update configuration
client.update_connector_config("file-source-connector", {"tasks.max": "2"})

# Manage connector lifecycle
client.pause_connector("file-source-connector")
client.resume_connector("file-source-connector")
client.restart_connector("file-source-connector")
```

### Connector Assets

**Define Kafka Connect connectors as Dagster assets**

```python
from dagster import Definitions
from dagster_kafka.connect import ConfluentConnectResource, create_connector_asset

# Create a connector asset
source_connector = create_connector_asset(
    group_name="kafka_connect",
)

# Define your Dagster job
defs = Definitions(
    assets=[source_connector],
    resources={
        "connect": ConfluentConnectResource(
            connect_url="http://localhost:8083",
        )
    },
)
```

#### Configuration Example

```yaml
ops:
  connector_asset:
    config:
      name: "mysql-source-connector"
      connector_class: "io.debezium.connector.mysql.MySqlConnector"
      config:
        tasks.max: "1"
        database.hostname: "mysql"
        database.port: "3306"
        database.user: "debezium"
        database.password: "dbz"
        database.server.id: "184054"
        database.server.name: "dbserver1"
        database.include.list: "inventory"
        database.history.kafka.bootstrap.servers: "kafka:9092"
        database.history.kafka.topic: "schema-changes.inventory"
```

### Health Monitoring

**Monitor connector health and implement auto-recovery**

```python
from dagster import job, op, sensor, SensorResult, RunRequest
from dagster_kafka.connect import ConfluentConnectResource

@sensor(
    name="connect_health_sensor",
    minimum_interval_seconds=60,
    required_resource_keys={"connect"},
)
def connector_health_sensor(context):
    """Monitor the health of Kafka Connect connectors."""
    connect = context.resources.connect
    connector_names = ["mysql-source", "elasticsearch-sink"]
    
    unhealthy_connectors = []
    
    # Check each connector
    for connector_name in connector_names:
        try:
            status = connect.get_connector_status(connector_name)
            connector_state = status["connector"]["state"]
            
            # Check if connector is unhealthy
            if connector_state != "RUNNING":
                context.log.warning(
                    f"Connector {connector_name} is in {connector_state} state"
                )
                unhealthy_connectors.append({
                    "name": connector_name,
                    "state": connector_state
                })
            
            # Also check tasks
            for task in status.get("tasks", []):
                task_state = task.get("state")
                task_id = task.get("id")
                
                if task_state != "RUNNING":
                    context.log.warning(
                        f"Task {task_id} of connector {connector_name} is in {task_state} state"
                    )
                    unhealthy_connectors.append({
                        "name": connector_name,
                        "task_id": task_id,
                        "state": task_state
                    })
        except Exception as e:
            context.log.error(f"Error checking connector {connector_name}: {e}")
    
    # If we have unhealthy connectors, trigger a remediation job
    if unhealthy_connectors:
        return RunRequest(
            run_key=f"connector_health_{context.cursor}",
            job_name="remediate_connectors_job",
            run_config={
                "ops": {
                    "remediate_connectors": {
                        "config": {
                            "unhealthy_connectors": unhealthy_connectors
                        }
                    }
                }
            },
        )
    
    return SensorResult(
        skip_reason="All connectors healthy",
        cursor=str(context.get_current_time()),
    )
```

### Recovery Patterns

**Implement automated recovery from connector failures**

```python
from dagster_kafka.connect import ConfluentConnectClient

def advanced_recovery(client, connector_name, max_attempts=3):
    """
    Advanced recovery pattern that tries different recovery methods
    based on the nature of the failure.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            # Check current status
            status = client.get_connector_status(connector_name)
            connector_state = status["connector"]["state"]
            
            if connector_state == "RUNNING":
                # Check if any tasks are failing
                tasks = status.get("tasks", [])
                failing_tasks = [
                    task for task in tasks
                    if task.get("state") != "RUNNING"
                ]
                
                if not failing_tasks:
                    return True
                
                # Restart only the failing tasks
                for task in failing_tasks:
                    task_id = task.get("id")
                    client.restart_task(connector_name, int(task_id))
            else:
                # Try different recovery strategies based on state
                if connector_state == "FAILED":
                    client.restart_connector(connector_name)
                elif connector_state == "PAUSED":
                    client.resume_connector(connector_name)
                else:
                    client.restart_connector(connector_name)
                    client.resume_connector(connector_name)
            
            # Wait for recovery to take effect and check again
            time.sleep(5)
            status = client.get_connector_status(connector_name)
            if status["connector"]["state"] == "RUNNING":
                return True
        except Exception as e:
            pass
    
    return False
```

### CLI Tools

The Confluent Connect integration includes several command-line tools for connector management and monitoring:

```bash
# List all connectors
python connect_cli.py list

# Get connector status
python connect_cli.py status mysql-connector

# Create a new connector
python connect_cli.py create connector_config.json

# Update connector configuration
python connect_cli.py update mysql-connector updated_config.json

# Restart a connector
python connect_cli.py restart mysql-connector

# Monitor connector health
python connect_health_monitor.py --auto-restart mysql-connector elasticsearch-connector
```

## Enterprise Features

### Comprehensive Enterprise Validation

**Version 1.3.0** - Most validated Kafka integration package ever created:

#### 12-Phase Enterprise Validation Completed
- **EXCEPTIONAL Performance**: 1,199 messages/second peak throughput
- **Security Hardened**: Complete credential validation + network security  
- **Stress Tested**: 100% success rate (305/305 operations over 8+ minutes)
- **Memory Efficient**: Stable under extended load (+42MB over 8 minutes)
- **Enterprise Ready**: Complete DLQ tooling suite with 5 CLI tools
- **Zero Critical Issues**: Across all validation phases
- **Connect Integration Validated**: Race condition and load testing complete
- **JSON Schema Validated**: 4th serialization format thoroughly tested

#### Validation Results Summary
| Phase | Test Type | Result | Key Metrics |
|-------|-----------|--------|-------------|
| **Phase 5** | Performance Testing | ✅ **PASS** | 1,199 msgs/sec peak throughput |
| **Phase 7** | Integration Testing | ✅ **PASS** | End-to-end message flow validated |
| **Phase 9** | Compatibility Testing | ✅ **PASS** | Python 3.12 + Dagster 1.11.3 |
| **Phase 10** | Security Audit | ✅ **PASS** | Credential + network security |
| **Phase 11** | Stress Testing | ✅ **EXCEPTIONAL** | 100% success rate, 305 operations |
| **Phase 12** | Connect Integration | ✅ **PASS** | Race condition and load testing |

### Core Enterprise Features
- **Complete Security**: SASL/SSL authentication and encryption
- **Schema Evolution**: Breaking change detection across all formats
- **Production Monitoring**: Real-time alerting with Slack/Email integration
- **High Performance**: Advanced caching, batching, and connection pooling
- **Error Recovery**: Multiple recovery strategies for production resilience
- **Dagster Components**: YAML-based configuration for teams
- **Connect Integration**: Complete Kafka Connect management

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

### Complete Kafka Ecosystem Integration

```python
from dagster import Definitions, asset, AssetExecutionContext
from dagster_kafka import KafkaResource
from dagster_kafka.connect import ConfluentConnectResource, create_connector_asset
from dagster_kafka.json_schema_io_manager import create_json_schema_kafka_io_manager

# Source connector that ingests data into Kafka
source_connector = create_connector_asset(
    key_prefix=["mysql", "cdc"],
    group_name="source_connectors",
)

# Processing asset that consumes and transforms data
@asset(
    key_prefix=["kafka", "processed"],
    group_name="processing",
    deps=[source_connector],  # Depends on source connector
    io_manager_key="json_schema_io_manager",
)
def transform_data(context: AssetExecutionContext):
    """Transform the data from the source topic."""
    # Your transformation logic
    return {"transformed": "data"}

# Sink connector that exports data to external systems
sink_connector = create_connector_asset(
    key_prefix=["elasticsearch", "sink"],
    group_name="sink_connectors",
    deps=[transform_data],  # Depends on the transformation
)

# Define your complete pipeline with Kafka ecosystem integration
defs = Definitions(
    assets=[source_connector, transform_data, sink_connector],
    resources={
        # Kafka resource for message consumption/production
        "kafka": KafkaResource(
            bootstrap_servers="localhost:9092",
        ),
        
        # Connect resource for connector management
        "connect": ConfluentConnectResource(
            connect_url="http://localhost:8083",
        ),
        
        # JSON Schema validation for data quality
        "json_schema_io_manager": create_json_schema_kafka_io_manager(
            kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
            schema_file="schemas/event_schema.json",
            enable_schema_validation=True,
            strict_validation=True,
        ),
    },
)
```

### Connector Health Monitoring Integration

```python
from dagster import Definitions, job, op, sensor, Config
from dagster_kafka.connect import ConfluentConnectResource

class UnhealthyConnectorsConfig(Config):
    unhealthy_connectors: List[Dict[str, Any]]

@op
def remediate_connectors(context, config: UnhealthyConnectorsConfig):
    """Auto-remediate unhealthy connectors."""
    connect = context.resources.connect
    unhealthy_connectors = config.unhealthy_connectors
    
    for connector in unhealthy_connectors:
        connector_name = connector["name"]
        
        # If it's a task issue, restart the specific task
        if "task_id" in connector:
            task_id = connector["task_id"]
            context.log.info(f"Restarting task {task_id} of connector {connector_name}")
            connect.restart_task(connector_name, int(task_id))
        
        # Otherwise restart the entire connector
        else:
            context.log.info(f"Restarting connector {connector_name}")
            connect.restart_connector(connector_name)
            
            # Resume if it was paused
            context.log.info(f"Resuming connector {connector_name}")
            connect.resume_connector(connector_name)
    
    return {
        "remediated_count": len(unhealthy_connectors),
        "connector_names": [c["name"] for c in unhealthy_connectors]
    }

@job
def remediate_connectors_job():
    """Job to remediate unhealthy connectors."""
    remediate_connectors()

# Define your Dagster components
defs = Definitions(
    jobs=[remediate_connectors_job],
    sensors=[connector_health_sensor],  # Use the sensor defined earlier
    resources={
        "connect": ConfluentConnectResource(
            connect_url="http://localhost:8083",
        )
    },
)
```

## Development

### Running Tests
```bash
# Run all validation tests (12 phases)
python -m pytest tests/ -v

# Specific test modules
python -m pytest tests/test_connect_client.py -v    # Connect client tests
python -m pytest tests/test_connect_resource.py -v  # Connect resource tests
python -m pytest tests/test_connect_assets.py -v    # Connect assets tests  
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

# Start local Kafka and Connect for testing
docker-compose up -d
```

### Example Directory Structure
```
examples/
├── json_examples/              # Basic JSON message examples
├── json_schema_examples/       # JSON Schema validation examples
├── avro_examples/              # Avro schema examples  
├── protobuf_examples/          # Protobuf examples
├── connect_examples/           # Confluent Connect integration examples (NEW)
├── components_examples/        # YAML Components configuration
├── dlq_examples/               # Complete DLQ tooling suite
├── security_examples/          # Enterprise security examples
├── performance_examples/       # Performance optimization
└── production_examples/        # Enterprise deployment patterns
```

## Why Choose This Integration

### Complete Solution
- **Only integration supporting all 4 major formats** (JSON, JSON Schema, Avro, Protobuf)
- **Complete Kafka ecosystem integration** with Confluent Connect support
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
- **Production-ready CLI tooling** for DLQ management and Connect operations

### Enterprise Ready
- **12-phase comprehensive validation** covering all scenarios
- **Real-world deployment patterns** and examples
- **Performance optimization** tools and monitoring
- **Enterprise security** for production Kafka clusters
- **Bulletproof error handling** with circuit breaker patterns
- **Complete operational tooling** for DLQ management and Connect integration

### Unprecedented Validation
- **Most validated package** in the Dagster ecosystem
- **Performance proven**: 1,199 msgs/sec peak throughput
- **Stability proven**: 100% success rate under stress
- **Security proven**: Complete credential and network validation
- **Enterprise proven**: Exceptional rating across all dimensions

## Roadmap

### Completed Features (v1.3.0)
- **JSON Support** - Complete native integration ✅
- **JSON Schema Support** - Data validation with evolution checking ✅
- **Avro Support** - Full Schema Registry + evolution validation ✅
- **Protobuf Support** - Complete Protocol Buffers integration ✅
- **Confluent Connect Integration** - Complete Connect REST API integration ✅
- **Connector Assets** - Define connectors as Dagster assets ✅
- **Health Monitoring** - Automated connector health monitoring ✅
- **Recovery Patterns** - Advanced connector recovery strategies ✅
- **Connect CLI Tools** - Complete command-line tools for Connect operations ✅
- **Dagster Components** - YAML-based configuration support ✅
- **Enterprise Security** - Complete SASL/SSL authentication and encryption ✅
- **Schema Evolution** - All compatibility levels across formats ✅
- **Production Monitoring** - Real-time alerting and metrics ✅
- **High-Performance Optimization** - Caching, batching, pooling ✅
- **Dead Letter Queues** - Advanced error handling with circuit breaker ✅
- **Complete DLQ Tooling Suite** - Inspector, Replayer, Monitoring, Alerting ✅
- **Comprehensive Testing** - 12-phase enterprise validation ✅
- **PyPI Distribution** - Official package published and validated ✅
- **Security Hardening** - Configuration injection protection ✅

### Upcoming Features
- **Enhanced JSON Schema** - Schema registry integration
- **Advanced Connect Monitoring** - Custom metrics and dashboards
- **Connect Templates** - Pre-built connector configurations

## Contributing

Contributions are welcome! This project aims to be the definitive Kafka integration for Dagster.

### Ways to contribute:
- **Report issues** - Found a bug? Let us know!
- **Feature requests** - What would make this more useful?
- **Documentation** - Help improve examples and guides
- **Code contributions** - PRs welcome for any improvements
- **Security testing** - Help test security configurations
- **DLQ testing** - Help test error handling scenarios
- **Connect testing** - Help test connector integration scenarios

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
- **Confluent Community**: For guidance on Connect integration best practices

---

## The Complete Enterprise Solution

**The most comprehensively validated Kafka integration for Dagster** - supporting all four major serialization formats (JSON, JSON Schema, Avro, Protobuf) with enterprise-grade production features, complete security, advanced Dead Letter Queue error handling, Confluent Connect integration, YAML-based Components, and complete operational tooling suite.

**Version 1.3.0** - Confluent Connect Integration Release

*Built by [Kingsley Okonkwo](https://github.com/kingsley-123) - Solving real data engineering problems with comprehensive open source solutions.*
</artifact>
</artifacts>

