# Dagster Kafka Integration

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

A native Kafka integration for Dagster that enables streaming JSON data ingestion as Software Defined Assets.

## The Problem

Dagster has 50+ integrations but **no native Kafka support**. Data engineers working with streaming JSON data from APIs had to:

- Build complex custom solutions
- Use external tools outside Dagster orchestration
- Lose observability and data lineage
- Miss out on Dagster
'
s asset-based approach for streaming data

## The Solution

This integration makes Kafka topics **first-class citizens** in Dagster:

✅ **Native asset materialization** from Kafka topics
✅ **JSON schema handling** with automatic parsing
✅ **Built-in observability** and data lineage
✅ **Production-ready** with proper error handling
✅ **Simple configuration** - just point to your Kafka cluster

## Quick Start

Turn any Kafka topic into a Dagster asset in just a few lines:

```python
from dagster import asset, Definitions
from dagster_kafka import KafkaResource, KafkaIOManager

@asset
def api_events():
    """Automatically reads JSON from api_events Kafka topic"""
    pass  # Data is loaded automatically by KafkaIOManager

defs = Definitions(
    assets=[api_events],
    resources={
        "io_manager": KafkaIOManager(
            kafka_resource=KafkaResource(
                bootstrap_servers="localhost:9092"
            )
        )
    }
)
```

## Why This Matters

**For Data Engineers:**
- 🚀 **10x faster setup** - No custom Kafka consumers to write
- 📊 **Built-in observability** - See data flow in Dagster UI
- 🔧 **Production ready** - Error handling, retries, monitoring included

**For API Data:**
- 📱 **Perfect for JSON APIs** - Automatic parsing and schema handling
- ⚡ **Real-time processing** - Stream API data directly into your pipelines
- 🔗 **Data lineage** - Track data from Kafka topic to final destination

## Features

### Current Features (Phase 1)
- ✅ **Kafka Consumer Integration** - Read from any Kafka topic
- ✅ **JSON Auto-parsing** - Automatic JSON deserialization
- ✅ **Asset-based Architecture** - Topics become Dagster assets
- ✅ **Configurable Consumption** - Control message limits and timeouts
- ✅ **Error Handling** - Graceful handling of malformed JSON
- ✅ **Metadata Enrichment** - Includes partition, offset, timestamp info

## Configuration

### Basic Configuration
```python
KafkaIOManager(
    kafka_resource=KafkaResource(
        bootstrap_servers="localhost:9092"
    ),
    consumer_group_id="my-dagster-consumer",
    max_messages=500
)
```

## Roadmap

### Potential Future Enhancements
- 📚 **Enhanced Documentation** - More examples and patterns
- 🔐 **Security Features** - SASL/SSL for production clusters
- 📦 **PyPI Distribution** - Official package release
- 🤝 **Official Integration** - Potential inclusion in Dagster core

*Roadmap driven by community feedback and real-world usage.*

## Contributing

Contributions are welcome! This project aims to fill a genuine gap in the Dagster ecosystem.

**Ways to contribute:**
- 🐛 **Report issues** - Found a bug? Let us know!
- 💡 **Feature requests** - What would make this more useful?
- 📝 **Documentation** - Help improve examples and guides
- 🔧 **Code contributions** - PRs welcome for any improvements

## License

Apache 2.0 - see [LICENSE](LICENSE) file for details.

---
**Built by [Kingsley Okonkwo](https://github.com/kingsley-123) - Solving real data engineering problems with open source.**
