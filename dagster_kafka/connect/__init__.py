"""
Confluent Connect integration for Dagster Kafka.

This package provides a complete integration between Dagster and Confluent Connect,
enabling the management of Kafka Connect connectors within Dagster pipelines.

Key components:
- ConfluentConnectClient: Client for the Confluent Connect REST API
- ConfluentConnectResource: Dagster resource for using the client in pipelines
- create_connector_asset: Factory function for creating connector assets
- create_connector_health_sensor: Factory function for creating health monitoring sensors
- create_connector_health_monitoring: Factory function for creating a complete monitoring solution
"""
from dagster_kafka.connect.client import ConfluentConnectClient, ConfluentConnectError
from dagster_kafka.connect.resource import ConfluentConnectResource
from dagster_kafka.connect.assets import create_connector_asset, ConnectorConfig
from dagster_kafka.connect.sensors import (
    create_connector_health_sensor,
    create_connector_health_monitoring,
    RemediationConfig
)

__all__ = [
    "ConfluentConnectClient", 
    "ConfluentConnectError",
    "ConfluentConnectResource",
    "create_connector_asset",
    "ConnectorConfig",
    "create_connector_health_sensor",
    "create_connector_health_monitoring",
    "RemediationConfig",
]