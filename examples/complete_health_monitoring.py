import os
import sys
from dagster import Definitions

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import (
    ConfluentConnectResource, 
    create_connector_asset,
    create_connector_health_monitoring
)

# Create a connector asset
mirror_connector_asset = create_connector_asset(
    group_name="kafka_connect",
)

# Create the health monitoring solution
remediation_job, health_sensor = create_connector_health_monitoring(
    connector_names=["test-mirror-source", "test-mock-source"],
    minimum_interval_seconds=30  # Check every 30 seconds
)

# Define all components
defs = Definitions(
    assets=[mirror_connector_asset],
    jobs=[remediation_job],
    sensors=[health_sensor],
    resources={
        "connect": ConfluentConnectResource(
            connect_url="http://localhost:8083",
        )
    },
)
