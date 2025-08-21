import os
import sys
from typing import List, Dict, Any
from dagster import Definitions, define_asset_job, job, op, In, Out, materialize, Config

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import (
    ConfluentConnectResource, 
    create_connector_asset,
    create_connector_health_sensor
)

# Create a connector asset
connector_asset = create_connector_asset(
    group_name="kafka_connect",
)

# Define a proper Config class for the op
class UnhealthyConnectorsConfig(Config):
    unhealthy_connectors: List[Dict[str, Any]]

# Create a job to handle unhealthy connectors
@op
def process_unhealthy_connectors(context, config: UnhealthyConnectorsConfig):
    """Process unhealthy connectors and take action."""
    unhealthy_connectors = config.unhealthy_connectors
    
    for connector in unhealthy_connectors:
        connector_name = connector["name"]
        issue = connector["issue"]
        context.log.error(f"Unhealthy connector {connector_name}: {issue}")
        
        # Here you could implement automatic remediation:
        # - Restart the connector
        # - Send alerts
        # - Create incidents in your monitoring system
        # - Log to a database for tracking
        
    return {
        "num_connectors": len(unhealthy_connectors),
        "connector_names": [c["name"] for c in unhealthy_connectors]
    }

@job
def handle_unhealthy_connectors_job():
    """Job to handle unhealthy connectors."""
    process_unhealthy_connectors()

# Create a health sensor that monitors our connector
connector_health_sensor = create_connector_health_sensor(
    connector_names=["test-mirror-source"],
    job_def=handle_unhealthy_connectors_job,
    minimum_interval_seconds=30,  # Check every 30 seconds
)

# Define all components
defs = Definitions(
    assets=[connector_asset],
    jobs=[handle_unhealthy_connectors_job],
    sensors=[connector_health_sensor],
    resources={
        "connect": ConfluentConnectResource(
            connect_url="http://localhost:8083",
        )
    },
)

if __name__ == "__main__":
    # For demonstration, materialize the connector asset
    print("Materializing connector asset...")
    materialize(
        assets=[connector_asset],
        resources={
            "connect": ConfluentConnectResource(
                connect_url="http://localhost:8083",
            )
        },
        run_config={
            "ops": {
                "connector_asset": {
                    "config": {
                        "name": "test-mirror-source",
                        "connector_class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
                        "config": {
                            "tasks.max": "1",
                            "topics": "test-topic",
                            "source.cluster.alias": "source",
                            "target.cluster.alias": "target",
                            "source.cluster.bootstrap.servers": "kafka:9092",
                            "target.cluster.bootstrap.servers": "kafka:9092"
                        }
                    }
                }
            }
        }
    )
    
    print("\nConnector asset materialized successfully!")
    print("\nTo run the health sensor, use the Dagster CLI:")
    print("dagster dev -f examples/connector_health_sensor_example.py")
