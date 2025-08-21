import os
import sys
import time

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import (
    ConfluentConnectResource, 
    create_connector_asset,
    ConnectorConfig
)
from dagster import materialize

def test_connector_with_health_check():
    """Create/update a connector and check its health."""
    print("Setting up connector and checking health...")
    
    # Create a connector asset
    connector_asset = create_connector_asset(
        group_name="kafka_connect",
    )
    
    # Create/update the connector
    print("\nMaterializing connector asset...")
    result = materialize(
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
    
    print(f"\nConnector materialization completed with success={result.success}")
    
    # Now manually check connector health
    connect = ConfluentConnectResource(
        connect_url="http://localhost:8083",
    ).setup_for_execution(None)
    
    print("\nChecking connector health...")
    connector_name = "test-mirror-source"
    
    try:
        status = connect.get_connector_status(connector_name)
        print(f"Connector status: {status}")
        
        state = status["connector"]["state"]
        print(f"Connector state: {state}")
        
        is_healthy = state == "RUNNING"
        print(f"Connector is {'healthy' if is_healthy else 'unhealthy'}")
        
        return result.success and is_healthy
    
    except Exception as e:
        print(f"Error checking connector health: {e}")
        return False

if __name__ == "__main__":
    test_connector_with_health_check()
