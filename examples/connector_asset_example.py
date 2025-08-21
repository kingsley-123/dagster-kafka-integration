import os
import sys
from dagster import materialize

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import ConfluentConnectResource, create_connector_asset, ConnectorConfig

# Create a connector asset with NO prefix to keep things simple
connector_asset = create_connector_asset(
    group_name="kafka_connect",
    key_prefix=None,  # No prefix
)

if __name__ == "__main__":
    # Use the materialize function with the structure shown in the error message
    result = materialize(
        assets=[connector_asset],
        resources={
            "connect": ConfluentConnectResource(
                connect_url="http://localhost:8083",
            )
        },
        run_config={
            "ops": {
                "connector_asset": {  # This must match the name of your asset function
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
    
    print(f"Job completed with status: {result.success}")
