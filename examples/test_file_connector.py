import sys
import os
import time

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect.client import ConfluentConnectClient, ConfluentConnectError

def test_file_connector():
    """Test creating a file connector."""
    connect_url = "http://localhost:8083"
    
    try:
        # Create the client
        client = ConfluentConnectClient(base_url=connect_url)
        
        # Create a test topic
        print("Setting up a test connector...")
        
        # First, check if the connector already exists and delete it if it does
        connectors = client.list_connectors()
        connector_name = "test-file-source"
        
        if connector_name in connectors:
            print(f"Connector {connector_name} already exists, deleting it...")
            client.delete_connector(connector_name)
            # Small delay to ensure deletion completes
            time.sleep(2)
        
        # Create a simple file source connector
        connector_config = {
            "name": connector_name,
            "config": {
                "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
                "tasks.max": "1",
                "file": "/tmp/test-source.txt",
                "topic": "test-topic"
            }
        }
        
        print(f"Creating connector {connector_name}...")
        result = client.create_connector(connector_config)
        print(f"Connector created: {result}")
        
        # Get connector status
        status = client.get_connector_status(connector_name)
        print(f"Connector status: {status}")
        
        # List available connector plugins
        plugins = client.get_connector_plugins()
        print(f"Available connector plugins: {len(plugins)}")
        for plugin in plugins:
            print(f"- {plugin.get('class')}")
        
        return True
    except ConfluentConnectError as e:
        print(f"Failed to work with Kafka Connect: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = test_file_connector()
    
    if not success:
        print("Check if Kafka Connect is running and accessible.")
        sys.exit(1)
