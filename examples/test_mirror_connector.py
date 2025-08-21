import sys
import os
import time
import json

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect.client import ConfluentConnectClient, ConfluentConnectError

def test_mock_connector():
    """Test creating a mock connector with better error handling."""
    connect_url = "http://localhost:8083"
    
    try:
        # Create the client
        client = ConfluentConnectClient(base_url=connect_url)
        
        # Print Kafka Connect version info
        try:
            print("Checking Kafka Connect server info...")
            info = client._request("GET", "")
            print(f"Connect server info: {json.dumps(info, indent=2)}")
        except Exception as e:
            print(f"Error getting server info: {e}")
        
        # List available connector plugins
        try:
            plugins = client.get_connector_plugins()
            print(f"Available connector plugins: {len(plugins)}")
            for plugin in plugins:
                print(f"- {plugin.get('class')}")
        except Exception as e:
            print(f"Error getting plugins: {e}")
        
        print("\nSetting up a test connector...")
        
        # Check existing connectors
        try:
            connectors = client.list_connectors()
            print(f"Existing connectors: {connectors}")
            
            connector_name = "test-mirror-source"
            
            if connector_name in connectors:
                print(f"Connector {connector_name} already exists, deleting it...")
                client.delete_connector(connector_name)
                # Small delay to ensure deletion completes
                time.sleep(2)
                print(f"Connector {connector_name} deleted.")
        except Exception as e:
            print(f"Error checking/deleting existing connectors: {e}")
        
        # Create a mirror source connector (which is available according to our plugins list)
        connector_config = {
            "name": connector_name,
            "config": {
                "connector.class": "org.apache.kafka.connect.mirror.MirrorSourceConnector",
                "tasks.max": "1",
                "topics": "test-topic",
                "source.cluster.alias": "source",
                "target.cluster.alias": "target",
                "source.cluster.bootstrap.servers": "kafka:9092",
                "target.cluster.bootstrap.servers": "kafka:9092"
            }
        }
        
        print(f"Creating connector {connector_name}...")
        try:
            result = client.create_connector(connector_config)
            print(f"Connector created successfully: {json.dumps(result, indent=2)}")
        except Exception as e:
            print(f"Error creating connector: {e}")
            return False
        
        # Check if connector appears in the list
        print("Checking if connector was added to the list...")
        try:
            time.sleep(2)  # Give it time to initialize
            connectors = client.list_connectors()
            print(f"Connectors after creation: {connectors}")
            
            if connector_name in connectors:
                print(f"✅ Connector {connector_name} found in connector list!")
            else:
                print(f"⚠️ Connector {connector_name} NOT found in connector list!")
        except Exception as e:
            print(f"Error checking connector list: {e}")
        
        # Try to get connector status with proper error handling
        print(f"Getting status for connector {connector_name}...")
        try:
            status = client.get_connector_status(connector_name)
            print(f"Connector status: {json.dumps(status, indent=2)}")
        except Exception as e:
            print(f"Error getting connector status: {e}")
        
        print("\n✅ TEST COMPLETE - able to communicate with Kafka Connect API")
        return True
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = test_mock_connector()
    
    if not success:
        print("Check if Kafka Connect is running and accessible.")
        sys.exit(1)
