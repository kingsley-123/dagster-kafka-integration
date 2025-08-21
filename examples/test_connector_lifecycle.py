import os
import sys
import time

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import ConfluentConnectClient

def test_connector_lifecycle():
    """Test the complete lifecycle of a connector."""
    client = ConfluentConnectClient(base_url="http://localhost:8083")
    
    # Test connector name
    test_connector_name = "lifecycle-test-connector"
    
    # Get available plugins
    print("Getting available connector plugins...")
    plugins = client.get_connector_plugins()
    
    if not plugins:
        print("No connector plugins found. Exiting.")
        return False
    
    # Use the first plugin for testing
    plugin_class = plugins[0]["class"]
    print(f"Using plugin: {plugin_class}")
    
    # Step 1: Create connector
    print(f"\n--- Step 1: Creating connector {test_connector_name} ---")
    
    # Delete if it already exists
    existing_connectors = client.list_connectors()
    if test_connector_name in existing_connectors:
        print(f"Connector {test_connector_name} already exists, deleting it...")
        client.delete_connector(test_connector_name)
        time.sleep(2)  # Wait for deletion to complete
    
    # Create a new connector
    connector_config = {
        "name": test_connector_name,
        "config": {
            "connector.class": plugin_class,
            "tasks.max": "1",
            "topics": "test-topic",
            "source.cluster.alias": "source",
            "target.cluster.alias": "target",
            "source.cluster.bootstrap.servers": "kafka:9092",
            "target.cluster.bootstrap.servers": "kafka:9092"
        }
    }
    
    try:
        result = client.create_connector(connector_config)
        print(f"Connector created: {result['name']}")
        print(f"Type: {result.get('type')}")
        time.sleep(5)  # Wait for connector to start
    except Exception as e:
        print(f"Error creating connector: {e}")
        return False
    
    # Step 2: Check status
    print(f"\n--- Step 2: Checking status of {test_connector_name} ---")
    try:
        status = client.get_connector_status(test_connector_name)
        print(f"Connector state: {status['connector']['state']}")
        print(f"Worker ID: {status['connector']['worker_id']}")
    except Exception as e:
        print(f"Error getting status: {e}")
        return False
    
    # Step 3: Pause connector
    print(f"\n--- Step 3: Pausing connector {test_connector_name} ---")
    try:
        client.pause_connector(test_connector_name)
        time.sleep(3)  # Wait for pause to take effect
        
        # Check status after pause
        status = client.get_connector_status(test_connector_name)
        print(f"Connector state after pause: {status['connector']['state']}")
        
        if status['connector']['state'] != "PAUSED":
            print(f"⚠️ Unexpected state: {status['connector']['state']} (expected: PAUSED)")
            return False
    except Exception as e:
        print(f"Error pausing connector: {e}")
        return False
    
    # Step 4: Resume connector
    print(f"\n--- Step 4: Resuming connector {test_connector_name} ---")
    try:
        client.resume_connector(test_connector_name)
        time.sleep(3)  # Wait for resume to take effect
        
        # Check status after resume
        status = client.get_connector_status(test_connector_name)
        print(f"Connector state after resume: {status['connector']['state']}")
        
        if status['connector']['state'] != "RUNNING":
            print(f"⚠️ Unexpected state: {status['connector']['state']} (expected: RUNNING)")
            return False
    except Exception as e:
        print(f"Error resuming connector: {e}")
        return False
    
    # Step 5: Restart connector
    print(f"\n--- Step 5: Restarting connector {test_connector_name} ---")
    try:
        client.restart_connector(test_connector_name)
        time.sleep(5)  # Wait for restart to complete
        
        # Check status after restart
        status = client.get_connector_status(test_connector_name)
        print(f"Connector state after restart: {status['connector']['state']}")
        
        if status['connector']['state'] != "RUNNING":
            print(f"⚠️ Unexpected state: {status['connector']['state']} (expected: RUNNING)")
            return False
    except Exception as e:
        print(f"Error restarting connector: {e}")
        return False
    
    # Step 6: Update configuration
    print(f"\n--- Step 6: Updating connector configuration ---")
    try:
        # Update tasks.max from 1 to 2
        updated_config = connector_config["config"].copy()
        updated_config["tasks.max"] = "2"
        
        result = client.update_connector_config(test_connector_name, updated_config)
        print(f"Configuration updated: {result}")
        
        # Verify update
        config = client.get_connector_config(test_connector_name)
        print(f"Updated tasks.max: {config.get('tasks.max')}")
        
        if config.get("tasks.max") != "2":
            print(f"⚠️ Configuration not updated correctly")
            return False
    except Exception as e:
        print(f"Error updating configuration: {e}")
        return False
    
    # Step 7: Delete connector
    print(f"\n--- Step 7: Deleting connector {test_connector_name} ---")
    try:
        client.delete_connector(test_connector_name)
        time.sleep(2)  # Wait for deletion to complete
        
        # Verify deletion
        connectors = client.list_connectors()
        if test_connector_name in connectors:
            print(f"⚠️ Connector not deleted successfully")
            return False
        
        print(f"Connector deleted successfully")
    except Exception as e:
        print(f"Error deleting connector: {e}")
        return False
    
    print("\n✅ All lifecycle tests passed successfully!")
    return True

if __name__ == "__main__":
    test_connector_lifecycle()
