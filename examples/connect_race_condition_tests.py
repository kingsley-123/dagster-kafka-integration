import os
import sys
import time
import threading
import random
from typing import List, Dict, Any

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect.client import ConfluentConnectClient

def create_test_connector(client, name_suffix="race-test"):
    """Create a test connector for race condition testing."""
    connector_name = f"test-{name_suffix}-connector"
    
    # Delete if it already exists
    existing_connectors = client.list_connectors()
    if connector_name in existing_connectors:
        print(f"Connector {connector_name} already exists, deleting it...")
        client.delete_connector(connector_name)
        time.sleep(2)
    
    # Get available plugins
    plugins = client.get_connector_plugins()
    plugin_class = plugins[0]["class"]
    
    # Create a new connector
    connector_config = {
        "name": connector_name,
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
    
    result = client.create_connector(connector_config)
    print(f"Created connector: {result['name']}")
    time.sleep(3)  # Wait for connector to start
    
    return connector_name

def operation_thread(client, connector_name, operation, thread_id, delay=0):
    """Thread that performs a specific operation on a connector."""
    if delay > 0:
        time.sleep(delay)
    
    try:
        print(f"Thread {thread_id}: Starting {operation} on {connector_name}")
        
        if operation == "pause":
            client.pause_connector(connector_name)
        elif operation == "resume":
            client.resume_connector(connector_name)
        elif operation == "restart":
            client.restart_connector(connector_name)
        elif operation == "delete":
            client.delete_connector(connector_name)
        elif operation == "status":
            status = client.get_connector_status(connector_name)
            print(f"Thread {thread_id}: Status of {connector_name}: {status['connector']['state']}")
        else:
            print(f"Thread {thread_id}: Unknown operation {operation}")
        
        print(f"Thread {thread_id}: Completed {operation} on {connector_name}")
    except Exception as e:
        print(f"Thread {thread_id}: Error performing {operation} on {connector_name}: {e}")

def test_concurrent_operations(num_threads=5):
    """
    Test concurrent operations on the same connector to check for race conditions.
    """
    client = ConfluentConnectClient(base_url="http://localhost:8083")
    
    # Create a test connector
    connector_name = create_test_connector(client, "concurrent")
    
    try:
        # Define operations to run concurrently
        operations = ["pause", "resume", "restart", "status"]
        
        # Create and start threads
        threads = []
        for i in range(num_threads):
            # Choose a random operation
            operation = random.choice(operations)
            
            # Random delay to increase chances of race conditions
            delay = random.uniform(0, 1)
            
            thread = threading.Thread(
                target=operation_thread,
                args=(client, connector_name, operation, i, delay)
            )
            threads.append(thread)
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Check final state
        try:
            status = client.get_connector_status(connector_name)
            print(f"\nFinal connector state: {status['connector']['state']}")
        except Exception as e:
            print(f"\nError getting final state: {e}")
    
    finally:
        # Clean up
        try:
            client.delete_connector(connector_name)
            print(f"Deleted test connector: {connector_name}")
        except Exception as e:
            print(f"Error deleting connector: {e}")

def test_restart_while_updating():
    """
    Test restarting a connector while updating its configuration.
    This is a common race condition scenario.
    """
    client = ConfluentConnectClient(base_url="http://localhost:8083")
    
    # Create a test connector
    connector_name = create_test_connector(client, "restart-update")
    
    try:
        # Start a thread to restart the connector
        restart_thread = threading.Thread(
            target=operation_thread,
            args=(client, connector_name, "restart", "restart-thread", 0.5)
        )
        
        # Start a thread to update the connector configuration
        def update_config_thread():
            try:
                print("Update thread: Getting current config")
                config = client.get_connector_config(connector_name)
                
                # Modify the config
                config["tasks.max"] = str(int(config.get("tasks.max", "1")) + 1)
                
                print("Update thread: Updating configuration")
                result = client.update_connector_config(connector_name, config)
                print(f"Update thread: Configuration updated: {result}")
            except Exception as e:
                print(f"Update thread: Error updating configuration: {e}")
        
        update_thread = threading.Thread(target=update_config_thread)
        
        # Start the threads
        restart_thread.start()
        update_thread.start()
        
        # Wait for both threads to complete
        restart_thread.join()
        update_thread.join()
        
        # Check final configuration and state
        try:
            config = client.get_connector_config(connector_name)
            status = client.get_connector_status(connector_name)
            
            print(f"\nFinal configuration: {config}")
            print(f"Final connector state: {status['connector']['state']}")
        except Exception as e:
            print(f"\nError getting final state: {e}")
    
    finally:
        # Clean up
        try:
            client.delete_connector(connector_name)
            print(f"Deleted test connector: {connector_name}")
        except Exception as e:
            print(f"Error deleting connector: {e}")

def run_race_condition_tests():
    """Run all race condition tests."""
    print("\n=== TEST 1: Concurrent Operations ===")
    test_concurrent_operations()
    
    print("\n=== TEST 2: Restart While Updating ===")
    test_restart_while_updating()

if __name__ == "__main__":
    run_race_condition_tests()
