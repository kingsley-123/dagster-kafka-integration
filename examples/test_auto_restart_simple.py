import os
import sys
import time
import subprocess

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect.client import ConfluentConnectClient

def test_auto_restart():
    """Test the auto-restart functionality directly."""
    client = ConfluentConnectClient(base_url="http://localhost:8083")
    connector_name = "test-mock-source"
    
    # Step 1: Check initial state
    print("\n--- Step 1: Checking initial state ---")
    try:
        status = client.get_connector_status(connector_name)
        initial_state = status["connector"]["state"]
        print(f"Initial connector state: {initial_state}")
        
        if initial_state != "RUNNING":
            print(f"⚠️ Connector not in RUNNING state. Attempting to resume...")
            client.resume_connector(connector_name)
            time.sleep(3)
            
            status = client.get_connector_status(connector_name)
            if status["connector"]["state"] != "RUNNING":
                print(f"⚠️ Could not get connector to RUNNING state. Test cannot proceed.")
                return False
            
            print(f"Connector resumed to RUNNING state.")
    except Exception as e:
        print(f"Error checking initial state: {e}")
        return False
    
    # Step 2: Simulate failure by pausing connector
    print("\n--- Step 2: Simulating failure by pausing connector ---")
    try:
        print(f"Pausing connector {connector_name}...")
        client.pause_connector(connector_name)
        time.sleep(3)
        
        status = client.get_connector_status(connector_name)
        paused_state = status["connector"]["state"]
        print(f"Connector state after pause: {paused_state}")
        
        if paused_state != "PAUSED":
            print(f"⚠️ Connector not paused correctly. Test cannot proceed.")
            return False
    except Exception as e:
        print(f"Error pausing connector: {e}")
        return False
    
    # Step 3: Auto-restart the connector
    print("\n--- Step 3: Auto-restarting connector ---")
    try:
        print(f"Restarting connector {connector_name}...")
        client.restart_connector(connector_name)
        time.sleep(5)
        
        status = client.get_connector_status(connector_name)
        restarted_state = status["connector"]["state"]
        print(f"Connector state after restart: {restarted_state}")
        
        if restarted_state != "RUNNING":
            print(f"⚠️ Connector not restarted correctly.")
            return False
    except Exception as e:
        print(f"Error restarting connector: {e}")
        return False
    
    print("\n✅ Auto-restart test completed successfully!")
    return True

if __name__ == "__main__":
    test_auto_restart()
