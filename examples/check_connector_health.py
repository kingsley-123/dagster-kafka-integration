import os
import sys
import time

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import ConfluentConnectClient

def check_connector_health():
    """Check the health of connectors directly using the client."""
    print("Checking connector health...")
    
    # Create the client
    client = ConfluentConnectClient(base_url="http://localhost:8083")
    
    # Get list of connectors
    try:
        connectors = client.list_connectors()
        print(f"Found {len(connectors)} connectors: {connectors}")
        
        # Check each connector's health
        for connector_name in connectors:
            print(f"\nChecking health of connector: {connector_name}")
            
            try:
                # Get connector status
                status = client.get_connector_status(connector_name)
                connector_state = status["connector"]["state"]
                print(f"Connector state: {connector_state}")
                
                # Check if connector is healthy
                is_healthy = connector_state == "RUNNING"
                print(f"Connector is {'healthy' if is_healthy else 'unhealthy'}")
                
                # Check tasks
                tasks = status.get("tasks", [])
                print(f"Tasks: {len(tasks)}")
                
                for task in tasks:
                    task_id = task.get("id", "unknown")
                    task_state = task.get("state", "unknown")
                    print(f"  Task {task_id}: {task_state}")
                    
                    # Check if task is healthy
                    is_task_healthy = task_state == "RUNNING"
                    if not is_task_healthy:
                        print(f"  ⚠️ Task {task_id} is unhealthy: {task_state}")
            
            except Exception as e:
                print(f"Error checking connector {connector_name}: {str(e)}")
    
    except Exception as e:
        print(f"Error listing connectors: {str(e)}")
        return False
    
    print("\nHealth check complete!")
    return True

if __name__ == "__main__":
    check_connector_health()
