import os
import sys
import time
from typing import Dict, Any, List

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect.client import ConfluentConnectClient
# Import your actual DLQ class
from dagster_kafka.dlq import DLQManager

class ConnectDLQIntegration:
    """
    Integrate Kafka Connect with the DLQ system to handle connector failures.
    """
    
    def __init__(self, connect_url, dlq_config=None):
        self.client = ConfluentConnectClient(base_url=connect_url)
        
        # Use the actual DLQManager from your project
        self.dlq_manager = DLQManager(**(dlq_config or {}))
    
    def check_connectors_once(self, connector_names=None):
        """
        Check connectors once and send alerts to DLQ for unhealthy connectors.
        
        Args:
            connector_names: List of connectors to check (None = all connectors)
        """
        if connector_names is None:
            connector_names = self.client.list_connectors()
        
        print(f"Checking {len(connector_names)} connectors with DLQ integration:")
        for name in connector_names:
            print(f"- {name}")
        
        unhealthy_count = 0
        
        for connector_name in connector_names:
            try:
                status = self.client.get_connector_status(connector_name)
                connector_state = status["connector"]["state"]
                
                print(f"Connector {connector_name}: {connector_state}")
                
                # Check if connector is unhealthy
                if connector_state != "RUNNING":
                    print(f"⚠️ Connector {connector_name} is in {connector_state} state")
                    self._handle_unhealthy_connector(connector_name, status, "connector_state")
                    unhealthy_count += 1
                
                # Check tasks
                for task in status.get("tasks", []):
                    task_id = task.get("id")
                    task_state = task.get("state")
                    
                    print(f"  Task {task_id}: {task_state}")
                    
                    if task_state != "RUNNING":
                        print(f"⚠️ Task {task_id} of connector {connector_name} is in {task_state} state")
                        self._handle_unhealthy_task(connector_name, task, "task_state")
                        unhealthy_count += 1
            
            except Exception as e:
                print(f"Error checking connector {connector_name}: {e}")
                self._handle_connector_error(connector_name, str(e))
                unhealthy_count += 1
        
        print(f"Health check complete. Found {unhealthy_count} issues.")
        return unhealthy_count == 0
    
    def _handle_unhealthy_connector(self, connector_name, status, reason):
        """Send unhealthy connector to DLQ."""
        # Create DLQ message - adapt this to your actual DLQManager format
        dlq_message = {
            "connector_name": connector_name,
            "status": status,
            "reason": reason,
            "timestamp": time.time()
        }
        
        # Use your actual DLQManager method to handle the error
        try:
            # Adjust this call based on your actual DLQManager API
            topic = f"connect-errors-{connector_name}"
            self.dlq_manager.handle_error(topic, dlq_message)
            print(f"Sent connector {connector_name} error to DLQ due to {reason}")
            
            # Also try to restart it
            try:
                self.client.restart_connector(connector_name)
                print(f"Initiated restart of connector {connector_name}")
            except Exception as e:
                print(f"Failed to restart connector {connector_name}: {e}")
        
        except Exception as e:
            print(f"Error sending to DLQ: {e}")
    
    def _handle_unhealthy_task(self, connector_name, task, reason):
        """Send unhealthy task to DLQ."""
        # Create DLQ message - adapt this to your actual DLQManager format
        dlq_message = {
            "connector_name": connector_name,
            "task_id": task.get("id"),
            "task_state": task.get("state"),
            "reason": reason,
            "timestamp": time.time()
        }
        
        # Use your actual DLQManager method to handle the error
        try:
            # Adjust this call based on your actual DLQManager API
            topic = f"connect-errors-{connector_name}-task-{task.get('id')}"
            self.dlq_manager.handle_error(topic, dlq_message)
            print(f"Sent task {task.get('id')} error to DLQ due to {reason}")
            
            # Also try to restart it
            try:
                self.client.restart_task(connector_name, int(task.get("id")))
                print(f"Initiated restart of task {task.get('id')}")
            except Exception as e:
                print(f"Failed to restart task {task.get('id')}: {e}")
        
        except Exception as e:
            print(f"Error sending to DLQ: {e}")
    
    def _handle_connector_error(self, connector_name, error_message):
        """Send connector error to DLQ."""
        # Create DLQ message - adapt this to your actual DLQManager format
        dlq_message = {
            "connector_name": connector_name,
            "error": error_message,
            "timestamp": time.time()
        }
        
        # Use your actual DLQManager method to handle the error
        try:
            # Adjust this call based on your actual DLQManager API
            topic = f"connect-errors-{connector_name}"
            self.dlq_manager.handle_error(topic, dlq_message)
            print(f"Sent connector error to DLQ: {connector_name} - {error_message}")
        except Exception as e:
            print(f"Error sending to DLQ: {e}")

# Mock DLQManager for testing if needed
class MockDLQManager:
    def handle_error(self, topic, message):
        print(f"[MOCK DLQ] Message sent to DLQ topic {topic}: {message}")

# Example usage
def main():
    # Check if we need to use the mock DLQManager
    try:
        from dagster_kafka.dlq import DLQManager
        dlq_manager = DLQManager()  # Create with default settings
    except (ImportError, Exception) as e:
        print(f"Using mock DLQManager due to: {e}")
        dlq_manager = MockDLQManager()
    
    # Create the integration with the DLQ manager
    integration = ConnectDLQIntegration(
        connect_url="http://localhost:8083",
        dlq_config={"manager": dlq_manager}
    )
    
    # Check specific connectors
    if len(sys.argv) > 1:
        connector_names = sys.argv[1:]
        integration.check_connectors_once(connector_names)
    else:
        # Check all connectors
        integration.check_connectors_once()

if __name__ == "__main__":
    main()
