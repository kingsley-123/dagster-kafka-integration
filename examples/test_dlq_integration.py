import os
import sys
import time
from typing import Dict, Any, List

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import ConfluentConnectClient
from dagster_kafka.dlq import DLQHandler  # Assuming this exists in your project

class ConnectDLQIntegration:
    """
    Integrate Kafka Connect with the DLQ system to handle connector failures.
    """
    
    def __init__(self, connect_url, dlq_config=None):
        self.client = ConfluentConnectClient(base_url=connect_url)
        
        # Initialize DLQ handler (this would use your actual DLQ implementation)
        self.dlq_handler = DLQHandler(**(dlq_config or {}))
    
    def monitor_connectors(self, connector_names=None, interval_seconds=60):
        """
        Monitor connectors and send alerts to DLQ for unhealthy connectors.
        
        Args:
            connector_names: List of connectors to monitor (None = all connectors)
            interval_seconds: Monitoring interval
        """
        if connector_names is None:
            connector_names = self.client.list_connectors()
        
        print(f"Monitoring {len(connector_names)} connectors with DLQ integration:")
        for name in connector_names:
            print(f"- {name}")
        
        while True:
            print(f"\nChecking connector health at {time.strftime('%Y-%m-%d %H:%M:%S')}...")
            
            for connector_name in connector_names:
                try:
                    status = self.client.get_connector_status(connector_name)
                    connector_state = status["connector"]["state"]
                    
                    # Check if connector is unhealthy
                    if connector_state != "RUNNING":
                        print(f"⚠️ Connector {connector_name} is in {connector_state} state")
                        self._handle_unhealthy_connector(connector_name, status, "connector_state")
                    
                    # Check tasks
                    for task in status.get("tasks", []):
                        task_id = task.get("id")
                        task_state = task.get("state")
                        
                        if task_state != "RUNNING":
                            print(f"⚠️ Task {task_id} of connector {connector_name} is in {task_state} state")
                            self._handle_unhealthy_task(connector_name, task, "task_state")
                
                except Exception as e:
                    print(f"Error checking connector {connector_name}: {e}")
                    self._handle_connector_error(connector_name, str(e))
            
            print(f"Next check in {interval_seconds} seconds...")
            time.sleep(interval_seconds)
    
    def _handle_unhealthy_connector(self, connector_name, status, reason):
        """Send unhealthy connector to DLQ."""
        # Create DLQ message
        dlq_message = {
            "type": "connect_health",
            "subtype": reason,
            "connector_name": connector_name,
            "status": status,
            "timestamp": time.time(),
            "severity": "critical"
        }
        
        # Send to DLQ
        try:
            self.dlq_handler.send_to_dlq(dlq_message)
            print(f"Sent connector {connector_name} to DLQ due to {reason}")
            
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
        # Create DLQ message
        dlq_message = {
            "type": "connect_health",
            "subtype": reason,
            "connector_name": connector_name,
            "task_id": task.get("id"),
            "task_state": task.get("state"),
            "timestamp": time.time(),
            "severity": "high"
        }
        
        # Send to DLQ
        try:
            self.dlq_handler.send_to_dlq(dlq_message)
            print(f"Sent task {task.get('id')} to DLQ due to {reason}")
            
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
        # Create DLQ message
        dlq_message = {
            "type": "connect_health",
            "subtype": "api_error",
            "connector_name": connector_name,
            "error": error_message,
            "timestamp": time.time(),
            "severity": "critical"
        }
        
        # Send to DLQ
        try:
            self.dlq_handler.send_to_dlq(dlq_message)
            print(f"Sent connector error to DLQ: {connector_name} - {error_message}")
        except Exception as e:
            print(f"Error sending to DLQ: {e}")

# Mock DLQ handler for testing
class MockDLQHandler:
    def send_to_dlq(self, message):
        print(f"[MOCK DLQ] Message sent to DLQ: {message}")

# Example usage
def main():
    # Create a mock DLQ handler for demonstration
    dlq_handler = MockDLQHandler()
    
    # Create the integration with the mock DLQ handler
    integration = ConnectDLQIntegration(
        connect_url="http://localhost:8083",
        dlq_config={"handler": dlq_handler}
    )
    
    # Monitor specific connectors
    if len(sys.argv) > 1:
        connector_names = sys.argv[1:]
        integration.monitor_connectors(connector_names, interval_seconds=10)
    else:
        # Monitor all connectors
        integration.monitor_connectors(interval_seconds=10)

if __name__ == "__main__":
    # Define a mock DLQHandler class so the example can run without the actual implementation
    if not hasattr(sys.modules[__name__], 'DLQHandler'):
        sys.modules[__name__].DLQHandler = MockDLQHandler
    
    main()
