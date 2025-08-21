"""
Design Pattern: Kafka Connect Error Recovery Patterns

This file demonstrates patterns for automatically recovering from 
Kafka Connect connector failures.
"""
import os
import sys
import time
from typing import Dict, Any, List

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect.client import ConfluentConnectClient

#
# PATTERN 1: Simple Auto-Recovery
#
def simple_recovery(client, connector_name, max_attempts=3):
    """
    Simple recovery pattern that tries to restart a connector.
    
    Args:
        client: ConfluentConnectClient instance
        connector_name: Name of the connector to recover
        max_attempts: Maximum number of restart attempts
    
    Returns:
        bool: True if recovery was successful
    """
    print(f"Attempting to recover connector {connector_name}")
    
    for attempt in range(1, max_attempts + 1):
        print(f"Recovery attempt {attempt}/{max_attempts}")
        
        try:
            # Check current status
            status = client.get_connector_status(connector_name)
            state = status["connector"]["state"]
            
            print(f"Current connector state: {state}")
            
            if state == "RUNNING":
                print(f"✅ Connector is already running. Recovery not needed.")
                return True
            
            # Try to restart
            print(f"Restarting connector...")
            client.restart_connector(connector_name)
            time.sleep(5)  # Wait for restart to take effect
            
            # Make sure to resume if it was paused
            if state == "PAUSED":
                print(f"Resuming connector after restart...")
                client.resume_connector(connector_name)
                time.sleep(3)
            
            # Check if recovery was successful
            status = client.get_connector_status(connector_name)
            new_state = status["connector"]["state"]
            
            print(f"State after recovery attempt: {new_state}")
            
            if new_state == "RUNNING":
                print(f"✅ Recovery successful!")
                return True
            
            print(f"⚠️ Recovery attempt {attempt} failed. Waiting before next attempt...")
            time.sleep(10)  # Wait between attempts
            
        except Exception as e:
            print(f"Error during recovery attempt {attempt}: {e}")
    
    print(f"❌ All recovery attempts failed for connector {connector_name}")
    return False

#
# PATTERN 2: Advanced Recovery with Task Restart
#
def advanced_recovery(client, connector_name, max_attempts=3):
    """
    Advanced recovery pattern that tries different recovery methods
    based on the nature of the failure.
    
    Args:
        client: ConfluentConnectClient instance
        connector_name: Name of the connector to recover
        max_attempts: Maximum number of restart attempts
    
    Returns:
        bool: True if recovery was successful
    """
    print(f"Attempting advanced recovery for connector {connector_name}")
    
    for attempt in range(1, max_attempts + 1):
        print(f"Recovery attempt {attempt}/{max_attempts}")
        
        try:
            # Check current status
            status = client.get_connector_status(connector_name)
            connector_state = status["connector"]["state"]
            
            print(f"Current connector state: {connector_state}")
            
            if connector_state == "RUNNING":
                # Check if any tasks are failing
                tasks = status.get("tasks", [])
                failing_tasks = [
                    task for task in tasks
                    if task.get("state") != "RUNNING"
                ]
                
                if not failing_tasks:
                    print(f"✅ Connector and all tasks are running. Recovery not needed.")
                    return True
                
                # Restart only the failing tasks
                print(f"Found {len(failing_tasks)} failing tasks. Restarting them individually...")
                
                for task in failing_tasks:
                    task_id = task.get("id")
                    print(f"Restarting task {task_id}...")
                    client.restart_task(connector_name, int(task_id))
                    time.sleep(3)
                
            else:
                # Try different recovery strategies based on state
                if connector_state == "FAILED":
                    print(f"Connector has FAILED. Trying restart...")
                    client.restart_connector(connector_name)
                elif connector_state == "PAUSED":
                    print(f"Connector is PAUSED. Trying resume...")
                    client.resume_connector(connector_name)
                else:
                    print(f"Connector in state {connector_state}. Trying both restart and resume...")
                    client.restart_connector(connector_name)
                    time.sleep(3)
                    client.resume_connector(connector_name)
            
            # Wait for recovery to take effect
            time.sleep(5)
            
            # Check if recovery was successful
            status = client.get_connector_status(connector_name)
            new_state = status["connector"]["state"]
            
            print(f"State after recovery attempt: {new_state}")
            
            # Check tasks again
            tasks = status.get("tasks", [])
            still_failing_tasks = [
                task for task in tasks
                if task.get("state") != "RUNNING"
            ]
            
            if new_state == "RUNNING" and not still_failing_tasks:
                print(f"✅ Recovery successful! Connector and all tasks are running.")
                return True
            
            print(f"⚠️ Recovery attempt {attempt} failed. Waiting before next attempt...")
            time.sleep(10)  # Wait between attempts
            
        except Exception as e:
            print(f"Error during recovery attempt {attempt}: {e}")
    
    print(f"❌ All recovery attempts failed for connector {connector_name}")
    return False

#
# PATTERN 3: Configuration-Based Recovery
#
def config_based_recovery(client, connector_name, max_attempts=3):
    """
    Configuration-based recovery that identifies and fixes
    configuration issues.
    
    Args:
        client: ConfluentConnectClient instance
        connector_name: Name of the connector to recover
        max_attempts: Maximum number of restart attempts
    
    Returns:
        bool: True if recovery was successful
    """
    print(f"Attempting configuration-based recovery for connector {connector_name}")
    
    try:
        # Get current configuration
        config = client.get_connector_config(connector_name)
        print(f"Current configuration: {config}")
        
        # Example configuration fixes based on common issues
        # (These would be customized for your specific connectors)
        
        # Example: Ensure proper SSL configuration
        if "security.protocol" in config and config["security.protocol"] == "SSL":
            if "ssl.endpoint.identification.algorithm" not in config:
                print("Adding missing SSL configuration...")
                config["ssl.endpoint.identification.algorithm"] = "https"
        
        # Example: Fix connection timeouts
        if "connection.timeout.ms" in config:
            timeout = int(config["connection.timeout.ms"])
            if timeout < 5000:
                print("Increasing connection timeout...")
                config["connection.timeout.ms"] = "10000"
        
        # Apply updated configuration
        print(f"Applying configuration updates...")
        client.update_connector_config(connector_name, config)
        time.sleep(5)
        
        # Restart connector with new configuration
        print(f"Restarting connector with updated configuration...")
        client.restart_connector(connector_name)
        time.sleep(5)
        
        # Resume if needed
        client.resume_connector(connector_name)
        time.sleep(3)
        
        # Check if recovery was successful
        status = client.get_connector_status(connector_name)
        new_state = status["connector"]["state"]
        
        print(f"State after configuration-based recovery: {new_state}")
        
        if new_state == "RUNNING":
            print(f"✅ Configuration-based recovery successful!")
            return True
    
    except Exception as e:
        print(f"Error during configuration-based recovery: {e}")
    
    print(f"❌ Configuration-based recovery failed")
    return False

# Example usage
def test_recovery_patterns():
    """Test all recovery patterns."""
    client = ConfluentConnectClient(base_url="http://localhost:8083")
    connector_name = "test-mock-source"
    
    # First pause the connector to simulate a failure
    print("\n--- Simulating failure by pausing connector ---")
    client.pause_connector(connector_name)
    time.sleep(3)
    
    # Try simple recovery
    print("\n--- PATTERN 1: Simple Recovery ---")
    simple_recovery(client, connector_name)
    
    # Simulate failure again
    print("\n--- Simulating failure by pausing connector ---")
    client.pause_connector(connector_name)
    time.sleep(3)
    
    # Try advanced recovery
    print("\n--- PATTERN 2: Advanced Recovery ---")
    advanced_recovery(client, connector_name)
    
    # Simulate failure again
    print("\n--- Simulating failure by pausing connector ---")
    client.pause_connector(connector_name)
    time.sleep(3)
    
    # Try configuration-based recovery
    print("\n--- PATTERN 3: Configuration-Based Recovery ---")
    config_based_recovery(client, connector_name)
    
    print("\n--- Recovery Testing Complete ---")

if __name__ == "__main__":
    test_recovery_patterns()
