import os
import sys
import time
import argparse
from typing import List, Dict, Any

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import ConfluentConnectClient

def check_connector_health(
    client: ConfluentConnectClient,
    connector_names: List[str],
    auto_restart: bool = False
):
    """
    Check the health of the specified connectors and optionally restart them if unhealthy.
    
    Args:
        client: The Confluent Connect client
        connector_names: List of connector names to check
        auto_restart: Whether to automatically restart unhealthy connectors/tasks
    
    Returns:
        A tuple of (healthy_connectors, unhealthy_connectors)
    """
    healthy_connectors = []
    unhealthy_connectors = []
    
    for connector_name in connector_names:
        try:
            print(f"Checking connector: {connector_name}")
            
            # Get connector status
            status = client.get_connector_status(connector_name)
            connector_state = status["connector"]["state"]
            
            print(f"  State: {connector_state}")
            
            # Check connector state
            if connector_state != "RUNNING":
                print(f"  ⚠️ Connector {connector_name} is in {connector_state} state")
                unhealthy_info = {
                    "name": connector_name,
                    "state": connector_state,
                    "issue": f"Connector state is {connector_state}"
                }
                unhealthy_connectors.append(unhealthy_info)
                
                # Auto-restart if enabled
                if auto_restart:
                    print(f"  🔄 Restarting connector {connector_name}...")
                    client.restart_connector(connector_name)
                    print(f"  ✅ Restart initiated for connector {connector_name}")
            else:
                healthy_connectors.append({
                    "name": connector_name,
                    "state": connector_state
                })
            
            # Check task states
            tasks = status.get("tasks", [])
            print(f"  Tasks: {len(tasks)}")
            
            for task in tasks:
                task_id = task.get("id")
                task_state = task.get("state")
                
                print(f"    Task {task_id}: {task_state}")
                
                if task_state != "RUNNING":
                    print(f"    ⚠️ Task {task_id} is in {task_state} state")
                    unhealthy_info = {
                        "name": connector_name,
                        "task_id": task_id,
                        "state": task_state,
                        "issue": f"Task {task_id} state is {task_state}"
                    }
                    unhealthy_connectors.append(unhealthy_info)
                    
                    # Auto-restart if enabled
                    if auto_restart:
                        print(f"    🔄 Restarting task {task_id}...")
                        client.restart_task(connector_name, int(task_id))
                        print(f"    ✅ Restart initiated for task {task_id}")
            
        except Exception as e:
            print(f"  ❌ Error checking connector {connector_name}: {e}")
            unhealthy_connectors.append({
                "name": connector_name,
                "issue": f"Error checking status: {str(e)}"
            })
    
    return healthy_connectors, unhealthy_connectors

def main():
    parser = argparse.ArgumentParser(description='Monitor health of Kafka Connect connectors')
    parser.add_argument('--url', default='http://localhost:8083', help='Kafka Connect REST API URL')
    parser.add_argument('--interval', type=int, default=30, help='Check interval in seconds')
    parser.add_argument('--auto-restart', action='store_true', help='Automatically restart unhealthy connectors')
    parser.add_argument('connectors', nargs='*', help='Connector names to monitor (if empty, all connectors will be monitored)')
    
    args = parser.parse_args()
    
    # Create client
    client = ConfluentConnectClient(base_url=args.url)
    
    # Get connector names if not specified
    connector_names = args.connectors
    if not connector_names:
        try:
            connector_names = client.list_connectors()
            print(f"Monitoring all {len(connector_names)} connectors: {connector_names}")
        except Exception as e:
            print(f"Error listing connectors: {e}")
            sys.exit(1)
    
    # Monitor connectors
    try:
        print(f"Starting health monitor with interval: {args.interval}s")
        print(f"Auto-restart: {'Enabled' if args.auto_restart else 'Disabled'}")
        
        while True:
            print("\n" + "="*50)
            print(f"Health check at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*50)
            
            healthy, unhealthy = check_connector_health(
                client, 
                connector_names,
                auto_restart=args.auto_restart
            )
            
            print("\nHealth check summary:")
            print(f"  Healthy connectors: {len(healthy)}")
            print(f"  Unhealthy connectors/tasks: {len(unhealthy)}")
            
            if unhealthy:
                print("\nUnhealthy components:")
                for item in unhealthy:
                    print(f"  - {item['name']}: {item['issue']}")
            
            # Wait for next check
            print(f"\nNext check in {args.interval} seconds...")
            time.sleep(args.interval)
    
    except KeyboardInterrupt:
        print("\nMonitor stopped by user")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
