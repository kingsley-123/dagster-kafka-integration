import os
import sys
import time
import concurrent.futures
from typing import List, Dict, Any

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect.client import ConfluentConnectClient

def monitor_connector(client, connector_name):
    """Monitor a single connector and return its status."""
    start_time = time.time()
    try:
        status = client.get_connector_status(connector_name)
        end_time = time.time()
        response_time = end_time - start_time
        
        return {
            "name": connector_name,
            "state": status["connector"]["state"],
            "response_time_ms": round(response_time * 1000, 2),
            "worker_id": status["connector"]["worker_id"],
            "tasks": len(status.get("tasks", [])),
            "error": None
        }
    except Exception as e:
        end_time = time.time()
        response_time = end_time - start_time
        
        return {
            "name": connector_name,
            "state": "ERROR",
            "response_time_ms": round(response_time * 1000, 2),
            "worker_id": None,
            "tasks": 0,
            "error": str(e)
        }

def create_test_connectors(client, count=5):
    """Create multiple test connectors for load testing."""
    print(f"Creating {count} test connectors...")
    
    # Get available plugins
    plugins = client.get_connector_plugins()
    if not plugins:
        print("No connector plugins found. Exiting.")
        return []
    
    # Use the first plugin for testing
    plugin_class = plugins[0]["class"]
    print(f"Using plugin: {plugin_class}")
    
    created_connectors = []
    
    for i in range(1, count + 1):
        connector_name = f"load-test-connector-{i}"
        
        # Delete if it already exists
        existing_connectors = client.list_connectors()
        if connector_name in existing_connectors:
            print(f"Connector {connector_name} already exists, deleting it...")
            client.delete_connector(connector_name)
            time.sleep(1)
        
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
        
        try:
            result = client.create_connector(connector_config)
            print(f"Created connector: {result['name']}")
            created_connectors.append(connector_name)
        except Exception as e:
            print(f"Error creating connector {connector_name}: {e}")
    
    return created_connectors

def cleanup_test_connectors(client, connector_names):
    """Delete test connectors after load testing."""
    print(f"Cleaning up {len(connector_names)} test connectors...")
    
    for connector_name in connector_names:
        try:
            client.delete_connector(connector_name)
            print(f"Deleted connector: {connector_name}")
        except Exception as e:
            print(f"Error deleting connector {connector_name}: {e}")

def run_load_test(connector_count=5, iterations=3, max_workers=10):
    """Run a load test with multiple connectors."""
    client = ConfluentConnectClient(base_url="http://localhost:8083")
    
    # Create test connectors
    test_connectors = create_test_connectors(client, connector_count)
    time.sleep(5)  # Wait for connectors to start
    
    try:
        # Add existing connectors to the list
        existing_connectors = client.list_connectors()
        all_connectors = list(set(existing_connectors))  # Deduplication
        
        print(f"\nRunning load test across {len(all_connectors)} connectors:")
        for name in all_connectors:
            print(f"- {name}")
        
        # Run the load test for multiple iterations
        for iteration in range(1, iterations + 1):
            print(f"\n--- Load Test Iteration {iteration}/{iterations} ---")
            
            start_time = time.time()
            
            # Monitor all connectors in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_connector = {
                    executor.submit(monitor_connector, client, name): name
                    for name in all_connectors
                }
                
                results = []
                for future in concurrent.futures.as_completed(future_to_connector):
                    connector_name = future_to_connector[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        print(f"Error monitoring connector {connector_name}: {e}")
            
            end_time = time.time()
            total_time = end_time - start_time
            
            # Calculate statistics
            total_response_time = sum(r["response_time_ms"] for r in results)
            avg_response_time = total_response_time / len(results) if results else 0
            max_response_time = max(r["response_time_ms"] for r in results) if results else 0
            min_response_time = min(r["response_time_ms"] for r in results) if results else 0
            
            # Print results
            print(f"\nLoad Test Results (Iteration {iteration}):")
            print(f"Total connectors monitored: {len(results)}")
            print(f"Total time: {round(total_time * 1000, 2)} ms")
            print(f"Average response time: {round(avg_response_time, 2)} ms")
            print(f"Min response time: {round(min_response_time, 2)} ms")
            print(f"Max response time: {round(max_response_time, 2)} ms")
            
            # Print connector states
            states = {}
            for r in results:
                state = r["state"]
                states[state] = states.get(state, 0) + 1
            
            print("\nConnector States:")
            for state, count in states.items():
                print(f"- {state}: {count} connectors")
            
            # Wait between iterations
            if iteration < iterations:
                wait_time = 5
                print(f"\nWaiting {wait_time} seconds before next iteration...")
                time.sleep(wait_time)
    
    finally:
        # Clean up test connectors
        cleanup_test_connectors(client, test_connectors)

if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Run load tests for Kafka Connect')
    parser.add_argument('--connectors', type=int, default=5, help='Number of test connectors to create')
    parser.add_argument('--iterations', type=int, default=3, help='Number of test iterations')
    parser.add_argument('--workers', type=int, default=10, help='Maximum number of parallel workers')
    
    args = parser.parse_args()
    
    run_load_test(
        connector_count=args.connectors,
        iterations=args.iterations,
        max_workers=args.workers
    )
