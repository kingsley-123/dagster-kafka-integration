import os
import sys
import time
import threading
import subprocess

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import ConfluentConnectClient

def simulate_failure(connector_name, delay_seconds=10):
    """Simulate a connector failure by pausing it after a delay."""
    print(f"[Simulator] Will pause connector {connector_name} in {delay_seconds} seconds...")
    time.sleep(delay_seconds)
    
    client = ConfluentConnectClient(base_url="http://localhost:8083")
    
    try:
        print(f"[Simulator] Pausing connector {connector_name} to simulate failure...")
        client.pause_connector(connector_name)
        print(f"[Simulator] Connector paused successfully. It should be detected as unhealthy.")
    except Exception as e:
        print(f"[Simulator] Error pausing connector: {e}")

def main():
    # Validate arguments
    if len(sys.argv) < 2:
        print("Usage: python simulate_failure.py <connector_name>")
        sys.exit(1)
    
    connector_name = sys.argv[1]
    
    # Start the health monitor in a separate process with auto-restart enabled
    monitor_cmd = [
        "python", 
        os.path.join(os.path.dirname(__file__), "connect_health_monitor.py"),
        "--interval", "5",  # Check every 5 seconds for quicker testing
        "--auto-restart",
        connector_name
    ]
    
    # Start the health monitor
    print(f"Starting health monitor for {connector_name} with auto-restart...")
    monitor_process = subprocess.Popen(
        monitor_cmd, 
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )
    
    # Start a thread to simulate failure
    failure_thread = threading.Thread(
        target=simulate_failure,
        args=(connector_name, 15)  # Pause connector after 15 seconds
    )
    failure_thread.daemon = True
    failure_thread.start()
    
    # Output from health monitor
    try:
        while True:
            line = monitor_process.stdout.readline()
            if not line and monitor_process.poll() is not None:
                break
            if line:
                print(line.rstrip())
    except KeyboardInterrupt:
        print("Test interrupted by user")
    finally:
        # Clean up
        if monitor_process.poll() is None:
            monitor_process.terminate()
            print("Health monitor terminated")
    
    # Check monitor output for error messages
    stderr = monitor_process.stderr.read()
    if stderr:
        print("Errors from health monitor:")
        print(stderr)

if __name__ == "__main__":
    main()
