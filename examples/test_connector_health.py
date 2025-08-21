import os
import sys
import time

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect import (
    ConfluentConnectResource, 
    create_connector_health_sensor
)

def test_connector_health_sensor():
    """Test the connector health sensor by running it once manually."""
    print("Testing connector health sensor...")
    
    # Create the resource instance
    connect_resource = ConfluentConnectResource(connect_url="http://localhost:8083")
    
    # Set up the resource for execution
    connect_resource = connect_resource.setup_for_execution(None)
    
    # Create a health sensor
    health_sensor = create_connector_health_sensor(
        connector_names=["test-mirror-source"],
        # No job_def so we don't trigger any jobs
    )
    
    # Create a simple context object that mimics SensorEvaluationContext
    class MockContext:
        def __init__(self):
            self.resources = type('obj', (object,), {'connect': connect_resource})
            self.log = type('obj', (object,), {
                'info': print,
                'warning': lambda msg: print(f"WARNING: {msg}"),
                'error': lambda msg: print(f"ERROR: {msg}")
            })
            self.cursor = str(int(time.time()))
            
        def get_current_time(self):
            return time.time()
    
    context = MockContext()
    
    # Manually evaluate the sensor once
    print("\nEvaluating sensor...")
    result = health_sensor(context)
    
    # Print the results
    print(f"\nSensor evaluation complete!")
    print(f"Skip reason: {result.skip_reason}")
    print(f"Cursor: {result.cursor}")
    print(f"Run requests: {len(result.run_requests) if result.run_requests else 0}")
    
    return result

if __name__ == "__main__":
    test_connector_health_sensor()
