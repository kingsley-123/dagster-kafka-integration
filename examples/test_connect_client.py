import sys
import os

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect.client import ConfluentConnectClient, ConfluentConnectError

def test_connect_client():
    """Test basic connectivity to Kafka Connect."""
    # This assumes you have Kafka Connect running locally
    # You can adjust the URL as needed
    connect_url = "http://localhost:8083"
    
    try:
        # Create the client
        client = ConfluentConnectClient(base_url=connect_url)
        
        # Try to list connectors
        connectors = client.list_connectors()
        
        print(f"Successfully connected to Kafka Connect at {connect_url}")
        print(f"Found {len(connectors)} connectors: {connectors}")
        
        return True
    except ConfluentConnectError as e:
        print(f"Failed to connect to Kafka Connect: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

if __name__ == "__main__":
    success = test_connect_client()
    
    if not success:
        print("Check if Kafka Connect is running and accessible.")
        print("You can start it using docker-compose from the project root:")
        print("docker-compose up -d")
        
        # Exit with error code
        sys.exit(1)
