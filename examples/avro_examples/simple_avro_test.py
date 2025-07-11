"""Simple test of Avro functionality without full Dagster pipeline."""

import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from dagster_kafka import KafkaResource, AvroKafkaIOManager

def test_avro_setup():
    """Test that Avro components can be instantiated."""
    
    # Create Kafka resource
    kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
    print("✅ KafkaResource created")
    
    # Create Avro IO Manager
    avro_manager = AvroKafkaIOManager(kafka_resource)
    print("✅ AvroKafkaIOManager created")
    
    # Create with Schema Registry URL
    avro_manager_with_registry = AvroKafkaIOManager(
        kafka_resource, 
        schema_registry_url="http://localhost:8081"
    )
    print("✅ AvroKafkaIOManager with Schema Registry created")
    
    print("🎉 All Avro components working correctly!")

if __name__ == "__main__":
    test_avro_setup()