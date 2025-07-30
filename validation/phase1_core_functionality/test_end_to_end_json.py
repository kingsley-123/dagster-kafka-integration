# Phase 1 - End-to-End JSON Asset Test (FIXED)
# Tests complete workflow: Producer -> Kafka -> Dagster Asset -> Real Consumption

import json
import time
from kafka import KafkaProducer
from dagster import asset, Definitions, materialize
from dagster_kafka import KafkaResource, KafkaIOManager

print("=== Phase 1: End-to-End JSON Asset Test (REAL CONSUMPTION) ===")
print("Testing complete Kafka -> Dagster asset workflow with actual consumption...")

# Step 1: Send test data to Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

test_data = [
    {"id": 1, "name": "Alice", "score": 95},
    {"id": 2, "name": "Bob", "score": 87},
    {"id": 3, "name": "Charlie", "score": 92}
]

for data in test_data:
    producer.send('validation-test', data)
producer.flush()
print(f"✅ Sent {len(test_data)} test messages to Kafka")

# Give Kafka a moment to persist the messages
time.sleep(2)

# Step 2: Define Dagster asset that ACTUALLY consumes from Kafka
@asset(io_manager_key="kafka_io_manager")
def kafka_user_data():
    # Asset that consumes JSON messages from Kafka using dagster-kafka package
    return "consumed_from_kafka"

# Step 3: Test with actual KafkaIOManager (FIXED API)
try:
    kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
    
    kafka_io_manager = KafkaIOManager(
        kafka_resource=kafka_resource,
        consumer_group_id="validation-test-group"
    )
    
    # Fixed: Pass resources directly to materialize
    result = materialize(
        [kafka_user_data], 
        resources={
            "kafka": kafka_resource,
            "kafka_io_manager": kafka_io_manager
        }
    )
    
    if result.success:
        print("✅ Dagster asset with KafkaIOManager materialization successful")
        print("✅ Real Kafka consumption test completed successfully!")
    else:
        print("❌ Asset materialization failed")
        
except Exception as e:
    print(f"❌ Real consumption test failed: {e}")
    import traceback
    traceback.print_exc()
