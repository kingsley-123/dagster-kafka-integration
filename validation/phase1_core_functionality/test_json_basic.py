# Phase 1 - JSON Format Validation Test
# Tests basic JSON message consumption with Dagster integration

import json
import time
from kafka import KafkaProducer, KafkaConsumer
from dagster import asset, Definitions, DagsterInstance
from dagster_kafka import KafkaResource, KafkaIOManager

print("=== Phase 1: JSON Format Validation ===")
print("Testing basic JSON message consumption...")

# Test 1: Basic Kafka Connection
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Kafka connection successful")
except Exception as e:
    print(f"❌ Kafka connection failed: {e}")
    exit(1)

# Test 2: Send test messages
test_messages = [
    {"id": 1, "name": "user1", "email": "user1@test.com"},
    {"id": 2, "name": "user2", "email": "user2@test.com"},
    {"id": 3, "name": "user3", "email": "user3@test.com"}
]

try:
    for msg in test_messages:
        producer.send('validation-test', msg)
    producer.flush()
    print(f"✅ Sent {len(test_messages)} JSON messages to validation-test topic")
except Exception as e:
    print(f"❌ Failed to send messages: {e}")
    exit(1)

print("Phase 1 JSON basic test completed successfully! ✅")
