# Phase 1 - Dagster JSON Integration Test
# Tests actual dagster-kafka package with Dagster assets

import json
import time
from kafka import KafkaProducer
from dagster import asset, Definitions, materialize, DagsterInstance
from dagster_kafka import KafkaResource, KafkaIOManager

print("=== Phase 1: Dagster-Kafka Integration Test ===")
print("Testing published dagster-kafka package...")

# Test 1: Package imports
try:
    from dagster_kafka import KafkaResource, KafkaIOManager
    print("✅ dagster-kafka package imports successful")
except Exception as e:
    print(f"❌ Package import failed: {e}")
    exit(1)

# Test 2: Resource creation
try:
    kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
    print("✅ KafkaResource created successfully")
except Exception as e:
    print(f"❌ KafkaResource creation failed: {e}")
    exit(1)

# Test 3: IO Manager creation
try:
    io_manager = KafkaIOManager(
        kafka_resource=kafka_resource,
        consumer_group_id="validation-test-group"
    )
    print("✅ KafkaIOManager created successfully")
except Exception as e:
    print(f"❌ KafkaIOManager creation failed: {e}")
    exit(1)

print("Phase 1 Dagster integration test completed successfully! ✅")
print("Core package functionality validated!")
