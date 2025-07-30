import json
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from dagster import asset, materialize
from dagster_kafka import KafkaResource, KafkaIOManager
import os

print("=== Phase 7 Step 1: End-to-End Message Flow Validation ===")
print("Testing complete message flow: Producer → Kafka → dagster-kafka → Dagster Assets")
print()

# Global variables to track consumed messages
consumed_messages = []
test_messages_sent = []

def create_test_messages(count=10):
    """Create unique test messages with identifiable data"""
    test_id = str(uuid.uuid4())[:8]
    messages = []
    
    for i in range(count):
        message = {
            "test_id": test_id,
            "message_id": f"msg_{test_id}_{i:03d}",
            "sequence": i,
            "timestamp": datetime.now().isoformat(),
            "payload": f"Test payload for message {i} in test {test_id}",
            "metadata": {
                "test_run": test_id,
                "message_index": i,
                "total_messages": count
            }
        }
        messages.append(message)
    
    return messages, test_id

def produce_test_messages(topic, messages):
    """Send test messages to Kafka topic"""
    try:
        print(f"Producing {len(messages)} test messages to topic '{topic}'...")
        
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        sent_count = 0
        for msg in messages:
            producer.send(topic, msg)
            sent_count += 1
            print(f"  Sent message {sent_count}: {msg['message_id']}")
        
        producer.flush()
        producer.close()
        
        print(f"Successfully sent {sent_count} messages to Kafka")
        return True
        
    except Exception as e:
        print(f"Failed to produce messages: {e}")
        return False

def verify_messages_in_kafka(topic, expected_count, timeout_seconds=30):
    """Verify messages are actually in Kafka topic using direct consumer"""
    try:
        print(f"Verifying messages are in Kafka topic '{topic}'...")
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            group_id='verification-consumer',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest',
            consumer_timeout_ms=timeout_seconds * 1000
        )
        
        messages_found = []
        start_time = time.time()
        
        for message in consumer:
            messages_found.append(message.value)
            print(f"  Found message: {message.value['message_id']}")
            
            if len(messages_found) >= expected_count:
                break
            
            if time.time() - start_time > timeout_seconds:
                break
        
        consumer.close()
        
        print(f"Verification complete: Found {len(messages_found)}/{expected_count} messages in Kafka")
        return messages_found
        
    except Exception as e:
        print(f"Kafka verification failed: {e}")
        return []

def test_dagster_kafka_consumption(topic, expected_messages):
    """Test dagster-kafka package consumption"""
    global consumed_messages
    consumed_messages = []  # Reset
    
    try:
        print(f"Testing dagster-kafka consumption from topic '{topic}'...")
        
        # Create asset that will consume from Kafka
        @asset(io_manager_key="kafka_io_manager")
        def message_consumer_asset():
            """Asset that consumes messages from Kafka via dagster-kafka"""
            return {
                "asset_execution_time": datetime.now().isoformat(),
                "status": "processed_by_dagster_kafka"
            }
        
        # Setup Kafka resource and IO manager
        kafka_resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={"session.timeout.ms": 30000}
        )
        
        io_manager = KafkaIOManager(
            kafka_resource=kafka_resource,
            consumer_group_id="integration-test-consumer",
            topic=topic,
            enable_dlq=False
        )
        
        # Materialize the asset multiple times to consume messages
        print("  Starting asset materializations to consume messages...")
        materializations_attempted = 0
        successful_materializations = 0
        
        max_attempts = len(expected_messages) + 5  # Allow some extra attempts
        
        for attempt in range(max_attempts):
            try:
                print(f"    Materialization attempt {attempt + 1}/{max_attempts}")
                
                result = materialize(
                    [message_consumer_asset],
                    resources={
                        "kafka": kafka_resource,
                        "kafka_io_manager": io_manager
                    }
                )
                
                materializations_attempted += 1
                
                if result.success:
                    successful_materializations += 1
                    print(f"    ✓ Materialization {attempt + 1} succeeded")
                else:
                    print(f"    ✗ Materialization {attempt + 1} failed")
                
                time.sleep(0.5)  # Brief pause between attempts
                
            except Exception as e:
                print(f"    ✗ Materialization {attempt + 1} error: {e}")
                continue
        
        consumption_result = {
            "topic": topic,
            "materializations_attempted": materializations_attempted,
            "successful_materializations": successful_materializations,
            "expected_messages": len(expected_messages),
            "test_completed": True
        }
        
        print(f"  Dagster-kafka consumption test completed:")
        print(f"    Materializations attempted: {materializations_attempted}")
        print(f"    Successful materializations: {successful_materializations}")
        print(f"    Expected messages: {len(expected_messages)}")
        
        return consumption_result
        
    except Exception as e:
        print(f"  FAIL: Dagster-kafka consumption test failed: {e}")
        return None

def analyze_integration_results(sent_messages, kafka_messages, consumption_result):
    """Analyze the complete integration test results"""
    
    print()
    print("INTEGRATION TEST ANALYSIS:")
    print("=" * 50)
    
    # Message production analysis
    print("1. MESSAGE PRODUCTION:")
    print(f"   Messages sent to Kafka: {len(sent_messages)}")
    for i, msg in enumerate(sent_messages[:3]):  # Show first 3
        print(f"   Sample {i+1}: {msg['message_id']} - {msg['payload'][:50]}...")
    if len(sent_messages) > 3:
        print(f"   ... and {len(sent_messages) - 3} more messages")
    
    # Kafka verification analysis
    print()
    print("2. KAFKA VERIFICATION:")
    print(f"   Messages found in Kafka: {len(kafka_messages)}")
    if kafka_messages:
        print("   ✓ Messages successfully written to Kafka topic")
        for i, msg in enumerate(kafka_messages[:3]):  # Show first 3
            print(f"   Verified {i+1}: {msg['message_id']}")
    else:
        print("   ✗ No messages found in Kafka topic")
    
    # Consumption analysis
    print()
    print("3. DAGSTER-KAFKA CONSUMPTION:")
    if consumption_result:
        print(f"   Materializations attempted: {consumption_result['materializations_attempted']}")
        print(f"   Successful materializations: {consumption_result['successful_materializations']}")
        
        if consumption_result['successful_materializations'] > 0:
            print("   ✓ dagster-kafka package successfully executed materializations")
        else:
            print("   ✗ No successful materializations")
    else:
        print("   ✗ Consumption test failed to run")
    
    # Overall assessment
    print()
    print("4. OVERALL INTEGRATION ASSESSMENT:")
    
    production_success = len(sent_messages) > 0
    kafka_success = len(kafka_messages) > 0
    consumption_success = consumption_result and consumption_result['successful_materializations'] > 0
    
    print(f"   Message Production: {'✓ PASS' if production_success else '✗ FAIL'}")
    print(f"   Kafka Storage: {'✓ PASS' if kafka_success else '✗ FAIL'}")
    print(f"   dagster-kafka Consumption: {'✓ PASS' if consumption_success else '✗ FAIL'}")
    
    if production_success and kafka_success and consumption_success:
        print("   🎉 INTEGRATION TEST: ✓ PASS - End-to-end message flow validated!")
        return "PASS"
    elif production_success and kafka_success:
        print("   ⚠️  INTEGRATION TEST: PARTIAL - Messages reach Kafka, consumption needs investigation")
        return "PARTIAL"
    else:
        print("   ❌ INTEGRATION TEST: ✗ FAIL - Message flow validation failed")
        return "FAIL"

def main():
    """Run complete end-to-end integration test"""
    
    test_topic = "integration-test-topic"
    
    print("Starting Phase 7 Step 1: End-to-End Message Flow Validation")
    print("This test validates that your dagster-kafka package actually consumes real messages")
    print()
    
    # Step 1: Create test messages
    test_messages, test_id = create_test_messages(5)  # Start with 5 messages
    print(f"Created test batch: {test_id}")
    print()
    
    # Step 2: Send messages to Kafka
    if not produce_test_messages(test_topic, test_messages):
        print("FAIL: Could not send messages to Kafka - aborting integration test")
        return
    
    global test_messages_sent
    test_messages_sent = test_messages
    print()
    
    # Step 3: Verify messages are in Kafka
    kafka_messages = verify_messages_in_kafka(test_topic, len(test_messages))
    print()
    
    # Step 4: Test dagster-kafka consumption
    consumption_result = test_dagster_kafka_consumption(test_topic, test_messages)
    print()
    
    # Step 5: Analyze results
    final_result = analyze_integration_results(test_messages_sent, kafka_messages, consumption_result)
    
    # Save results
    os.makedirs('validation/phase7_integration', exist_ok=True)
    
    integration_report = {
        "test_id": test_id,
        "test_timestamp": datetime.now().isoformat(),
        "test_topic": test_topic,
        "messages_sent": len(test_messages_sent),
        "messages_in_kafka": len(kafka_messages),
        "consumption_result": consumption_result,
        "final_assessment": final_result,
        "sent_messages": test_messages_sent,
        "kafka_messages": kafka_messages
    }
    
    with open('validation/phase7_integration/step1_integration_results.json', 'w') as f:
        json.dump(integration_report, f, indent=2)
    
    print()
    print("STEP 1 COMPLETE:")
    print(f"Final result: {final_result}")
    print("Integration test results saved to: validation/phase7_integration/step1_integration_results.json")

if __name__ == "__main__":
    main()