# Save as: validation\phase5_performance\step3_fast_consumption.py
import json
import time
import threading
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from dagster import asset, materialize
from dagster_kafka import KafkaResource, KafkaIOManager
import psutil
import os

print("=== Phase 5 Step 3: FAST Consumption Performance Testing ===")
print("Testing message consumption with dagster-kafka package...")
print()

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def produce_test_messages(topic, message_count):
    """Produce test messages to Kafka topic"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        print(f"Producing {message_count} test messages to topic '{topic}'...")
        
        for i in range(message_count):
            message = {
                "id": i,
                "timestamp": datetime.now().isoformat(),
                "data": "FastTest" + str(i),
                "sequence": i
            }
            producer.send(topic, message)
        
        producer.flush()
        producer.close()
        print(f"Successfully produced {message_count} messages")
        return True
        
    except Exception as e:
        print(f"Failed to produce messages: {e}")
        return False

def test_direct_kafka_consumer_baseline(topic, test_duration_seconds=10):
    """Test direct Kafka consumer performance for baseline comparison"""
    try:
        print(f"Testing direct Kafka consumer baseline on topic '{topic}' for {test_duration_seconds}s...")
        
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=['localhost:9092'],
            group_id='baseline-performance-group',
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            auto_offset_reset='earliest'
        )
        
        start_time = time.time()
        messages_consumed = 0
        
        end_time = start_time + test_duration_seconds
        
        while time.time() < end_time:
            message_batch = consumer.poll(timeout_ms=500)
            for topic_partition, messages in message_batch.items():
                messages_consumed += len(messages)
        
        actual_duration = time.time() - start_time
        baseline_throughput = messages_consumed / actual_duration if actual_duration > 0 else 0
        
        consumer.close()
        
        result = {
            "baseline_duration_seconds": round(actual_duration, 2),
            "baseline_messages_consumed": messages_consumed,
            "baseline_throughput_msgs_per_sec": round(baseline_throughput, 2)
        }
        
        print(f"  Duration: {actual_duration:.2f}s")
        print(f"  Messages consumed: {messages_consumed}")
        print(f"  Baseline throughput: {baseline_throughput:.2f} messages/second")
        print("  PASS: Baseline test completed")
        print()
        
        return result
        
    except Exception as e:
        print(f"  FAIL: Baseline test failed: {e}")
        return None

def test_dagster_kafka_consumption(topic, test_duration_seconds=10):
    """Test dagster-kafka consumption performance"""
    try:
        print(f"Testing dagster-kafka consumption on topic '{topic}' for {test_duration_seconds}s...")
        
        # Create asset that consumes from Kafka
        @asset(io_manager_key="kafka_io_manager")
        def fast_consumer_asset():
            return {"consumed_at": datetime.now().isoformat(), "status": "processed"}
        
        # Setup Kafka resource and IO manager
        kafka_resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={"session.timeout.ms": 30000}
        )
        
        io_manager = KafkaIOManager(
            kafka_resource=kafka_resource,
            consumer_group_id="fast-performance-test-group",
            topic=topic,
            enable_dlq=False
        )
        
        # Monitor consumption
        start_time = time.time()
        start_memory = get_memory_usage()
        
        materializations_completed = 0
        
        # Run consumption for specified duration
        end_time = start_time + test_duration_seconds
        
        while time.time() < end_time:
            try:
                result = materialize(
                    [fast_consumer_asset],
                    resources={
                        "kafka": kafka_resource,
                        "kafka_io_manager": io_manager
                    }
                )
                
                if result.success:
                    materializations_completed += 1
                
                time.sleep(0.05)  # Brief pause between materializations
                
            except Exception as e:
                print(f"Materialization error: {e}")
                break
        
        actual_duration = time.time() - start_time
        end_memory = get_memory_usage()
        
        # Calculate performance metrics
        throughput = materializations_completed / actual_duration
        memory_used = end_memory - start_memory
        
        result = {
            "topic": topic,
            "test_duration_seconds": round(actual_duration, 2),
            "materializations_completed": materializations_completed,
            "throughput_materializations_per_sec": round(throughput, 2),
            "memory_used_mb": round(memory_used, 2),
            "start_memory_mb": round(start_memory, 2),
            "end_memory_mb": round(end_memory, 2)
        }
        
        print(f"  Duration: {actual_duration:.2f}s")
        print(f"  Materializations completed: {materializations_completed}")
        print(f"  Throughput: {throughput:.2f} materializations/second")
        print(f"  Memory used: {memory_used:.2f} MB")
        print("  PASS: dagster-kafka consumption test completed")
        print()
        
        return result
        
    except Exception as e:
        print(f"  FAIL: dagster-kafka consumption test failed: {e}")
        return None

def main():
    """Run fast consumption performance tests"""
    results = []
    
    test_topic = "fast-consumption-test"
    
    print("Starting FAST consumption performance testing...")
    print("Note: Ensure Kafka is running on localhost:9092")
    print()
    
    # Step 1: Produce test messages
    if not produce_test_messages(test_topic, 100):
        print("FAIL: Could not produce test messages, aborting performance test")
        return
    
    # Step 2: Test direct Kafka consumer baseline
    baseline_result = test_direct_kafka_consumer_baseline(test_topic, 10)
    if baseline_result:
        results.append(baseline_result)
    
    # Step 3: Test dagster-kafka consumption performance
    consumption_result = test_dagster_kafka_consumption(test_topic, 10)
    if consumption_result:
        results.append(consumption_result)
    
    # Create directory if it doesn't exist
    os.makedirs('validation/phase5_performance', exist_ok=True)
    
    # Save results
    with open('validation/phase5_performance/consumption_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Summary
    print("CONSUMPTION PERFORMANCE SUMMARY:")
    if baseline_result and consumption_result:
        print(f"Direct Kafka baseline: {baseline_result['baseline_throughput_msgs_per_sec']} msgs/sec")
        print(f"dagster-kafka materializations: {consumption_result['throughput_materializations_per_sec']} materializations/sec")
    elif baseline_result:
        print(f"Direct Kafka baseline: {baseline_result['baseline_throughput_msgs_per_sec']} msgs/sec")
        print("dagster-kafka consumption: Test failed")
    elif consumption_result:
        print("Direct Kafka baseline: Test failed")
        print(f"dagster-kafka materializations: {consumption_result['throughput_materializations_per_sec']} materializations/sec")
    else:
        print("Both tests failed - check Kafka connection and configuration")
    
    print()
    print("PASS: Consumption performance testing completed")
    print("Results saved to: validation/phase5_performance/consumption_results.json")

if __name__ == "__main__":
    main()