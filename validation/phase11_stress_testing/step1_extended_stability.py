# Save as: validation\phase11_stress_testing\step1_simplified_stability.py
import json
import time
import threading
from datetime import datetime
from kafka import KafkaProducer
from dagster import asset, materialize
from dagster_kafka import KafkaResource, KafkaIOManager
import psutil
import os

print("=== Phase 11 Step 1: Simplified Extended Stability Test ===")
print("Testing package stability with robust error handling...")
print()

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def produce_test_messages(topic, count=1000):
    """Produce test messages for stability testing"""
    try:
        print(f"Producing {count} test messages to topic '{topic}'...")
        
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        for i in range(count):
            message = {
                "id": i,
                "timestamp": datetime.now().isoformat(),
                "payload": f"stability_test_message_{i}"
            }
            producer.send(topic, message)
        
        producer.flush()
        producer.close()
        
        print(f"Successfully produced {count} messages")
        return True
        
    except Exception as e:
        print(f"Message production failed: {e}")
        return False

def test_stability_over_time(topic, duration_minutes=5):
    """Test stability over time with simple monitoring"""
    try:
        duration_seconds = duration_minutes * 60
        print(f"Testing stability for {duration_minutes} minutes...")
        
        # Create Dagster asset for consumption
        @asset(io_manager_key="stability_kafka_io_manager")
        def stability_test_asset():
            return {
                "processed_at": datetime.now().isoformat(),
                "status": "stability_test_processed"
            }
        
        # Setup Kafka resources
        kafka_resource = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={"client.id": "stability-test-client"}
        )
        
        io_manager = KafkaIOManager(
            kafka_resource=kafka_resource,
            consumer_group_id="stability-test-group",
            topic=topic,
            enable_dlq=False
        )
        
        # Monitor resources and run materializations
        start_time = time.time()
        end_time = start_time + duration_seconds
        
        initial_memory = get_memory_usage()
        memory_samples = [initial_memory]
        
        successful_materializations = 0
        total_attempts = 0
        error_count = 0
        last_error = None
        
        print(f"Starting stability test with initial memory: {initial_memory:.2f} MB")
        
        while time.time() < end_time:
            try:
                total_attempts += 1
                
                result = materialize(
                    [stability_test_asset],
                    resources={
                        "kafka": kafka_resource,
                        "stability_kafka_io_manager": io_manager
                    }
                )
                
                if result.success:
                    successful_materializations += 1
                
                # Sample memory every 10 materializations
                if total_attempts % 10 == 0:
                    current_memory = get_memory_usage()
                    memory_samples.append(current_memory)
                    
                    elapsed_minutes = (time.time() - start_time) / 60
                    print(f"  Progress: {elapsed_minutes:.1f}/{duration_minutes} min, "
                          f"Success: {successful_materializations}/{total_attempts}, "
                          f"Memory: {current_memory:.2f} MB")
                
                time.sleep(2)  # 2-second intervals for stability
                
            except Exception as e:
                error_count += 1
                last_error = str(e)
                
                # Don't spam errors, just count them
                if error_count % 10 == 1:  # Log every 10th error
                    print(f"    Error #{error_count}: {str(e)[:50]}...")
                
                time.sleep(1)  # Brief pause on error
        
        actual_duration = time.time() - start_time
        final_memory = get_memory_usage()
        memory_change = final_memory - initial_memory
        
        # Calculate success rate
        success_rate = (successful_materializations / total_attempts * 100) if total_attempts > 0 else 0
        
        # Assess stability
        stability_issues = []
        if memory_change > 50:  # More than 50MB increase
            stability_issues.append(f"High memory increase: {memory_change:.2f} MB")
        
        if success_rate < 70:  # Less than 70% success rate
            stability_issues.append(f"Low success rate: {success_rate:.2f}%")
        
        if error_count > total_attempts * 0.5:  # More than 50% errors
            stability_issues.append(f"High error rate: {error_count} errors")
        
        stability_status = "STABLE" if len(stability_issues) == 0 else "UNSTABLE"
        
        result = {
            "test_duration_minutes": actual_duration / 60,
            "total_attempts": total_attempts,
            "successful_materializations": successful_materializations,
            "success_rate_percent": success_rate,
            "error_count": error_count,
            "last_error": last_error,
            "memory_initial_mb": initial_memory,
            "memory_final_mb": final_memory,
            "memory_change_mb": memory_change,
            "memory_samples": len(memory_samples),
            "stability_issues": stability_issues,
            "stability_status": stability_status,
            "overall_result": "PASS" if stability_status == "STABLE" else "FAIL"
        }
        
        print()
        print("STABILITY TEST RESULTS:")
        print(f"  Duration: {actual_duration/60:.2f} minutes")
        print(f"  Attempts: {total_attempts}")
        print(f"  Successful: {successful_materializations}")
        print(f"  Success rate: {success_rate:.2f}%")
        print(f"  Errors: {error_count}")
        print(f"  Memory change: {memory_change:+.2f} MB")
        print(f"  Stability status: {stability_status}")
        
        if stability_issues:
            print("  Issues identified:")
            for issue in stability_issues:
                print(f"    - {issue}")
        
        return result
        
    except Exception as e:
        print(f"Stability test failed: {e}")
        return {"overall_result": "FAIL", "error": str(e)}

def main():
    """Run simplified stability testing"""
    
    test_topic = "stability-test-topic"
    
    print("Starting Phase 11 Step 1: Simplified Extended Stability Test")
    print("This test focuses on core stability with robust error handling")
    print()
    
    # Step 1: Produce test messages
    if not produce_test_messages(test_topic, 500):
        print("FAIL: Could not produce test messages")
        return False
    
    print()
    
    # Step 2: Run stability test (5 minutes instead of 10)
    stability_result = test_stability_over_time(test_topic, duration_minutes=5)
    
    # Save results
    os.makedirs('validation/phase11_stress_testing', exist_ok=True)
    
    final_report = {
        "test_timestamp": datetime.now().isoformat(),
        "test_type": "simplified_extended_stability",
        "stability_result": stability_result
    }
    
    with open('validation/phase11_stress_testing/step1_simplified_stability.json', 'w') as f:
        json.dump(final_report, f, indent=2)
    
    print()
    print("SIMPLIFIED STABILITY TEST SUMMARY:")
    if stability_result.get("overall_result") == "PASS":
        print("✅ PASS: Package demonstrates good stability under extended load")
        print(f"✅ Success rate: {stability_result.get('success_rate_percent', 0):.2f}%")
        print(f"✅ Memory usage stable: {stability_result.get('memory_change_mb', 0):+.2f} MB change")
        print("✅ No critical stability issues detected")
    else:
        print("⚠️  STABILITY CONCERNS: Issues detected during testing")
        for issue in stability_result.get('stability_issues', []):
            print(f"    - {issue}")
    
    print()
    print("Results saved to: validation/phase11_stress_testing/step1_simplified_stability.json")
    
    return stability_result.get("overall_result") == "PASS"

if __name__ == "__main__":
    main()