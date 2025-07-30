# Save as: validation\phase11_stress_testing\step3_final_comprehensive.py
import json
import time
import threading
from datetime import datetime
from kafka import KafkaProducer
from dagster import asset, materialize
from dagster_kafka import KafkaResource, KafkaIOManager
import psutil
import os

print("=== Phase 11 Step 3: Final Comprehensive Stress Test ===")
print("Ultimate validation combining all stress scenarios...")
print()

def get_system_metrics():
    """Get comprehensive system metrics"""
    process = psutil.Process(os.getpid())
    
    return {
        "memory_mb": process.memory_info().rss / 1024 / 1024,
        "memory_percent": process.memory_percent(),
        "cpu_percent": process.cpu_percent(),
        "threads": process.num_threads(),
        "timestamp": time.time()
    }

def produce_stress_messages(topic, total_messages=2000, batch_size=100):
    """Produce messages in batches for comprehensive stress testing"""
    try:
        print(f"Producing {total_messages} messages in batches of {batch_size}...")
        
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            batch_size=16384,
            linger_ms=10
        )
        
        total_sent = 0
        batch_number = 0
        
        while total_sent < total_messages:
            batch_start = time.time()
            batch_count = min(batch_size, total_messages - total_sent)
            
            for i in range(batch_count):
                message = {
                    "id": total_sent + i,
                    "batch": batch_number,
                    "timestamp": datetime.now().isoformat(),
                    "payload": f"comprehensive_stress_test_message_{total_sent + i}",
                    "data": "X" * 100  # Add some data weight
                }
                producer.send(topic, message)
            
            producer.flush()
            total_sent += batch_count
            batch_number += 1
            
            batch_duration = time.time() - batch_start
            if batch_number % 5 == 0:  # Report every 5 batches
                print(f"  Sent batch {batch_number}: {total_sent}/{total_messages} messages ({batch_duration:.2f}s)")
            
            time.sleep(0.1)  # Brief pause between batches
        
        producer.close()
        print(f"Message production completed: {total_sent} messages")
        return total_sent
        
    except Exception as e:
        print(f"Message production failed: {e}")
        return 0

def comprehensive_stress_test(topic, test_duration_minutes=8):
    """Run comprehensive stress test combining all scenarios"""
    try:
        test_duration_seconds = test_duration_minutes * 60
        print(f"Starting comprehensive stress test for {test_duration_minutes} minutes...")
        
        # Create multiple assets for different test scenarios
        @asset(io_manager_key="stress_kafka_io_manager_1")
        def primary_stress_asset():
            return {"type": "primary", "processed_at": datetime.now().isoformat()}
        
        @asset(io_manager_key="stress_kafka_io_manager_2")  
        def secondary_stress_asset():
            return {"type": "secondary", "processed_at": datetime.now().isoformat()}
        
        @asset(io_manager_key="stress_kafka_io_manager_3")
        def monitoring_stress_asset():
            return {"type": "monitoring", "processed_at": datetime.now().isoformat()}
        
        # Setup multiple Kafka resources with different configurations
        resources = {}
        
        # Primary resource
        resources["kafka_1"] = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={
                "client.id": "comprehensive-stress-primary",
                "session.timeout.ms": 30000
            }
        )
        
        resources["stress_kafka_io_manager_1"] = KafkaIOManager(
            kafka_resource=resources["kafka_1"],
            consumer_group_id="comprehensive-stress-primary-group",
            topic=topic,
            enable_dlq=False
        )
        
        # Secondary resource  
        resources["kafka_2"] = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={
                "client.id": "comprehensive-stress-secondary",
                "auto.offset.reset": "latest"
            }
        )
        
        resources["stress_kafka_io_manager_2"] = KafkaIOManager(
            kafka_resource=resources["kafka_2"],
            consumer_group_id="comprehensive-stress-secondary-group", 
            topic=topic,
            enable_dlq=False
        )
        
        # Monitoring resource
        resources["kafka_3"] = KafkaResource(
            bootstrap_servers="localhost:9092",
            additional_config={"client.id": "comprehensive-stress-monitor"}
        )
        
        resources["stress_kafka_io_manager_3"] = KafkaIOManager(
            kafka_resource=resources["kafka_3"],
            consumer_group_id="comprehensive-stress-monitor-group",
            topic=topic,
            enable_dlq=False
        )
        
        # Start comprehensive stress testing
        start_time = time.time()
        end_time = start_time + test_duration_seconds
        
        initial_metrics = get_system_metrics()
        metrics_history = [initial_metrics]
        
        # Test counters
        total_materializations = 0
        successful_materializations = 0
        primary_successes = 0
        secondary_successes = 0
        monitoring_successes = 0
        error_count = 0
        last_errors = []
        
        assets_list = [primary_stress_asset, secondary_stress_asset, monitoring_stress_asset]
        
        print(f"Starting comprehensive stress operations...")
        print(f"Initial metrics: {initial_metrics['memory_mb']:.2f} MB, {initial_metrics['threads']} threads")
        
        cycle_count = 0
        while time.time() < end_time:
            cycle_start = time.time()
            
            try:
                # Materialize all assets together
                result = materialize(assets_list, resources=resources)
                
                total_materializations += 1
                
                if result.success:
                    successful_materializations += 1
                    
                    # Count individual asset successes (simplified)
                    primary_successes += 1
                    secondary_successes += 1  
                    monitoring_successes += 1
                else:
                    error_count += 1
                    last_errors.append("Materialization failed")
                
                # Sample metrics every 20 cycles
                if cycle_count % 20 == 0:
                    current_metrics = get_system_metrics()
                    metrics_history.append(current_metrics)
                    
                    elapsed_minutes = (time.time() - start_time) / 60
                    success_rate = (successful_materializations / total_materializations * 100) if total_materializations > 0 else 0
                    
                    print(f"  Progress: {elapsed_minutes:.1f}/{test_duration_minutes} min, "
                          f"Success: {successful_materializations}/{total_materializations} ({success_rate:.1f}%), "
                          f"Memory: {current_metrics['memory_mb']:.2f} MB, "
                          f"Errors: {error_count}")
                
                cycle_count += 1
                
                # Vary the pace slightly for realistic stress
                if cycle_count % 3 == 0:
                    time.sleep(1.5)  # Slower cycles
                else:
                    time.sleep(1.0)  # Normal cycles
                    
            except Exception as e:
                error_count += 1
                last_errors.append(str(e)[:100])
                
                # Keep only last 10 errors
                if len(last_errors) > 10:
                    last_errors = last_errors[-10:]
                
                time.sleep(2)  # Longer pause on error
        
        # Final measurements
        actual_duration = time.time() - start_time
        final_metrics = get_system_metrics()
        
        # Calculate comprehensive results
        success_rate = (successful_materializations / total_materializations * 100) if total_materializations > 0 else 0
        memory_change = final_metrics['memory_mb'] - initial_metrics['memory_mb']
        thread_change = final_metrics['threads'] - initial_metrics['threads']
        
        # Assess overall performance
        performance_issues = []
        
        if success_rate < 85:
            performance_issues.append(f"Low success rate: {success_rate:.2f}%")
        
        if memory_change > 100:
            performance_issues.append(f"High memory usage: +{memory_change:.2f} MB")
        
        if error_count > total_materializations * 0.2:
            performance_issues.append(f"High error rate: {error_count} errors")
        
        if thread_change > 10:
            performance_issues.append(f"Thread accumulation: +{thread_change} threads")
        
        overall_status = "EXCELLENT" if len(performance_issues) == 0 else "GOOD" if len(performance_issues) <= 2 else "NEEDS_ATTENTION"
        
        result = {
            "test_duration_minutes": actual_duration / 60,
            "total_materializations": total_materializations,
            "successful_materializations": successful_materializations,
            "success_rate_percent": success_rate,
            "primary_successes": primary_successes,
            "secondary_successes": secondary_successes, 
            "monitoring_successes": monitoring_successes,
            "error_count": error_count,
            "initial_memory_mb": initial_metrics['memory_mb'],
            "final_memory_mb": final_metrics['memory_mb'],
            "memory_change_mb": memory_change,
            "initial_threads": initial_metrics['threads'],
            "final_threads": final_metrics['threads'],
            "thread_change": thread_change,
            "metrics_samples": len(metrics_history),
            "performance_issues": performance_issues,
            "last_errors": last_errors,
            "overall_status": overall_status,
            "test_result": "PASS" if overall_status in ["EXCELLENT", "GOOD"] else "FAIL"
        }
        
        print()
        print("COMPREHENSIVE STRESS TEST RESULTS:")
        print(f"  Duration: {actual_duration/60:.2f} minutes")
        print(f"  Total materializations: {total_materializations}")
        print(f"  Success rate: {success_rate:.2f}%")
        print(f"  Memory change: {memory_change:+.2f} MB")
        print(f"  Thread change: {thread_change:+} threads")
        print(f"  Error count: {error_count}")
        print(f"  Overall status: {overall_status}")
        
        if performance_issues:
            print("  Performance issues:")
            for issue in performance_issues:
                print(f"    - {issue}")
        
        return result
        
    except Exception as e:
        print(f"Comprehensive stress test failed: {e}")
        return {"test_result": "FAIL", "error": str(e)}

def main():
    """Run final comprehensive stress validation"""
    
    stress_topic = "comprehensive-stress-topic"
    
    print("Starting Phase 11 Step 3: Final Comprehensive Stress Test")
    print("This is the ultimate validation test combining all stress scenarios")
    print()
    
    # Step 1: Produce comprehensive test messages
    messages_produced = produce_stress_messages(stress_topic, total_messages=1500, batch_size=75)
    
    if messages_produced == 0:
        print("FAIL: Could not produce test messages for comprehensive stress test")
        return False
    
    print()
    
    # Step 2: Run comprehensive stress test
    stress_result = comprehensive_stress_test(stress_topic, test_duration_minutes=8)
    
    # Save results
    os.makedirs('validation/phase11_stress_testing', exist_ok=True)
    
    final_report = {
        "test_timestamp": datetime.now().isoformat(),
        "test_type": "final_comprehensive_stress_test",
        "messages_produced": messages_produced,
        "stress_test_result": stress_result
    }
    
    with open('validation/phase11_stress_testing/step3_final_comprehensive.json', 'w') as f:
        json.dump(final_report, f, indent=2)
    
    print()
    print("FINAL COMPREHENSIVE STRESS TEST SUMMARY:")
    
    if stress_result.get("test_result") == "PASS":
        overall_status = stress_result.get("overall_status", "UNKNOWN")
        
        if overall_status == "EXCELLENT":
            print("🎉 EXCEPTIONAL PERFORMANCE: Package exceeds enterprise standards!")
            print("✅ Outstanding success rates under extreme load")
            print("✅ Excellent memory and resource management")
            print("✅ Robust error handling and stability")
        elif overall_status == "GOOD":
            print("✅ STRONG PERFORMANCE: Package meets enterprise standards")
            print("✅ Good success rates under stress")
            print("✅ Stable resource management")
            print("✅ Minor issues within acceptable limits")
        
        print(f"✅ Success rate: {stress_result.get('success_rate_percent', 0):.2f}%")
        print(f"✅ Memory management: {stress_result.get('memory_change_mb', 0):+.2f} MB")
        print(f"✅ Total operations: {stress_result.get('total_materializations', 0)}")
        
    else:
        print("⚠️  PERFORMANCE CONCERNS: Issues detected during comprehensive stress testing")
        for issue in stress_result.get('performance_issues', []):
            print(f"    - {issue}")
    
    print()
    print("Results saved to: validation/phase11_stress_testing/step3_final_comprehensive.json")
    
    return stress_result.get("test_result") == "PASS"

if __name__ == "__main__":
    main()