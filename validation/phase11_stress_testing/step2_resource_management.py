# Save as: validation\phase11_stress_testing\step2_resource_management.py
import json
import time
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from dagster import asset, materialize
from dagster_kafka import KafkaResource, KafkaIOManager
import psutil
import os
import gc

print("=== Phase 11 Step 2: Resource Management Under Pressure ===")
print("Testing resource cleanup, memory management, and connection handling...")
print()

def get_detailed_memory_info():
    """Get detailed memory information"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    
    return {
        "rss_mb": memory_info.rss / 1024 / 1024,  # Resident Set Size
        "vms_mb": memory_info.vms / 1024 / 1024,  # Virtual Memory Size
        "percent": process.memory_percent(),
        "num_threads": process.num_threads(),
        "num_fds": process.num_fds() if hasattr(process, 'num_fds') else 0
    }

def test_resource_creation_cleanup_cycles(cycles=20):
    """Test repeated resource creation and cleanup"""
    try:
        print(f"Testing resource creation/cleanup over {cycles} cycles...")
        
        initial_memory = get_detailed_memory_info()
        memory_snapshots = [initial_memory]
        
        for cycle in range(cycles):
            cycle_start_memory = get_detailed_memory_info()
            
            # Create multiple resources
            resources = []
            io_managers = []
            
            for i in range(10):  # 10 resources per cycle
                kafka_resource = KafkaResource(
                    bootstrap_servers="localhost:9092",
                    additional_config={
                        "client.id": f"resource-test-cycle-{cycle}-{i}",
                        "session.timeout.ms": 30000
                    }
                )
                
                io_manager = KafkaIOManager(
                    kafka_resource=kafka_resource,
                    consumer_group_id=f"resource-test-group-{cycle}-{i}",
                    enable_dlq=False
                )
                
                resources.append(kafka_resource)
                io_managers.append(io_manager)
            
            cycle_peak_memory = get_detailed_memory_info()
            
            # Cleanup resources
            del resources
            del io_managers
            
            # Force garbage collection
            gc.collect()
            time.sleep(0.1)  # Brief pause for cleanup
            
            cycle_end_memory = get_detailed_memory_info()
            memory_snapshots.append(cycle_end_memory)
            
            if cycle % 5 == 0:  # Report every 5 cycles
                print(f"  Cycle {cycle}: Memory {cycle_start_memory['rss_mb']:.2f} → "
                      f"{cycle_peak_memory['rss_mb']:.2f} → {cycle_end_memory['rss_mb']:.2f} MB")
        
        final_memory = get_detailed_memory_info()
        total_memory_change = final_memory['rss_mb'] - initial_memory['rss_mb']
        
        # Analyze memory stability
        memory_values = [snap['rss_mb'] for snap in memory_snapshots]
        max_memory = max(memory_values)
        avg_memory = sum(memory_values) / len(memory_values)
        
        # Check for memory leaks
        memory_leak_threshold = 20  # MB
        has_memory_leak = total_memory_change > memory_leak_threshold
        
        result = {
            "test_type": "resource_creation_cleanup",
            "cycles": cycles,
            "resources_per_cycle": 10,
            "initial_memory_mb": initial_memory['rss_mb'],
            "final_memory_mb": final_memory['rss_mb'],
            "total_memory_change_mb": total_memory_change,
            "max_memory_mb": max_memory,
            "avg_memory_mb": avg_memory,
            "has_memory_leak": has_memory_leak,
            "memory_leak_threshold_mb": memory_leak_threshold,
            "final_threads": final_memory['num_threads'],
            "final_file_descriptors": final_memory['num_fds'],
            "status": "PASS" if not has_memory_leak else "FAIL"
        }
        
        print()
        print("RESOURCE CLEANUP TEST RESULTS:")
        print(f"  Total memory change: {total_memory_change:+.2f} MB")
        print(f"  Max memory usage: {max_memory:.2f} MB")
        print(f"  Memory leak detected: {'YES' if has_memory_leak else 'NO'}")
        print(f"  Final threads: {final_memory['num_threads']}")
        print(f"  Test status: {result['status']}")
        
        return result
        
    except Exception as e:
        print(f"Resource cleanup test failed: {e}")
        return {"test_type": "resource_creation_cleanup", "status": "FAIL", "error": str(e)}

def test_concurrent_resource_usage(num_threads=10, operations_per_thread=20):
    """Test concurrent resource usage"""
    try:
        print(f"Testing concurrent resource usage: {num_threads} threads, {operations_per_thread} ops each...")
        
        def worker_thread(thread_id):
            """Worker function for concurrent testing"""
            thread_results = {
                "thread_id": thread_id,
                "successful_operations": 0,
                "failed_operations": 0,
                "errors": []
            }
            
            for op in range(operations_per_thread):
                try:
                    # Create asset for this operation
                    @asset(io_manager_key=f"concurrent_kafka_io_manager_{thread_id}")
                    def concurrent_test_asset():
                        return {
                            "thread_id": thread_id,
                            "operation": op,
                            "timestamp": datetime.now().isoformat()
                        }
                    
                    # Create resources
                    kafka_resource = KafkaResource(
                        bootstrap_servers="localhost:9092",
                        additional_config={"client.id": f"concurrent-test-{thread_id}-{op}"}
                    )
                    
                    io_manager = KafkaIOManager(
                        kafka_resource=kafka_resource,
                        consumer_group_id=f"concurrent-test-group-{thread_id}",
                        topic="concurrent-test-topic",
                        enable_dlq=False
                    )
                    
                    # Materialize asset
                    result = materialize(
                        [concurrent_test_asset],
                        resources={
                            "kafka": kafka_resource,
                            f"concurrent_kafka_io_manager_{thread_id}": io_manager
                        }
                    )
                    
                    if result.success:
                        thread_results["successful_operations"] += 1
                    else:
                        thread_results["failed_operations"] += 1
                    
                    time.sleep(0.1)  # Brief pause between operations
                    
                except Exception as e:
                    thread_results["failed_operations"] += 1
                    thread_results["errors"].append(str(e))
            
            return thread_results
        
        # Run concurrent threads
        start_time = time.time()
        initial_memory = get_detailed_memory_info()
        
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            # Submit all worker threads
            future_to_thread = {
                executor.submit(worker_thread, thread_id): thread_id 
                for thread_id in range(num_threads)
            }
            
            # Collect results
            thread_results = []
            for future in as_completed(future_to_thread):
                thread_id = future_to_thread[future]
                try:
                    result = future.result()
                    thread_results.append(result)
                    print(f"  Thread {thread_id}: {result['successful_operations']}/{operations_per_thread} successful")
                except Exception as e:
                    print(f"  Thread {thread_id}: Failed with error: {e}")
                    thread_results.append({
                        "thread_id": thread_id,
                        "successful_operations": 0,
                        "failed_operations": operations_per_thread,
                        "errors": [str(e)]
                    })
        
        end_time = time.time()
        final_memory = get_detailed_memory_info()
        
        # Analyze concurrent results
        total_operations = num_threads * operations_per_thread
        total_successful = sum(r["successful_operations"] for r in thread_results)
        total_failed = sum(r["failed_operations"] for r in thread_results)
        success_rate = (total_successful / total_operations * 100) if total_operations > 0 else 0
        
        memory_change = final_memory['rss_mb'] - initial_memory['rss_mb']
        duration = end_time - start_time
        
        result = {
            "test_type": "concurrent_resource_usage",
            "num_threads": num_threads,
            "operations_per_thread": operations_per_thread,
            "total_operations": total_operations,
            "successful_operations": total_successful,
            "failed_operations": total_failed,
            "success_rate_percent": success_rate,
            "duration_seconds": duration,
            "memory_change_mb": memory_change,
            "thread_results": thread_results,
            "status": "PASS" if success_rate >= 80 else "FAIL"
        }
        
        print()
        print("CONCURRENT RESOURCE USAGE RESULTS:")
        print(f"  Total operations: {total_operations}")
        print(f"  Successful: {total_successful}")
        print(f"  Success rate: {success_rate:.2f}%")
        print(f"  Duration: {duration:.2f} seconds")
        print(f"  Memory change: {memory_change:+.2f} MB")
        print(f"  Test status: {result['status']}")
        
        return result
        
    except Exception as e:
        print(f"Concurrent resource usage test failed: {e}")
        return {"test_type": "concurrent_resource_usage", "status": "FAIL", "error": str(e)}

def main():
    """Run comprehensive resource management testing"""
    
    print("Starting Phase 11 Step 2: Resource Management Under Pressure")
    print("This test validates proper resource cleanup and concurrent usage")
    print()
    
    # Test 1: Resource creation/cleanup cycles
    cleanup_result = test_resource_creation_cleanup_cycles(cycles=15)
    print()
    
    # Test 2: Concurrent resource usage
    concurrent_result = test_concurrent_resource_usage(num_threads=8, operations_per_thread=15)
    
    # Save results
    os.makedirs('validation/phase11_stress_testing', exist_ok=True)
    
    final_report = {
        "test_timestamp": datetime.now().isoformat(),
        "test_type": "resource_management_under_pressure",
        "cleanup_test_result": cleanup_result,
        "concurrent_test_result": concurrent_result
    }
    
    with open('validation/phase11_stress_testing/step2_resource_management.json', 'w') as f:
        json.dump(final_report, f, indent=2)
    
    # Overall assessment
    cleanup_passed = cleanup_result.get("status") == "PASS"
    concurrent_passed = concurrent_result.get("status") == "PASS"
    overall_passed = cleanup_passed and concurrent_passed
    
    print()
    print("RESOURCE MANAGEMENT TEST SUMMARY:")
    print(f"  Resource cleanup test: {'✅ PASS' if cleanup_passed else '❌ FAIL'}")
    print(f"  Concurrent usage test: {'✅ PASS' if concurrent_passed else '❌ FAIL'}")
    print(f"  Overall result: {'✅ PASS' if overall_passed else '❌ FAIL'}")
    
    if overall_passed:
        print("✅ Excellent resource management under pressure")
        print("✅ No memory leaks detected")
        print("✅ Strong concurrent operation support")
    else:
        print("⚠️  Resource management issues detected")
    
    print()
    print("Results saved to: validation/phase11_stress_testing/step2_resource_management.json")
    
    return overall_passed

if __name__ == "__main__":
    main()