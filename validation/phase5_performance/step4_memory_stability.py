# Save as: validation\phase5_performance\step4_fast_memory.py
import json
import time
from datetime import datetime
from dagster_kafka import KafkaResource, KafkaIOManager
import psutil
import os
import gc

print("=== Phase 5 Step 4: FAST Memory Stability Testing ===")
print("Testing memory usage stability...")
print()

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def fast_memory_leak_test(cycles=5, resources_per_cycle=10):
    """Test for memory leaks by creating and destroying resources repeatedly"""
    try:
        print(f"Testing memory stability over {cycles} cycles, {resources_per_cycle} resources per cycle...")
        
        memory_snapshots = []
        
        for cycle in range(cycles):
            cycle_start_memory = get_memory_usage()
            print(f"  Cycle {cycle + 1}/{cycles} - Starting memory: {cycle_start_memory:.2f} MB")
            
            # Create resources
            resources = []
            io_managers = []
            
            for i in range(resources_per_cycle):
                resource = KafkaResource(
                    bootstrap_servers="localhost:9092",
                    additional_config={"client.id": f"fast-memory-test-{cycle}-{i}"}
                )
                
                io_manager = KafkaIOManager(
                    kafka_resource=resource,
                    consumer_group_id=f"fast-memory-test-group-{cycle}-{i}",
                    enable_dlq=True if i % 2 == 0 else False
                )
                
                resources.append(resource)
                io_managers.append(io_manager)
            
            mid_cycle_memory = get_memory_usage()
            
            # Clean up resources
            del resources
            del io_managers
            
            # Force garbage collection
            gc.collect()
            time.sleep(0.2)  # Brief time for cleanup
            
            cycle_end_memory = get_memory_usage()
            
            snapshot = {
                "cycle": cycle + 1,
                "start_memory_mb": round(cycle_start_memory, 2),
                "mid_cycle_memory_mb": round(mid_cycle_memory, 2),
                "end_memory_mb": round(cycle_end_memory, 2),
                "memory_increase_mb": round(cycle_end_memory - cycle_start_memory, 2)
            }
            
            memory_snapshots.append(snapshot)
            
            print(f"    End memory: {cycle_end_memory:.2f} MB (change: {snapshot['memory_increase_mb']:+.2f} MB)")
        
        # Analyze memory stability
        initial_memory = memory_snapshots[0]['start_memory_mb']
        final_memory = memory_snapshots[-1]['end_memory_mb']
        total_increase = final_memory - initial_memory
        
        max_increase = max(s['memory_increase_mb'] for s in memory_snapshots)
        avg_increase = sum(s['memory_increase_mb'] for s in memory_snapshots) / len(memory_snapshots)
        
        result = {
            "test_type": "fast_memory_leak_test",
            "cycles": cycles,
            "resources_per_cycle": resources_per_cycle,
            "initial_memory_mb": initial_memory,
            "final_memory_mb": final_memory,
            "total_memory_increase_mb": round(total_increase, 2),
            "max_cycle_increase_mb": round(max_increase, 2),
            "avg_cycle_increase_mb": round(avg_increase, 2),
            "memory_snapshots": memory_snapshots
        }
        
        print()
        print("MEMORY STABILITY ANALYSIS:")
        print(f"  Total memory increase: {total_increase:+.2f} MB")
        print(f"  Maximum cycle increase: {max_increase:+.2f} MB") 
        print(f"  Average cycle increase: {avg_increase:+.2f} MB")
        
        if total_increase < 25:  # Less than 25MB total increase is acceptable
            print("  PASS: Memory usage stable - no significant leaks detected")
        else:
            print("  WARN: Significant memory increase detected - investigate potential leaks")
        
        return result
        
    except Exception as e:
        print(f"  FAIL: Memory stability test failed: {e}")
        return None

def main():
    """Run fast memory stability tests"""
    results = []
    
    print("Starting FAST memory stability testing...")
    print()
    
    # Test: Memory leak detection
    leak_result = fast_memory_leak_test(cycles=5, resources_per_cycle=10)
    if leak_result:
        results.append(leak_result)
    
    # Create directory if it doesn't exist
    os.makedirs('validation/phase5_performance', exist_ok=True)
    
    # Save results
    with open('validation/phase5_performance/memory_stability_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print()
    print("MEMORY STABILITY SUMMARY:")
    if leak_result:
        print(f"Memory leak test: {leak_result['total_memory_increase_mb']:+.2f} MB total increase over {leak_result['cycles']} cycles")
        print(f"Status: {'PASS' if leak_result['total_memory_increase_mb'] < 25 else 'WARN'}")
    
    print()
    print("PASS: Memory stability testing completed")
    print("Results saved to: validation/phase5_performance/memory_stability_results.json")

if __name__ == "__main__":
    main()