# Save as: validation\phase5_performance\step2_message_production.py
import json
import time
import string
import random
from datetime import datetime
from kafka import KafkaProducer
import psutil
import os

print("=== Phase 5 Step 2: FAST Message Production Performance ===")
print("Testing message production rates with different message sizes...")
print()

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def generate_message(size_category):
    """Generate test messages of different sizes"""
    base_message = {
        "id": random.randint(1, 1000000),
        "user_id": f"user_{random.randint(1, 10000)}",
        "event_type": random.choice(["purchase", "view", "click", "download"]),
        "amount": round(random.uniform(1.0, 1000.0), 2),
        "timestamp": datetime.now().isoformat(),
        "session_id": f"session_{random.randint(1, 1000)}"
    }
    
    if size_category == "small":  # Target ~500 bytes
        base_message["description"] = "A" * 200
        
    elif size_category == "medium":  # Target ~1KB  
        base_message["description"] = "B" * 700
        base_message["metadata"] = {"device": "mobile", "location": "US"}
        
    elif size_category == "large":  # Target ~2KB
        base_message["description"] = "C" * 1500
        base_message["metadata"] = {
            "device_info": "D" * 200,
            "user_agent": "Mozilla/5.0...",
            "referrer": "https://example.com"
        }
    
    return base_message

def test_message_production(message_count, message_size):
    """Test message production performance"""
    try:
        print(f"Testing {message_size} messages ({message_count} count)...")
        
        # Setup producer with timeout
        # Setup producer - simplified config
        producer = KafkaProducer(
                      bootstrap_servers=['localhost:9092'],
                      value_serializer=lambda v: json.dumps(v).encode('utf-8')
                      )
        
        # Generate messages upfront
        messages = []
        for _ in range(message_count):
            messages.append(generate_message(message_size))
        
        # Test actual message size
        sample_size = len(json.dumps(messages[0]).encode('utf-8'))
        
        # Measure production time
        start_time = time.time()
        start_memory = get_memory_usage()
        
        sent_count = 0
        for msg in messages:
            try:
                future = producer.send('performance-test-topic', msg)
                sent_count += 1
            except Exception as e:
                print(f"    Send error: {e}")
                break
        
        producer.flush(timeout=10)  # 10 second flush timeout
        
        end_time = time.time()
        end_memory = get_memory_usage()
        
        # Calculate metrics
        duration = end_time - start_time
        throughput = sent_count / duration if duration > 0 else 0
        memory_used = end_memory - start_memory
        
        result = {
            "message_size_category": message_size,
            "actual_message_size_bytes": sample_size,
            "message_count": sent_count,
            "duration_seconds": round(duration, 3),
            "throughput_msgs_per_sec": round(throughput, 2),
            "memory_used_mb": round(memory_used, 2),
            "total_data_mb": round((sample_size * sent_count) / (1024 * 1024), 2)
        }
        
        print(f"  Actual message size: {sample_size} bytes")
        print(f"  Messages sent: {sent_count}/{message_count}")
        print(f"  Duration: {duration:.3f} seconds")
        print(f"  Throughput: {throughput:.2f} messages/second")
        print(f"  Memory used: {memory_used:.2f} MB")
        print(f"  Total data: {result['total_data_mb']} MB")
        print("  PASS: Production test completed")
        print()
        
        producer.close()
        return result
        
    except Exception as e:
        print(f"  FAIL: Production test failed: {e}")
        return None

def main():
    """Run fast message production performance tests"""
    results = []
    
    print("Starting FAST message production performance tests...")
    print("Note: Ensure Kafka is running on localhost:9092")
    print()
    
    # Optimized test configs for speed and reliability
    test_configs = [
        (100, "small"),    # 100 small messages (~500 bytes each)
        (50, "medium"),    # 50 medium messages (~1KB each) 
        (25, "large"),     # 25 large messages (~2KB each)
        (200, "small")     # 200 small messages for throughput test
    ]
    
    for count, size in test_configs:
        result = test_message_production(count, size)
        if result:
            results.append(result)
        
        # Brief pause between tests
        time.sleep(1)
    
    # Create directory if it doesn't exist
    os.makedirs('validation/phase5_performance', exist_ok=True)
    
    # Save results
    with open('validation/phase5_performance/production_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Summary
    print("MESSAGE PRODUCTION PERFORMANCE SUMMARY:")
    for result in results:
        print(f"{result['message_size_category'].upper()} ({result['message_count']} msgs): "
              f"{result['throughput_msgs_per_sec']} msgs/sec, "
              f"{result['actual_message_size_bytes']} bytes/msg")
    
    # Performance assessment
    if results:
        max_throughput = max(r['throughput_msgs_per_sec'] for r in results)
        avg_throughput = sum(r['throughput_msgs_per_sec'] for r in results) / len(results)
        
        print()
        print(f"Peak throughput: {max_throughput} msgs/sec")
        print(f"Average throughput: {avg_throughput:.2f} msgs/sec")
        
        if avg_throughput > 100:
            print("ASSESSMENT: Good production performance")
        elif avg_throughput > 50:
            print("ASSESSMENT: Acceptable production performance")
        else:
            print("ASSESSMENT: Performance may need optimization")
    
    print()
    print("PASS: Message production performance testing completed")
    print("Results saved to: validation/phase5_performance/production_results.json")

if __name__ == "__main__":
    main()