#!/usr/bin/env python3
"""
Create stress test data for DLQ Inspector
Tests larger messages, encoding issues, and edge cases
"""

import json
import time
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def create_stress_test_data():
    """Create more challenging test data"""
    
    print("🔧 Creating stress test DLQ messages...")
    
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: x.encode('utf-8') if isinstance(x, str) else x,
        max_request_size=5242880  # 5MB for large messages
    )
    
    # More challenging test scenarios
    test_messages = [
        # Large JSON message (1MB+)
        {
            'value': json.dumps({
                "user_id": fake.uuid4(),
                "large_data": fake.text() * 10000,  # Very large text
                "array_data": [fake.sentence() for _ in range(1000)],  # Large array
                "nested": {"deep": {"very": {"nested": {"data": fake.text() * 100}}}}
            }),
            'headers': [('error_type', b'message_too_large'), ('retry_count', b'1'), ('original_topic', b'user_events')]
        },
        
        # Binary data that's not UTF-8
        {
            'value': bytes(range(256)),  # All possible byte values
            'headers': [('error_type', b'encoding_error'), ('retry_count', b'2'), ('original_topic', b'binary_data')]
        },
        
        # Message with corrupted JSON containing special characters
        {
            'value': '{"emoji": "🚀💥", "special": "café naïve résumé", "broken": json}',
            'headers': [('error_type', b'json_corruption'), ('retry_count', b'4'), ('original_topic', b'international')]
        },
        
        # Very long line without breaks (stress parsing)
        {
            'value': fake.sentence() * 1000,  # One very long sentence
            'headers': [('error_type', b'format_error'), ('retry_count', b'1'), ('original_topic', b'logs')]
        },
        
        # Null bytes in content
        {
            'value': f"Valid start\x00\x01\x02null bytes here\x00end",
            'headers': [('error_type', b'null_bytes'), ('retry_count', b'3'), ('original_topic', b'binary_stream')]
        },
        
        # Headers with special characters
        {
            'value': json.dumps({"normal": "content"}),
            'headers': [
                ('error_type', 'special_chars_éß'.encode('utf-8')), 
                ('retry_count', b'2'),
                ('original_topic', 'topic_with_émojis_🚀'.encode('utf-8'))
            ]
        },
        
        # Maximum retry count
        {
            'value': '{"message": "persistent failure"}',
            'headers': [('error_type', b'persistent_failure'), ('retry_count', b'10'), ('original_topic', b'critical_service')]
        },
        
        # Empty headers
        {
            'value': json.dumps({"id": 123, "status": "failed"}),
            'headers': []  # No headers at all
        }
    ]
    
    topic_name = 'stress_test_dlq'
    
    # Send messages
    for i, msg in enumerate(test_messages):
        try:
            # Handle binary values
            if isinstance(msg['value'], bytes):
                producer_binary = KafkaProducer(bootstrap_servers=['localhost:9092'])
                future = producer_binary.send(
                    topic_name,
                    value=msg['value'],
                    headers=msg['headers']
                )
                producer_binary.flush()
                producer_binary.close()
            else:
                future = producer.send(
                    topic_name,
                    value=msg['value'],
                    headers=msg['headers']
                )
            
            result = future.get(timeout=10)
            print(f"✅ Sent stress test message {i+1}: {len(str(msg['value']))} chars")
            
        except Exception as e:
            print(f"❌ Failed to send message {i+1}: {e}")
    
    producer.flush()
    producer.close()
    
    print(f"🎉 Created {len(test_messages)} stress test messages in topic '{topic_name}'")
    print("Ready for stress testing!")

if __name__ == "__main__":
    create_stress_test_data()