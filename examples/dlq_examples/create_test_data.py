#!/usr/bin/env python3
"""
Create simple test data for DLQ Inspector testing
"""

import json
import time
from kafka import KafkaProducer

def create_test_messages():
    """Create some basic test messages in a DLQ topic"""
    
    print("🔧 Creating test DLQ messages...")
    
    # Create producer
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: x.encode('utf-8') if isinstance(x, str) else x
    )
    
    # Test messages - mix of good and problematic
    test_messages = [
        # Good JSON message
        {
            'value': json.dumps({"user_id": "123", "action": "login", "timestamp": time.time()}),
            'headers': [('error_type', b'timeout_error'), ('retry_count', b'3')]
        },
        
        # Malformed JSON
        {
            'value': '{"incomplete": "json"',
            'headers': [('error_type', b'json_parse_error'), ('retry_count', b'1')]
        },
        
        # Another good message
        {
            'value': json.dumps({"order_id": "456", "status": "failed", "reason": "payment_declined"}),
            'headers': [('error_type', b'business_logic_error'), ('retry_count', b'2')]
        },
        
        # Non-JSON message
        {
            'value': 'this is not json at all',
            'headers': [('error_type', b'serialization_error'), ('retry_count', b'1')]
        },
        
        # Empty message
        {
            'value': '',
            'headers': [('error_type', b'empty_message'), ('retry_count', b'1')]
        }
    ]
    
    topic_name = 'test_dlq'
    
    # Send messages
    for i, msg in enumerate(test_messages):
        try:
            future = producer.send(
                topic_name,
                value=msg['value'],
                headers=msg['headers']
            )
            
            # Wait for send to complete
            result = future.get(timeout=10)
            print(f"✅ Sent message {i+1}: {result}")
            
        except Exception as e:
            print(f"❌ Failed to send message {i+1}: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
    
    # Flush and close
    producer.flush()
    producer.close()
    
    print(f"🎉 Created {len(test_messages)} test messages in topic '{topic_name}'")
    print("Ready to test DLQ Inspector!")

if __name__ == "__main__":
    create_test_messages()