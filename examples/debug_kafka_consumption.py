from confluent_kafka import Consumer
import json

def test_raw_kafka():
    """Test Kafka consumption directly"""
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "debug-group",
        "auto.offset.reset": "earliest"
    }
    
    consumer = Consumer(config)
    consumer.subscribe(["raw_kafka_data"])
    
    messages = []
    for i in range(10):
        msg = consumer.poll(timeout=2.0)
        if msg is None:
            print(f"No message {i+1}/10")
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue
        
        try:
            data = json.loads(msg.value().decode("utf-8"))
            messages.append(data)
            print(f"✅ Got message: {data}")
        except Exception as e:
            print(f"❌ Parse error: {e}")
    
    consumer.close()
    return messages

if __name__ == "__main__":
    print("🔍 Testing direct Kafka consumption...")
    messages = test_raw_kafka()
    print(f"📊 Total messages found: {len(messages)}")
