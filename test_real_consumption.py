from dagster import asset, materialize
from dagster_kafka import KafkaResource, KafkaIOManager

# Create a test that forces Kafka consumption
def test_kafka_consumption():
    """Test direct Kafka consumption"""
    kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
    io_manager = KafkaIOManager(
        kafka_resource=kafka_resource,
        consumer_group_id="test-direct",
        max_messages=5
    )
    
    # Test the consumption directly
    messages = io_manager._consume_messages("test-api-events")
    print(f"📨 Consumed {len(messages)} messages")
    for i, msg in enumerate(messages):
        print(f"📩 Message {i+1}: {msg}")
    return messages

if __name__ == "__main__":
    print("🧪 Testing ACTUAL Kafka message consumption...")
    try:
        messages = test_kafka_consumption()
        if len(messages) > 0:
            print("✅ SUCCESS: Actually consuming Kafka messages!")
        else:
            print("⚠️  WARNING: No messages consumed")
    except Exception as e:
        print(f"❌ ERROR: {e}")
