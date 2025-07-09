from dagster import asset, materialize, AssetIn
from dagster_kafka import KafkaResource, KafkaIOManager
import uuid

@asset
def raw_kafka_data():
    """This will be the Kafka topic data"""
    return []

@asset
def processed_events(raw_kafka_data):
    """Process the Kafka messages"""
    print(f"📊 Processing {len(raw_kafka_data)} messages from Kafka")
    for i, msg in enumerate(raw_kafka_data):
        print(f"📩 Message {i+1}: {msg}")
    return len(raw_kafka_data)

if __name__ == "__main__":
    print("🧪 Testing Kafka consumption with FRESH consumer group...")
    
    # Use unique consumer group to avoid offset issues
    unique_group = f"dagster-test-{str(uuid.uuid4())[:8]}"
    print(f"🆔 Using consumer group: {unique_group}")
    
    resources = {
        "io_manager": KafkaIOManager(
            kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
            consumer_group_id=unique_group,
            max_messages=10
        )
    }
    
    try:
        result = materialize([raw_kafka_data, processed_events], resources=resources)
        print("✅ SUCCESS: Kafka integration reads messages correctly!")
    except Exception as e:
        print(f"❌ ERROR: {e}")
