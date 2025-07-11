from dagster import materialize, asset, Definitions
from dagster_kafka import KafkaResource, KafkaIOManager

@asset
def test_api_events():
    """This should read JSON messages from test-api-events topic"""
    pass  # Data loaded by KafkaIOManager

defs = Definitions(
    assets=[test_api_events],
    resources={
        "io_manager": KafkaIOManager(
            kafka_resource=KafkaResource(
                bootstrap_servers="localhost:9092"
            ),
            consumer_group_id="dagster-test",
            max_messages=10
        )
    }
)

if __name__ == "__main__":
    print("🧪 Testing Kafka Integration...")
    try:
        result = materialize([test_api_events], resources=defs.resources)
        print("✅ SUCCESS: Kafka integration works!")
        print(f"✅ Asset materialized: {result.success}")
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print("❌ Integration needs debugging")
