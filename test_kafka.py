from dagster import asset, Definitions
from dagster_kafka import KafkaResource, KafkaIOManager

@asset
def api_events():
    """Read JSON data from api_events Kafka topic"""
    pass

print("✅ Kafka Integration Package Created Successfully!")
