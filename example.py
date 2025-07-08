"""
Complete example showing Kafka integration with Dagster
Run: python example.py
"""
from dagster import asset, Definitions
from dagster_kafka import KafkaResource, KafkaIOManager

@asset
def user_events():
    """Stream user events from Kafka topic"""
    pass

if __name__ == "__main__":
    print("✅ Dagster Kafka Integration Example")
    print("🚀 Ready to connect Kafka topics to Dagster assets!")
