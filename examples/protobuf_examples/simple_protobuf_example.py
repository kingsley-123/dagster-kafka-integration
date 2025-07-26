"""
Simple Protobuf Kafka Integration Example

This example demonstrates basic usage of the ProtobufKafkaIOManager
with sample protobuf schemas.
"""

from dagster import asset, Definitions, materialize
from dagster_kafka import KafkaResource
from dagster_kafka.protobuf_io_manager import create_protobuf_kafka_io_manager


# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"  # Optional


@asset(io_manager_key="protobuf_kafka_io_manager")
def user_events():
    """
    Consume user event messages from Kafka topic using Protobuf format.
    This would typically contain User and UserEvent messages as defined in user.proto.
    """
    # Messages are automatically loaded from 'user_events' topic
    # The IO manager handles Protobuf deserialization
    pass


@asset(io_manager_key="protobuf_kafka_io_manager") 
def product_events():
    """
    Consume product event messages from Kafka topic using Protobuf format.
    This would contain Product and ProductEvent messages as defined in product.proto.
    """
    # Messages are automatically loaded from 'product_events' topic
    pass


@asset
def processed_user_data(user_events):
    """
    Process the consumed user events.
    
    Args:
        user_events: List of user event messages from Kafka
        
    Returns:
        Processed user data
    """
    print(f"Processing {len(user_events)} user events")
    
    # Example processing
    processed_data = []
    for event in user_events:
        processed_event = {
            "topic": event.get("topic"),
            "partition": event.get("partition"), 
            "offset": event.get("offset"),
            "timestamp": event.get("timestamp"),
            "key": event.get("key"),
            "raw_bytes_size": event.get("value_size", 0),
            "processed_at": "2024-01-01T00:00:00Z"
        }
        processed_data.append(processed_event)
    
    print(f"Processed {len(processed_data)} user events")
    return processed_data


@asset
def product_analytics(product_events):
    """
    Analyze product events for business insights.
    
    Args:
        product_events: List of product event messages from Kafka
        
    Returns:
        Product analytics summary
    """
    print(f"Analyzing {len(product_events)} product events")
    
    # Example analytics
    analytics = {
        "total_events": len(product_events),
        "event_sources": set(),
        "avg_message_size": 0,
        "topics_processed": set()
    }
    
    total_size = 0
    for event in product_events:
        analytics["topics_processed"].add(event.get("topic"))
        size = event.get("value_size", 0)
        total_size += size
    
    if analytics["total_events"] > 0:
        analytics["avg_message_size"] = total_size / analytics["total_events"]
    
    # Convert sets to lists for JSON serialization
    analytics["event_sources"] = list(analytics["event_sources"])
    analytics["topics_processed"] = list(analytics["topics_processed"])
    
    print(f"Analytics: {analytics}")
    return analytics


# Dagster definitions
defs = Definitions(
    assets=[user_events, product_events, processed_user_data, product_analytics],
    resources={
        "protobuf_kafka_io_manager": create_protobuf_kafka_io_manager(
            kafka_resource=KafkaResource(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS),
            schema_registry_url=SCHEMA_REGISTRY_URL,  # Optional
            consumer_group_id="dagster-protobuf-example"
        )
    }
)


if __name__ == "__main__":
    print("Running Simple Protobuf Kafka Example")
    print("=" * 50)
    
    # Note: This would require actual Kafka cluster with protobuf messages
    # For testing without Kafka, the assets will return empty lists
    
    try:
        result = materialize([user_events, product_events, processed_user_data, product_analytics])
        print(f"Materialization completed successfully!")
        print(f"Assets materialized: {len(result.asset_materializations)}")
        
    except Exception as e:
        print(f"Example requires running Kafka cluster")
        print(f"   Error: {e}")
        print(f"   This is expected when Kafka is not running")
        
    print("\n To run with real Kafka:")
    print("   1. Start Kafka: docker-compose up -d")
    print("   2. Produce some protobuf messages to topics:")
    print("      - user_events")  
    print("      - product_events")
    print("   3. Run this example again")