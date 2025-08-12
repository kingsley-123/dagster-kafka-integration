"""
Example usage of JSON Schema Kafka Integration
"""

from dagster import asset, Definitions
from dagster_kafka import KafkaResource, create_json_schema_kafka_io_manager, DLQStrategy

# Define a simple user events schema
user_events_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "user_id": {
            "type": "string",
            "description": "Unique user identifier"
        },
        "event_type": {
            "type": "string",
            "enum": ["login", "logout", "page_view", "click"],
            "description": "Type of user event"
        },
        "timestamp": {
            "type": "string",
            "format": "date-time",
            "description": "Event timestamp"
        },
        "metadata": {
            "type": "object",
            "properties": {
                "ip_address": {"type": "string"},
                "user_agent": {"type": "string"}
            }
        }
    },
    "required": ["user_id", "event_type", "timestamp"]
}

# Create assets using JSON Schema validation
@asset(io_manager_key="json_schema_io_manager")
def user_events():
    """Load user events from Kafka with JSON Schema validation."""
    # This asset will automatically validate incoming messages against the schema
    pass

@asset(io_manager_key="lenient_json_schema_io_manager") 
def analytics_events():
    """Load analytics events with lenient validation (warnings only)."""
    pass

# Create resources
def create_example_resources():
    """Create resources for the example."""
    
    # Basic Kafka resource
    kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
    
    # Strict JSON Schema IO Manager
    strict_json_manager = create_json_schema_kafka_io_manager(
        kafka_resource=kafka_resource,
        schema_dict=user_events_schema,
        consumer_group_id="strict-user-events",
        enable_schema_validation=True,
        strict_validation=True,  # Fail on validation errors
        enable_dlq=True,
        dlq_strategy=DLQStrategy.RETRY_THEN_DLQ,
        dlq_max_retries=3
    )
    
    # Lenient JSON Schema IO Manager 
    lenient_json_manager = create_json_schema_kafka_io_manager(
        kafka_resource=kafka_resource,
        schema_dict=user_events_schema,
        consumer_group_id="lenient-analytics",
        enable_schema_validation=True,
        strict_validation=False,  # Only warn on validation errors
        enable_dlq=True,
        dlq_strategy=DLQStrategy.IMMEDIATE
    )
    
    return {
        "kafka": kafka_resource,
        "json_schema_io_manager": strict_json_manager,
        "lenient_json_schema_io_manager": lenient_json_manager
    }

# Create Dagster definitions
defs = Definitions(
    assets=[user_events, analytics_events],
    resources=create_example_resources()
)

def demonstrate_schema_features():
    """Demonstrate JSON Schema features."""
    print("JSON Schema Kafka Integration - Feature Demo")
    print("=" * 50)
    
    kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
    
    # Test schema validation with different data
    io_manager = create_json_schema_kafka_io_manager(
        kafka_resource=kafka_resource,
        schema_dict=user_events_schema,
        enable_schema_validation=True,
        strict_validation=False  # For demo purposes
    )
    
    # Get schema info
    schema_info = io_manager._get_schema_and_validator()
    print(f"✅ Schema loaded: {schema_info[0] is not None}")
    print(f"✅ Validator created: {schema_info[1] is not None}")
    
    # Show schema details
    schema, validator = schema_info
    if schema:
        print(f"\n📋 Schema Properties:")
        for prop, details in schema.get("properties", {}).items():
            prop_type = details.get("type", "unknown")
            required = "✓" if prop in schema.get("required", []) else " "
            print(f"  [{required}] {prop}: {prop_type}")
    
    print(f"\n🔧 Configuration:")
    print(f"  Validation enabled: {io_manager.enable_schema_validation}")
    print(f"  Strict validation: {io_manager.strict_validation}")
    print(f"  DLQ enabled: {io_manager.enable_dlq}")
    print(f"  DLQ strategy: {io_manager.dlq_strategy}")
    
    print(f"\n🎯 Integration Features:")
    print(f"  ✅ JSON Schema validation")
    print(f"  ✅ Dead Letter Queue (DLQ) support")
    print(f"  ✅ Circuit breaker patterns")
    print(f"  ✅ Retry mechanisms")
    print(f"  ✅ Configurable validation modes")
    print(f"  ✅ Enterprise security support")

if __name__ == "__main__":
    demonstrate_schema_features()
    print(f"\n🚀 JSON Schema Kafka integration is ready!")
    print(f"💡 Next: Run your Dagster pipeline with: dagster dev")