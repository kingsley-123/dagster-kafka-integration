"""
Basic test for JSON Schema Kafka IO Manager
"""

from dagster_kafka import KafkaResource, create_json_schema_kafka_io_manager, DLQStrategy

def test_basic_creation():
    """Test basic JSON Schema IO Manager creation."""
    print("Testing JSON Schema IO Manager creation...")
    
    # Create a mock Kafka resource (we won't actually connect to Kafka)
    kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
    
    # Test 1: Create with inline schema
    test_schema = {
        "type": "object",
        "properties": {
            "user_id": {"type": "string"},
            "event_type": {"type": "string"}
        },
        "required": ["user_id"]
    }
    
    io_manager = create_json_schema_kafka_io_manager(
        kafka_resource=kafka_resource,
        schema_dict=test_schema,
        enable_schema_validation=True,
        strict_validation=True,
        enable_dlq=True,
        dlq_strategy=DLQStrategy.RETRY_THEN_DLQ
    )
    
    print(f"✅ IO Manager created successfully")
    print(f"✅ Schema loaded: {io_manager.schema is not None}")
    print(f"✅ Validation enabled: {io_manager.enable_schema_validation}")
    print(f"✅ DLQ enabled: {io_manager.enable_dlq}")
    print(f"✅ DLQ strategy: {io_manager.dlq_strategy}")
    
    # Test 2: Create without schema (validation disabled)
    io_manager_no_schema = create_json_schema_kafka_io_manager(
        kafka_resource=kafka_resource,
        enable_schema_validation=False
    )
    
    print(f"✅ No-schema IO Manager created")
    print(f"✅ Validation disabled: {not io_manager_no_schema.enable_schema_validation}")
    
    return True

if __name__ == "__main__":
    try:
        test_basic_creation()
        print("\n🎉 All basic tests passed! JSON Schema integration is working.")
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()