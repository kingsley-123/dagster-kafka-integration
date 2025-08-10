#!/usr/bin/env python3
"""
Simple test to verify KafkaComponent works correctly.
"""

from dagster_kafka import KafkaComponent, KafkaConfig, ConsumerConfig, TopicConfig
import dagster as dg

def test_kafka_component():
    """Test that KafkaComponent can build definitions."""
    print("🧪 Testing KafkaComponent...")
    
    # Create component configuration
    kafka_config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        security_protocol="PLAINTEXT"
    )
    
    consumer_config = ConsumerConfig(
        consumer_group_id="test-consumer",
        max_messages=100,
        enable_dlq=True
    )
    
    topics = [
        TopicConfig(
            name="test-topic",
            format="json",
            asset_key="test_data"
        )
    ]
    
    # Create the component
    component = KafkaComponent(
        kafka_config=kafka_config,
        consumer_config=consumer_config,
        topics=topics
    )
    
    print("✅ Component created successfully!")
    
    # Test building definitions
    try:
        # Create a mock context
        context = type('MockContext', (), {})()
        defs = component.build_defs(context)
        
        print("✅ Definitions built successfully!")
        print(f"📊 Created {len(defs.assets)} assets")
        print(f"🔧 Created {len(defs.resources)} resources")
        
        # Check that assets were created
        assert len(defs.assets) == 1, f"Expected 1 asset, got {len(defs.assets)}"
        
        # Check that resources were created
        expected_resources = {"kafka", "test_data_io_manager"}
        actual_resources = set(defs.resources.keys())
        assert expected_resources.issubset(actual_resources), f"Missing resources. Expected {expected_resources}, got {actual_resources}"
        
        print("✅ All assertions passed!")
        return True
        
    except Exception as e:
        print(f"❌ Error building definitions: {e}")
        return False

def test_multiple_topics():
    """Test component with multiple topics and formats."""
    print("\n🧪 Testing multiple topics...")
    
    kafka_config = KafkaConfig(bootstrap_servers="localhost:9092")
    consumer_config = ConsumerConfig(consumer_group_id="multi-test")
    
    topics = [
        TopicConfig(name="json-topic", format="json"),
        TopicConfig(
            name="avro-topic", 
            format="avro", 
            schema_registry_url="http://localhost:8081"
        )
    ]
    
    component = KafkaComponent(
        kafka_config=kafka_config,
        consumer_config=consumer_config,
        topics=topics
    )
    
    try:
        context = type('MockContext', (), {})()
        defs = component.build_defs(context)
        
        print(f"✅ Multi-topic test passed! Created {len(defs.assets)} assets")
        assert len(defs.assets) == 2, f"Expected 2 assets, got {len(defs.assets)}"
        return True
        
    except Exception as e:
        print(f"❌ Multi-topic test failed: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Starting KafkaComponent Tests\n")
    
    # Run tests
    test1_passed = test_kafka_component()
    test2_passed = test_multiple_topics()
    
    # Summary
    print(f"\n📋 Test Summary:")
    print(f"   Single Topic Test: {'✅ PASSED' if test1_passed else '❌ FAILED'}")
    print(f"   Multiple Topics Test: {'✅ PASSED' if test2_passed else '❌ FAILED'}")
    
    if test1_passed and test2_passed:
        print(f"\n🎉 All tests passed! KafkaComponent is working correctly!")
    else:
        print(f"\n💥 Some tests failed. Check the errors above.")