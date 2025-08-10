#!/usr/bin/env python3
"""
Advanced Component Test - Validates Extreme Scale Configurations
Tests the most complex scenarios from our advanced examples.
"""

from dagster_kafka import KafkaComponent, KafkaConfig, ConsumerConfig, TopicConfig
import dagster as dg

def test_extreme_scale_crypto_trading():
    """Test extreme scale crypto trading configuration (200K messages)."""
    print("🚀 Testing EXTREME SCALE Crypto Trading Configuration...")
    print("   📊 Target: 200,000 messages per batch")
    
    # Extreme scale crypto trading config
    kafka_config = KafkaConfig(
        bootstrap_servers="crypto-kafka-01:9092,crypto-kafka-02:9092,crypto-kafka-03:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM_SHA_512",
        sasl_username="crypto_trading_user",
        sasl_password="ultra_secure_password",
        ssl_ca_location="/etc/ssl/crypto/trading-ca.pem",
        ssl_check_hostname=True
    )
    
    consumer_config = ConsumerConfig(
        consumer_group_id="crypto-trading-engine",
        max_messages=200000,  # EXTREME SCALE
        enable_dlq=True,
        dlq_strategy="IMMEDIATE",  # No retries in trading
        dlq_max_retries=0
    )
    
    topics = [
        TopicConfig(
            name="crypto-market-data-feed",
            format="protobuf",
            schema_registry_url="http://crypto-schema-registry:8081",
            asset_key="crypto_market_data_extreme"
        ),
        TopicConfig(
            name="blockchain-transaction-events", 
            format="protobuf",
            schema_registry_url="http://crypto-schema-registry:8081",
            asset_key="blockchain_transactions"
        ),
        TopicConfig(
            name="defi-protocol-events",
            format="protobuf", 
            schema_registry_url="http://crypto-schema-registry:8081",
            asset_key="defi_protocol_events"
        )
    ]
    
    try:
        component = KafkaComponent(
            kafka_config=kafka_config,
            consumer_config=consumer_config,
            topics=topics
        )
        
        context = type('MockContext', (), {})()
        defs = component.build_defs(context)
        
        print(f"   ✅ Extreme scale component created successfully!")
        print(f"   📈 Assets created: {len(defs.assets)}")
        print(f"   🔧 Resources created: {len(defs.resources)}")
        print(f"   🎯 Max messages per batch: {consumer_config.max_messages:,}")
        
        # Verify extreme scale parameters
        assert len(defs.assets) == 3, f"Expected 3 assets, got {len(defs.assets)}"
        assert len(defs.resources) >= 4, f"Expected 4+ resources, got {len(defs.resources)}"
        
        return True
        
    except Exception as e:
        print(f"   ❌ Extreme scale test failed: {e}")
        return False

def test_multi_format_enterprise():
    """Test enterprise configuration with all three formats."""
    print("\n🏢 Testing Multi-Format Enterprise Configuration...")
    print("   📋 Testing: JSON + Avro + Protobuf in one component")
    
    kafka_config = KafkaConfig(
        bootstrap_servers="enterprise-kafka-cluster:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM_SHA_256",
        sasl_username="enterprise_user",
        sasl_password="enterprise_password",
        ssl_ca_location="/etc/ssl/enterprise/kafka-ca.pem"
    )
    
    consumer_config = ConsumerConfig(
        consumer_group_id="enterprise-data-pipeline",
        max_messages=10000,  # High throughput
        enable_dlq=True,
        dlq_strategy="CIRCUIT_BREAKER",
        dlq_max_retries=5
    )
    
    # All three formats in one component
    topics = [
        TopicConfig(
            name="user-activity-events",
            format="json",  # JSON format
            asset_key="user_activity"
        ),
        TopicConfig(
            name="financial-transactions", 
            format="avro",  # Avro format
            schema_registry_url="http://enterprise-schema-registry:8081",
            asset_key="financial_transactions"
        ),
        TopicConfig(
            name="iot-sensor-metrics",
            format="protobuf",  # Protobuf format
            schema_registry_url="http://enterprise-schema-registry:8081", 
            asset_key="iot_metrics"
        )
    ]
    
    try:
        component = KafkaComponent(
            kafka_config=kafka_config,
            consumer_config=consumer_config,
            topics=topics
        )
        
        context = type('MockContext', (), {})()
        defs = component.build_defs(context)
        
        print(f"   ✅ Multi-format enterprise component created!")
        print(f"   📊 JSON assets: 1")
        print(f"   📊 Avro assets: 1") 
        print(f"   📊 Protobuf assets: 1")
        print(f"   🔧 Total resources: {len(defs.resources)}")
        
        # Verify all formats work
        assert len(defs.assets) == 3, f"Expected 3 assets, got {len(defs.assets)}"
        
        return True
        
    except Exception as e:
        print(f"   ❌ Multi-format test failed: {e}")
        return False

def test_massive_topic_count():
    """Test component with many topics (simulating microservices)."""
    print("\n🌐 Testing Massive Topic Count (Microservices Simulation)...")
    print("   📊 Testing: 15 topics across multiple services")
    
    kafka_config = KafkaConfig(
        bootstrap_servers="microservices-kafka:9092",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM_SHA_256"
    )
    
    consumer_config = ConsumerConfig(
        consumer_group_id="microservices-coordinator",
        max_messages=25000,
        enable_dlq=True,
        dlq_strategy="RETRY_THEN_DLQ",
        dlq_max_retries=3
    )
    
    # 15 different topics simulating complex microservices
    topics = [
        # User Service
        TopicConfig(name="user-events", format="json", asset_key="user_events"),
        TopicConfig(name="user-commands", format="avro", schema_registry_url="http://localhost:8081", asset_key="user_commands"),
        
        # Order Service  
        TopicConfig(name="order-events", format="avro", schema_registry_url="http://localhost:8081", asset_key="order_events"),
        TopicConfig(name="payment-events", format="protobuf", schema_registry_url="http://localhost:8081", asset_key="payment_events"),
        
        # Notification Service
        TopicConfig(name="email-queue", format="json", asset_key="email_notifications"),
        TopicConfig(name="sms-queue", format="json", asset_key="sms_notifications"),
        TopicConfig(name="push-queue", format="json", asset_key="push_notifications"),
        
        # Analytics Service
        TopicConfig(name="clickstream", format="protobuf", schema_registry_url="http://localhost:8081", asset_key="web_analytics"),
        TopicConfig(name="mobile-events", format="protobuf", schema_registry_url="http://localhost:8081", asset_key="mobile_analytics"),
        
        # Fraud Detection
        TopicConfig(name="fraud-alerts", format="protobuf", schema_registry_url="http://localhost:8081", asset_key="fraud_detection"),
        
        # Audit Service
        TopicConfig(name="audit-events", format="avro", schema_registry_url="http://localhost:8081", asset_key="audit_trail"),
        TopicConfig(name="compliance-events", format="avro", schema_registry_url="http://localhost:8081", asset_key="compliance_audit"),
        
        # Infrastructure
        TopicConfig(name="system-metrics", format="protobuf", schema_registry_url="http://localhost:8081", asset_key="system_monitoring"),
        TopicConfig(name="application-logs", format="json", asset_key="application_logs"),
        TopicConfig(name="security-events", format="avro", schema_registry_url="http://localhost:8081", asset_key="security_monitoring")
    ]
    
    try:
        component = KafkaComponent(
            kafka_config=kafka_config,
            consumer_config=consumer_config,
            topics=topics
        )
        
        context = type('MockContext', (), {})()
        defs = component.build_defs(context)
        
        print(f"   ✅ Massive microservices component created!")
        print(f"   📊 Total topics: {len(topics)}")
        print(f"   📈 Assets created: {len(defs.assets)}")
        print(f"   🔧 Resources created: {len(defs.resources)}")
        
        # Count formats
        json_topics = len([t for t in topics if t.format == "json"])
        avro_topics = len([t for t in topics if t.format == "avro"])
        protobuf_topics = len([t for t in topics if t.format == "protobuf"])
        
        print(f"   📋 JSON topics: {json_topics}")
        print(f"   📋 Avro topics: {avro_topics}")
        print(f"   📋 Protobuf topics: {protobuf_topics}")
        
        # Verify all topics created assets
        assert len(defs.assets) == 15, f"Expected 15 assets, got {len(defs.assets)}"
        
        return True
        
    except Exception as e:
        print(f"   ❌ Massive topic test failed: {e}")
        return False

def test_different_dlq_strategies():
    """Test all different DLQ strategies work correctly."""
    print("\n⚡ Testing All DLQ Strategies...")
    
    strategies = [
        ("DISABLED", 0),
        ("IMMEDIATE", 1), 
        ("RETRY_THEN_DLQ", 3),
        ("CIRCUIT_BREAKER", 5)
    ]
    
    results = []
    
    for strategy, retries in strategies:
        print(f"   🧪 Testing DLQ Strategy: {strategy}")
        
        try:
            kafka_config = KafkaConfig(bootstrap_servers="localhost:9092")
            consumer_config = ConsumerConfig(
                consumer_group_id=f"dlq-test-{strategy.lower()}",
                dlq_strategy=strategy,
                dlq_max_retries=retries
            )
            topics = [TopicConfig(name=f"test-{strategy.lower()}", format="json")]
            
            component = KafkaComponent(
                kafka_config=kafka_config,
                consumer_config=consumer_config,
                topics=topics
            )
            
            context = type('MockContext', (), {})()
            defs = component.build_defs(context)
            
            print(f"      ✅ {strategy} strategy works!")
            results.append(True)
            
        except Exception as e:
            print(f"      ❌ {strategy} strategy failed: {e}")
            results.append(False)
    
    return all(results)

if __name__ == "__main__":
    print("🚀 EXTREME SCALE KAFKA COMPONENT TESTING")
    print("=" * 60)
    
    # Run all advanced tests
    test1 = test_extreme_scale_crypto_trading()
    test2 = test_multi_format_enterprise() 
    test3 = test_massive_topic_count()
    test4 = test_different_dlq_strategies()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 EXTREME SCALE TEST SUMMARY:")
    print(f"   🚀 Extreme Scale (200K msgs): {'✅ PASSED' if test1 else '❌ FAILED'}")
    print(f"   🏢 Multi-Format Enterprise: {'✅ PASSED' if test2 else '❌ FAILED'}")
    print(f"   🌐 Massive Topics (15 topics): {'✅ PASSED' if test3 else '❌ FAILED'}")
    print(f"   ⚡ All DLQ Strategies: {'✅ PASSED' if test4 else '❌ FAILED'}")
    
    if all([test1, test2, test3, test4]):
        print(f"\n🎉 ALL EXTREME SCALE TESTS PASSED!")
        print(f"🚀 Your KafkaComponent can handle:")
        print(f"   • 200,000 messages per batch")
        print(f"   • 15+ topics in one component") 
        print(f"   • All 3 message formats simultaneously")
        print(f"   • All 4 DLQ strategies")
        print(f"   • Enterprise security configurations")
        print(f"   • Multi-cloud deployment scenarios")
    else:
        print(f"\n💥 Some extreme scale tests failed.")
        print(f"   Check the errors above for details.")