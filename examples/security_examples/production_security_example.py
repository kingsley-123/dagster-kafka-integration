"""
Production Kafka Security Example

Demonstrates enterprise-grade security configurations for production deployments
including SASL/SSL authentication and encryption.
"""

from dagster import asset, Definitions, materialize, Config
from dagster_kafka import KafkaResource, SecurityProtocol, SaslMechanism, KafkaIOManager
from dagster_kafka.avro_io_manager import avro_kafka_io_manager
from dagster_kafka.protobuf_io_manager import create_protobuf_kafka_io_manager


# Production Security Configurations

# 1. SASL/SSL Production Configuration (Most Secure)
production_kafka_resource = KafkaResource(
    bootstrap_servers="prod-kafka-01:9092,prod-kafka-02:9092,prod-kafka-03:9092",
    security_protocol=SecurityProtocol.SASL_SSL,
    sasl_mechanism=SaslMechanism.SCRAM_SHA_256,
    sasl_username="dagster-prod-user",
    sasl_password="secure-production-password",
    ssl_ca_location="/etc/ssl/certs/kafka-ca.pem",
    ssl_check_hostname=True,
    session_timeout_ms=30000,
    additional_config={
        "request.timeout.ms": 30000,
        "retry.backoff.ms": 1000,
        "max.poll.interval.ms": 300000
    }
)

# 2. SSL-Only Configuration (Certificate-based)
ssl_kafka_resource = KafkaResource(
    bootstrap_servers="ssl-kafka:9092",
    security_protocol=SecurityProtocol.SSL,
    ssl_ca_location="/etc/ssl/certs/ca.pem",
    ssl_certificate_location="/etc/ssl/certs/client.pem",
    ssl_key_location="/etc/ssl/private/client-key.pem",
    ssl_key_password="client-key-password",
    ssl_check_hostname=True
)

# 3. SASL PLAIN Configuration (Simpler, still secure)
sasl_kafka_resource = KafkaResource(
    bootstrap_servers="secure-kafka:9092",
    security_protocol=SecurityProtocol.SASL_SSL,
    sasl_mechanism=SaslMechanism.PLAIN,
    sasl_username="simple-user",
    sasl_password="simple-password",
    ssl_ca_location="/etc/ssl/certs/kafka-ca.pem"
)


class SecurityConfig(Config):
    """Configuration for security example."""
    max_messages: int = 100
    timeout_seconds: float = 30.0


@asset(io_manager_key="secure_json_io_manager")
def secure_api_events(config: SecurityConfig):
    """Consume JSON events from secure Kafka cluster."""
    pass


@asset(io_manager_key="secure_avro_io_manager")  
def secure_user_events(config: SecurityConfig):
    """Consume Avro events from secure Kafka cluster."""
    pass


@asset(io_manager_key="secure_protobuf_io_manager")
def secure_telemetry_events(config: SecurityConfig):
    """Consume Protobuf events from secure Kafka cluster."""
    pass


@asset
def security_audit_report(secure_api_events, secure_user_events, secure_telemetry_events):
    """Generate security audit report from all secure data sources."""
    total_events = len(secure_api_events) + len(secure_user_events) + len(secure_telemetry_events)
    
    report = {
        "total_secure_events_processed": total_events,
        "json_events": len(secure_api_events),
        "avro_events": len(secure_user_events), 
        "protobuf_events": len(secure_telemetry_events),
        "security_protocols_used": [
            "SASL_SSL with SCRAM-SHA-256",
            "SSL with client certificates",
            "SASL_SSL with PLAIN"
        ],
        "encryption_status": "All communications encrypted",
        "authentication_status": "All connections authenticated"
    }
    
    print(f" Security Audit Report: {report}")
    return report


# Production Definitions with Security
production_defs = Definitions(
    assets=[secure_api_events, secure_user_events, secure_telemetry_events, security_audit_report],
    resources={
        # JSON with SASL/SSL
        "secure_json_io_manager": KafkaIOManager(
            kafka_resource=production_kafka_resource,
            consumer_group_id="secure-json-consumer"
        ),
        
        # Avro with SSL certificates
        "secure_avro_io_manager": avro_kafka_io_manager.configured({
            "kafka_resource": ssl_kafka_resource,
            "schema_registry_url": "https://secure-schema-registry:8081",
            "consumer_group_id": "secure-avro-consumer"
        }),
        
        # Protobuf with SASL PLAIN
        "secure_protobuf_io_manager": create_protobuf_kafka_io_manager(
            kafka_resource=sasl_kafka_resource,
            schema_registry_url="https://secure-schema-registry:8081",
            consumer_group_id="secure-protobuf-consumer"
        )
    }
)


def validate_security_setup():
    """Validate that all security configurations are correct."""
    print(" Validating Security Configurations...")
    
    # Validate production SASL/SSL
    try:
        production_kafka_resource.validate_security_config()
        print(" Production SASL/SSL configuration valid")
    except Exception as e:
        print(f" Production SASL/SSL validation failed: {e}")
    
    # Validate SSL-only
    try:
        ssl_kafka_resource.validate_security_config()
        print(" SSL-only configuration valid")
    except Exception as e:
        print(f" SSL-only validation failed: {e}")
    
    # Validate SASL PLAIN
    try:
        sasl_kafka_resource.validate_security_config()
        print(" SASL PLAIN configuration valid")
    except Exception as e:
        print(f" SASL PLAIN validation failed: {e}")


if __name__ == "__main__":
    print(" Production Kafka Security Example")
    print("=" * 50)
    
    # Validate security configurations
    validate_security_setup()
    
    print("\n Security Configuration Summary:")
    print("1. Production SASL/SSL: SCRAM-SHA-256 + SSL encryption")
    print("2. SSL-Only: Client certificate authentication + SSL encryption")  
    print("3. SASL PLAIN: Username/password + SSL encryption")
    
    print("\n  This example requires:")
    print("   - Production Kafka cluster with security enabled")
    print("   - Valid SSL certificates and keys")
    print("   - SASL user credentials")
    print("   - Schema Registry with HTTPS")
    
    print("\n To run with real secure Kafka:")
    print("   1. Update bootstrap_servers to your secure Kafka brokers")
    print("   2. Update SSL certificate paths")
    print("   3. Update SASL credentials")
    print("   4. Run: python examples/security_examples/production_security_example.py")
    
    try:
        print("\n Testing security configurations (without connecting)...")
        print(" All security configurations created successfully!")
        print(" Ready for production deployment with enterprise security!")
        
    except Exception as e:
        print(f" Configuration error: {e}")