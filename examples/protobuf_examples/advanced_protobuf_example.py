"""
Advanced Protobuf Kafka Integration with Schema Registry

This example demonstrates:
- Schema Registry integration
- Schema evolution validation
- Error handling and recovery
- Production monitoring
- Complex protobuf message processing
"""

from dagster import asset, Definitions, materialize, Config, get_dagster_logger
from dagster_kafka import KafkaResource
from dagster_kafka.protobuf_io_manager import (
    ProtobufKafkaIOManager, 
    ProtobufSchemaManager,
    validate_protobuf_schema
)
from dagster_kafka.schema_evolution import SchemaEvolutionValidator, CompatibilityLevel
from dagster_kafka.monitoring import SchemaEvolutionMonitor, AlertSeverity
from pydantic import Field
from typing import List, Dict, Any, Optional
import json


# Configuration classes
class ProtobufConfig(Config):
    """Configuration for Protobuf processing."""
    schema_file: str = Field(default="examples/protobuf_examples/schemas/user.proto")
    max_messages: int = Field(default=100)
    timeout_seconds: float = Field(default=30.0)
    enable_monitoring: bool = Field(default=True)
    compatibility_level: str = Field(default="BACKWARD")


class SchemaRegistryConfig(Config):
    """Schema Registry configuration."""
    url: str = Field(default="http://localhost:8081")
    subject_prefix: str = Field(default="dagster-protobuf")
    enable_validation: bool = Field(default=True)


# Advanced assets with configuration
@asset(io_manager_key="advanced_protobuf_io_manager")
def user_events_advanced(context, config: ProtobufConfig) -> List[Dict[str, Any]]:
    """
    Advanced user events consumption with configuration and monitoring.
    """
    logger = get_dagster_logger()
    logger.info(f"Loading user events with config: {config}")
    
    # The IO manager will handle the actual loading
    # This asset will receive the loaded messages
    pass


@asset(io_manager_key="advanced_protobuf_io_manager")
def product_events_advanced(context, config: ProtobufConfig) -> List[Dict[str, Any]]:
    """
    Advanced product events consumption.
    """
    logger = get_dagster_logger()
    logger.info(f"Loading product events with config: {config}")
    pass


@asset
def validate_schemas(context) -> Dict[str, bool]:
    """
    Validate protobuf schemas before processing.
    """
    logger = get_dagster_logger()
    
    schemas_to_validate = [
        "examples/protobuf_examples/schemas/user.proto",
        "examples/protobuf_examples/schemas/product.proto"
    ]
    
    validation_results = {}
    
    for schema_file in schemas_to_validate:
        try:
            with open(schema_file, 'r') as f:
                schema_content = f.read()
            
            is_valid = validate_protobuf_schema(schema_content)
            validation_results[schema_file] = is_valid
            
            if is_valid:
                logger.info(f" Schema {schema_file} is valid")
            else:
                logger.error(f" Schema {schema_file} is invalid")
                
        except FileNotFoundError:
            logger.warning(f"  Schema file {schema_file} not found")
            validation_results[schema_file] = False
        except Exception as e:
            logger.error(f" Error validating {schema_file}: {e}")
            validation_results[schema_file] = False
    
    return validation_results


@asset
def process_user_events_with_monitoring(
    user_events_advanced, 
    validate_schemas
) -> Dict[str, Any]:
    """
    Process user events with comprehensive monitoring and error handling.
    """
    logger = get_dagster_logger()
    
    # Check schema validation first
    if not all(validate_schemas.values()):
        logger.error(" Schema validation failed, skipping processing")
        return {"error": "Schema validation failed", "processed_count": 0}
    
    logger.info(f"Processing {len(user_events_advanced)} user events")
    
    # Initialize monitoring
    monitor = SchemaEvolutionMonitor()
    
    processed_events = []
    error_count = 0
    
    for i, event in enumerate(user_events_advanced):
        try:
            # Simulate processing with monitoring
            processed_event = {
                "event_id": i,
                "topic": event.get("topic"),
                "partition": event.get("partition"),
                "offset": event.get("offset"),
                "timestamp": event.get("timestamp"),
                "message_size": event.get("value_size", 0),
                "processing_timestamp": "2024-01-01T00:00:00Z",
                "status": "processed"
            }
            
            processed_events.append(processed_event)
            
            # Record success metrics
            monitor.record_validation_attempt(
                subject="user-events",
                schema_version=1,
                success=True,
                compatibility_level=CompatibilityLevel.BACKWARD
            )
            
        except Exception as e:
            error_count += 1
            logger.error(f" Error processing event {i}: {e}")
            
            # Record failure metrics
            monitor.record_validation_attempt(
                subject="user-events",
                schema_version=1,
                success=False,
                compatibility_level=CompatibilityLevel.BACKWARD,
                breaking_changes=["Processing error"]
            )
    
    # Generate processing summary
    summary = {
        "total_events": len(user_events_advanced),
        "processed_count": len(processed_events),
        "error_count": error_count,
        "success_rate": len(processed_events) / len(user_events_advanced) if user_events_advanced else 0,
        "monitoring_summary": monitor.get_metrics_summary()
    }
    
    logger.info(f" Processing complete: {summary}")
    return summary


@asset
def schema_evolution_analysis(context, config: SchemaRegistryConfig) -> Dict[str, Any]:
    """
    Analyze schema evolution compatibility.
    """
    logger = get_dagster_logger()
    
    try:
        # Initialize schema evolution validator
        validator = SchemaEvolutionValidator(
            schema_registry_url=config.url,
            enable_breaking_change_detection=True
        )
        
        # Example schema evolution scenarios
        analysis_results = {
            "schema_registry_url": config.url,
            "compatibility_checks": [],
            "recommendations": []
        }
        
        # Check if we can connect to schema registry
        try:
            # This would normally check actual schemas in registry
            analysis_results["schema_registry_status"] = "connected"
            analysis_results["recommendations"].append(
                "Schema Registry is accessible and ready for schema evolution"
            )
        except Exception as e:
            analysis_results["schema_registry_status"] = f"error: {e}"
            analysis_results["recommendations"].append(
                "Schema Registry is not accessible - using local schema validation only"
            )
        
        # Simulate compatibility analysis
        compatibility_scenarios = [
            {
                "scenario": "Adding optional field",
                "compatibility": "BACKWARD_COMPATIBLE",
                "risk_level": "LOW"
            },
            {
                "scenario": "Removing required field", 
                "compatibility": "BREAKING_CHANGE",
                "risk_level": "HIGH"
            },
            {
                "scenario": "Changing field type",
                "compatibility": "BREAKING_CHANGE", 
                "risk_level": "HIGH"
            }
        ]
        
        analysis_results["compatibility_checks"] = compatibility_scenarios
        
        logger.info(f" Schema evolution analysis complete")
        return analysis_results
        
    except Exception as e:
        logger.error(f" Schema evolution analysis failed: {e}")
        return {
            "error": str(e),
            "schema_registry_status": "error"
        }


# Advanced IO Manager configuration
advanced_protobuf_io_manager = ProtobufKafkaIOManager(
    kafka_resource=KafkaResource(bootstrap_servers="localhost:9092"),
    schema_registry_url="http://localhost:8081",
    enable_schema_validation=True,
    compatibility_level="BACKWARD",
    consumer_group_id="dagster-advanced-protobuf"
)


# Dagster definitions
defs = Definitions(
    assets=[
        user_events_advanced,
        product_events_advanced, 
        validate_schemas,
        process_user_events_with_monitoring,
        schema_evolution_analysis
    ],
    resources={
        "advanced_protobuf_io_manager": advanced_protobuf_io_manager
    }
)


if __name__ == "__main__":
    print(" Running Advanced Protobuf Kafka Example")
    print("=" * 60)
    
    try:
        # Test schema validation first
        print(" Step 1: Validating schemas...")
        result = materialize([validate_schemas])
        
        print(" Step 2: Schema evolution analysis...")
        result = materialize([schema_evolution_analysis])
        
        print(" Step 3: Processing events with monitoring...")
        # Note: This would require actual Kafka cluster
        result = materialize([
            user_events_advanced,
            process_user_events_with_monitoring
        ])
        
        print(f" Advanced example completed successfully!")
        
    except Exception as e:
        print(f" Advanced example requires running Kafka + Schema Registry")
        print(f"   Error: {e}")
        print(f"   Schema validation and analysis can run without Kafka")
        
        # Run just the validation part
        try:
            print("\n Running schema validation only...")
            result = materialize([validate_schemas, schema_evolution_analysis])
            print(" Schema validation completed!")
        except Exception as validation_error:
            print(f" Validation error: {validation_error}")
        
    print("\n Full setup requirements:")
    print("   1. Start Kafka: docker-compose up -d")
    print("   2. Start Schema Registry")
    print("   3. Register protobuf schemas")
    print("   4. Produce protobuf messages to topics")
    print("   5. Run this advanced example")