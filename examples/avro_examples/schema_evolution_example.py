"""
Example demonstrating schema evolution validation in Dagster Kafka integration.
Shows how to handle schema changes safely in production pipelines.
"""

from dagster import asset, Definitions, Config
from dagster_kafka import KafkaResource, avro_kafka_io_manager, CompatibilityLevel


class SchemaEvolutionConfig(Config):
    """Configuration for schema evolution validation."""
    topic: str = "user-events"
    schema_file: str = "schemas/user_v2.avsc"
    max_messages: int = 10
    validate_evolution: bool = True


@asset(io_manager_key="avro_kafka_io_manager")
def user_events_with_validation(context, config: SchemaEvolutionConfig):
    """
    Load user events with schema evolution validation.
    Prevents breaking changes from crashing the pipeline.
    """
    io_manager = context.resources.avro_kafka_io_manager
    
    # Schema evolution validation happens automatically
    # If schema is incompatible, this will raise an error before consuming
    return io_manager.load_input(
        context,
        topic=config.topic,
        schema_file=config.schema_file,
        max_messages=config.max_messages,
        validate_evolution=config.validate_evolution
    )


@asset
def schema_evolution_history(context):
    """Get the schema evolution history for analysis."""
    io_manager = context.resources.avro_kafka_io_manager
    
    # Get evolution history for the topic
    history = io_manager.get_schema_evolution_history("user-events")
    
    context.log.info(f"Found {len(history)} schema versions")
    for version_info in history:
        context.log.info(f"Version {version_info['version']}: Schema ID {version_info['schema_id']}")
    
    return history


# Configuration with different compatibility levels
defs = Definitions(
    assets=[user_events_with_validation, schema_evolution_history],
    resources={
        "kafka": KafkaResource(bootstrap_servers="localhost:9092"),
        "avro_kafka_io_manager": avro_kafka_io_manager.configured({
            "schema_registry_url": "http://localhost:8081",
            "enable_schema_validation": True,
            "compatibility_level": "BACKWARD"  # BACKWARD, FORWARD, FULL, etc.
        })
    }
)

# Example usage with different compatibility levels:
# 
# BACKWARD: New schema can read old data
# FORWARD: Old schema can read new data  
# FULL: Both backward and forward compatible
# BACKWARD_TRANSITIVE: Backward compatible with all previous versions
# FORWARD_TRANSITIVE: Forward compatible with all previous versions
# FULL_TRANSITIVE: Both backward and forward transitive
# NONE: No compatibility checking