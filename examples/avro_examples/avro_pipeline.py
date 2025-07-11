"""
Example Dagster pipeline using Avro Kafka integration.
Demonstrates consuming Avro messages from Kafka topics.
"""

from dagster import asset, Definitions, Config
from dagster_kafka import KafkaResource, avro_kafka_io_manager


class UserEventsConfig(Config):
    schema_file: str
    max_messages: int = 10


class AnalyticsEventsConfig(Config):
    schema_id: int
    max_messages: int = 10


# Configure resources
kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")


@asset(io_manager_key="avro_kafka_io_manager")
def user_events(context, config: UserEventsConfig):
    """Load user events from Kafka using local Avro schema."""
    # Get the IO manager from context
    io_manager = context.resources.avro_kafka_io_manager
    
    return io_manager.load_input(
        context,
        topic="user-events",
        schema_file=config.schema_file,
        max_messages=config.max_messages
    )


@asset(io_manager_key="avro_kafka_io_manager")
def analytics_events(context, config: AnalyticsEventsConfig):
    """Load analytics events from Kafka using Schema Registry."""
    # Get the IO manager from context
    io_manager = context.resources.avro_kafka_io_manager
    
    return io_manager.load_input(
        context,
        topic="analytics-events",
        schema_id=config.schema_id,
        max_messages=config.max_messages
    )


# Define the job
defs = Definitions(
    assets=[user_events, analytics_events],
    resources={
        "kafka": kafka_resource,
        "avro_kafka_io_manager": avro_kafka_io_manager.configured({
            "schema_registry_url": "http://localhost:8081"
        })
    }
)