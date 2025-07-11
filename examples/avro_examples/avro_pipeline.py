"""
Example Dagster pipeline using Avro Kafka integration.
Demonstrates consuming Avro messages from Kafka topics.
"""

from dagster import asset, Definitions
from dagster_kafka import KafkaResource, AvroKafkaIOManager, avro_kafka_io_manager


# Configure resources
kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")


@asset(
    io_manager_key="avro_kafka_io_manager",
    config_schema={"schema_file": str, "max_messages": int}
)
def user_events(context, avro_kafka_io_manager: AvroKafkaIOManager):
    """Load user events from Kafka using local Avro schema."""
    return avro_kafka_io_manager.load_input(
        context,
        topic="user-events",
        schema_file=context.op_config["schema_file"],
        max_messages=context.op_config.get("max_messages", 10)
    )


@asset(
    io_manager_key="avro_kafka_io_manager", 
    config_schema={"schema_id": int}
)
def analytics_events(context, avro_kafka_io_manager: AvroKafkaIOManager):
    """Load analytics events from Kafka using Schema Registry."""
    return avro_kafka_io_manager.load_input(
        context,
        topic="analytics-events",
        schema_id=context.op_config["schema_id"]
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