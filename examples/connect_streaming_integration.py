"""
Design Pattern: Integrating Kafka Connect with Kafka Streaming Pipelines

This file demonstrates how Confluent Connect can be integrated with existing
Kafka streaming pipelines in Dagster.
"""
import os
import sys
from typing import Dict, Any, List

# Add the project root to the Python path to ensure imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from dagster_kafka.connect.client import ConfluentConnectClient
from dagster_kafka.connect.assets import create_connector_asset, ConnectorConfig
# Import your existing Kafka IO managers
from dagster_kafka.io_manager import KafkaIOManager
from dagster_kafka.json_schema_io_manager import JsonSchemaIOManager

"""
PATTERN 1: Connector → Kafka → Processing Pipeline

In this pattern, a Kafka Connect source connector ingests data into Kafka topics,
and your existing Kafka consumer assets process this data.

┌─────────────┐    ┌─────────┐    ┌────────────┐
│ Source      │    │         │    │ Processing │
│ Connector   │───►│ Kafka   │───►│ Pipeline   │
│ (Databases) │    │ Topics  │    │ (Assets)   │
└─────────────┘    └─────────┘    └────────────┘
"""
def create_source_to_processing_pipeline():
    """
    Creates a pipeline where a source connector feeds data to processing assets.
    
    Example: A database connector that streams changes to a Kafka topic,
    which is then processed by your existing Kafka consumer assets.
    """
    from dagster import Definitions, asset, AssetExecutionContext
    
    # Define the connector asset that will ingest data
    source_connector = create_connector_asset(
        key_prefix=["mysql", "cdc"],
        group_name="connectors",
    )
    
    # Your existing Kafka consumer asset that processes the data
    @asset(
        key_prefix=["kafka", "processed"],
        group_name="processing",
        deps=[source_connector],  # Make this dependent on the connector
        io_manager_key="kafka_json_schema_io_manager",
    )
    def process_mysql_changes(context: AssetExecutionContext):
        """Process the MySQL CDC events from Kafka."""
        # Your existing Kafka consumption logic
        pass
    
    # Define all components
    defs = Definitions(
        assets=[source_connector, process_mysql_changes],
        resources={
            "connect": ConfluentConnectResource(
                connect_url="http://localhost:8083",
            ),
            # Your existing Kafka resources
            "kafka_io_manager": KafkaIOManager(
                # Configuration
            ),
            "kafka_json_schema_io_manager": JsonSchemaIOManager(
                # Configuration
            ),
        },
    )
    
    return defs

"""
PATTERN 2: Processing Pipeline → Kafka → Connector

In this pattern, your processing pipeline outputs data to Kafka topics,
which are then consumed by Kafka Connect sink connectors to load into
external systems.

┌────────────┐    ┌─────────┐    ┌─────────────┐
│ Processing │    │         │    │ Sink        │
│ Pipeline   │───►│ Kafka   │───►│ Connector   │
│ (Assets)   │    │ Topics  │    │ (Databases) │
└────────────┘    └─────────┘    └─────────────┘
"""
def create_processing_to_sink_pipeline():
    """
    Creates a pipeline where processing assets output to sink connectors.
    
    Example: Your processing assets output data to Kafka topics, 
    which are then loaded into a database by a sink connector.
    """
    from dagster import Definitions, asset, AssetExecutionContext, Output
    
    # Your existing Kafka producer asset
    @asset(
        key_prefix=["kafka", "output"],
        group_name="processing",
        io_manager_key="kafka_json_schema_io_manager",
    )
    def generate_database_records(context: AssetExecutionContext):
        """Generate records to be loaded into the database."""
        # Your existing Kafka production logic
        return Output({"data": "example"})
    
    # Define the sink connector asset that will load data
    sink_connector = create_connector_asset(
        key_prefix=["elasticsearch", "sink"],
        group_name="connectors",
        # Make this dependent on the Kafka producer asset
        # This ensures the connector is only created/updated
        # after the Kafka topic is ready
        deps=[generate_database_records],
    )
    
    # Define all components
    defs = Definitions(
        assets=[generate_database_records, sink_connector],
        resources={
            "connect": ConfluentConnectResource(
                connect_url="http://localhost:8083",
            ),
            # Your existing Kafka resources
            "kafka_io_manager": KafkaIOManager(
                # Configuration
            ),
            "kafka_json_schema_io_manager": JsonSchemaIOManager(
                # Configuration
            ),
        },
    )
    
    return defs

"""
PATTERN 3: Bidirectional Integration

In this pattern, you have both source and sink connectors, forming a complete
data pipeline from source systems through Kafka to destination systems.

┌────────────┐    ┌─────────┐    ┌────────────┐    ┌─────────┐    ┌────────────┐
│ Source     │    │         │    │ Processing │    │         │    │ Sink       │
│ Connector  │───►│ Kafka   │───►│ Pipeline   │───►│ Kafka   │───►│ Connector  │
│ (Input)    │    │ Topics  │    │ (Assets)   │    │ Topics  │    │ (Output)   │
└────────────┘    └─────────┘    └────────────┘    └─────────┘    └────────────┘
"""
def create_bidirectional_pipeline():
    """
    Creates a complete pipeline with source and sink connectors.
    
    Example: A CDC connector streams database changes to Kafka,
    your assets process and transform this data, then a sink connector
    loads the results into Elasticsearch.
    """
    from dagster import Definitions, asset, AssetExecutionContext, Output
    
    # Source connector asset
    source_connector = create_connector_asset(
        key_prefix=["mysql", "cdc"],
        group_name="source_connectors",
    )
    
    # Processing asset
    @asset(
        key_prefix=["kafka", "processed"],
        group_name="processing",
        deps=[source_connector],  # Depends on source connector
        io_manager_key="kafka_json_schema_io_manager",
    )
    def transform_data(context: AssetExecutionContext):
        """Transform the data from the source topic to the destination format."""
        # Your transformation logic
        return Output({"transformed": "data"})
    
    # Sink connector asset
    sink_connector = create_connector_asset(
        key_prefix=["elasticsearch", "sink"],
        group_name="sink_connectors",
        deps=[transform_data],  # Depends on the transformation
    )
    
    # Define all components
    defs = Definitions(
        assets=[source_connector, transform_data, sink_connector],
        resources={
            "connect": ConfluentConnectResource(
                connect_url="http://localhost:8083",
            ),
            # Your existing Kafka resources
            "kafka_io_manager": KafkaIOManager(
                # Configuration
            ),
            "kafka_json_schema_io_manager": JsonSchemaIOManager(
                # Configuration
            ),
        },
    )
    
    return defs

"""
PATTERN 4: Health-Aware Streaming

In this pattern, connector health is monitored and affects the behavior
of your streaming pipeline.

┌─────────────┐    ┌─────────────┐
│ Health      │───►│ Conditional │
│ Monitor     │    │ Processing  │
└─────────────┘    └─────────────┘
       │                  │
       ▼                  ▼
┌─────────────┐    ┌─────────────┐
│ Connector   │◄───┤ Kafka       │
│ Management  │    │ Topics      │
└─────────────┘    └─────────────┘
"""
def create_health_aware_streaming():
    """
    Creates a pipeline where connector health affects streaming behavior.
    
    Example: The pipeline stops processing data if the connector is unhealthy,
    and automatically recovers when the connector is healthy again.
    """
    from dagster import Definitions, job, op, sensor, RunRequest, SensorResult
    
    @sensor(minimum_interval_seconds=60)
    def connector_health_sensor(context):
        """Monitor connector health and trigger processing job conditionally."""
        client = ConfluentConnectClient(base_url="http://localhost:8083")
        connector_name = "important-source-connector"
        
        try:
            status = client.get_connector_status(connector_name)
            is_healthy = status["connector"]["state"] == "RUNNING"
            
            if is_healthy:
                context.log.info(f"Connector {connector_name} is healthy. Starting processing job.")
                return RunRequest(run_key=None, job_name="process_data_job")
            else:
                context.log.warning(f"Connector {connector_name} is unhealthy. Skipping processing.")
                return SensorResult(skip_reason=f"Connector {connector_name} is not running")
        except Exception as e:
            context.log.error(f"Error checking connector health: {e}")
            return SensorResult(skip_reason=f"Error checking connector health: {e}")
    
    @op
    def process_data(context):
        """Process data from Kafka only when the connector is healthy."""
        # Your data processing logic
        pass
    
    @job
    def process_data_job():
        """Job that processes data from Kafka."""
        process_data()
    
    # Define all components
    defs = Definitions(
        jobs=[process_data_job],
        sensors=[connector_health_sensor],
    )
    
    return defs

# Usage example
if __name__ == "__main__":
    print("This is a design pattern module that demonstrates integration approaches.")
    print("It does not execute any actual code.")
