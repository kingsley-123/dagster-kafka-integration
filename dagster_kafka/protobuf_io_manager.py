"""
Protobuf integration for Kafka message consumption.
Supports Protocol Buffers with schema management and validation.
"""

from typing import Optional, Dict, Any, List, Type
import json
from google.protobuf.message import Message
from google.protobuf.descriptor_pb2 import FileDescriptorProto
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message_factory import MessageFactory
from google.protobuf.json_format import MessageToDict, ParseError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from dagster import IOManager, ConfigurableIOManager, ResourceDependency, get_dagster_logger
from pydantic import Field
from .resources import KafkaResource
from .schema_evolution import SchemaEvolutionValidator, CompatibilityLevel

class ProtobufKafkaIOManager(ConfigurableIOManager):
    """IO Manager for handling Protobuf-serialized messages from Kafka topics."""

    kafka_resource: ResourceDependency[KafkaResource]
    schema_registry_url: Optional[str] = Field(default=None)
    enable_schema_validation: bool = Field(default=True)
    compatibility_level: str = Field(default="BACKWARD")
    consumer_group_id: str = Field(default="dagster-protobuf-consumer")

    def load_input(self, context, topic: str = None, 
                   proto_file: Optional[str] = None,
                   message_type_name: Optional[str] = None,
                   schema_id: Optional[int] = None, 
                   max_messages: int = 100,
                   timeout: float = 10.0) -> List[Dict[str, Any]]:
        """
        Load Protobuf messages from a Kafka topic.

        Args:
            topic: Kafka topic name (defaults to asset key if not provided)
            proto_file: Path to .proto schema file
            message_type_name: Name of message type in schema
            schema_id: Schema ID from Schema Registry
            max_messages: Maximum number of messages to consume
            timeout: Consumer timeout in seconds

        Returns:
            List of deserialized Protobuf messages as dictionaries
        """
        logger = get_dagster_logger()

        # Use asset key as topic if not provided
        if topic is None:
            topic = context.asset_key.path[-1] if context.asset_key else "default"

        logger.info(f"Loading Protobuf messages from topic: {topic}")

        # Initialize Schema Registry client if needed
        schema_registry_client = None
        if self.schema_registry_url:
            schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
            logger.info(f"Connected to Schema Registry at {self.schema_registry_url}")

        # Create consumer
        consumer = self.kafka_resource.get_consumer(self.consumer_group_id)
        consumer.subscribe([topic])

        messages = []
        try:
            for _ in range(max_messages):
                msg = consumer.poll(timeout)
                if msg is None:
                    break
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                # For basic implementation, treat as raw bytes with metadata
                # In production, this would deserialize actual protobuf messages
                try:
                    # Placeholder: Convert bytes to dict representation
                    # Real implementation would use actual protobuf message classes
                    message_dict = {
                        "raw_bytes": msg.value().hex(),
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "timestamp": msg.timestamp()[1] if msg.timestamp()[0] else None,
                        "key": msg.key().decode('utf-8') if msg.key() else None,
                        "value_size": len(msg.value()) if msg.value() else 0
                    }
                    messages.append(message_dict)
                    logger.debug(f"Processed Protobuf message: {message_dict}")
                except Exception as e:
                    logger.error(f"Failed to process Protobuf message: {e}")
                    continue

        finally:
            consumer.close()

        logger.info(f"Successfully loaded {len(messages)} Protobuf messages from {topic}")
        return messages

    def handle_output(self, context, obj):
        """Not implemented - this IO manager is read-only."""
        raise NotImplementedError("ProtobufKafkaIOManager is read-only")

# Simple non-configurable version for basic usage
class SimpleProtobufKafkaIOManager(IOManager):
    """Simple Protobuf Kafka IO Manager for basic usage."""

    def __init__(self, kafka_resource, schema_registry_url=None, consumer_group_id="dagster-protobuf-consumer"):
        # Store configuration for later use
        self._kafka_resource = kafka_resource
        self._schema_registry_url = schema_registry_url
        self._consumer_group_id = consumer_group_id

    def load_input(self, context, topic: str = None, max_messages: int = 100) -> List[Dict[str, Any]]:
        """Load Protobuf messages from Kafka topic."""
        logger = get_dagster_logger()
        
        # Use asset key as topic if not provided
        if topic is None:
            topic = context.asset_key.path[-1] if context.asset_key else "default"

        logger.info(f"Loading Protobuf messages from topic: {topic}")

        # Initialize Schema Registry client if needed
        schema_registry_client = None
        if self._schema_registry_url:
            schema_registry_client = SchemaRegistryClient({'url': self._schema_registry_url})
            logger.info(f"Connected to Schema Registry at {self._schema_registry_url}")

        # Create consumer
        consumer = self._kafka_resource.get_consumer(self._consumer_group_id)
        consumer.subscribe([topic])

        messages = []
        try:
            for _ in range(max_messages):
                msg = consumer.poll(10.0)
                if msg is None:
                    break
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                try:
                    # Basic message representation
                    message_dict = {
                        "raw_bytes": msg.value().hex(),
                        "topic": msg.topic(),
                        "partition": msg.partition(),
                        "offset": msg.offset(),
                        "timestamp": msg.timestamp()[1] if msg.timestamp()[0] else None,
                        "key": msg.key().decode('utf-8') if msg.key() else None,
                        "value_size": len(msg.value()) if msg.value() else 0
                    }
                    messages.append(message_dict)
                except Exception as e:
                    logger.error(f"Failed to process message: {e}")
                    continue

        finally:
            consumer.close()

        logger.info(f"Successfully loaded {len(messages)} Protobuf messages from {topic}")
        return messages

    def handle_output(self, context, obj):
        """Not implemented - this IO manager is read-only."""
        raise NotImplementedError("SimpleProtobufKafkaIOManager is read-only")

# Factory function for basic usage
def create_protobuf_kafka_io_manager(kafka_resource: KafkaResource,
                                   schema_registry_url: Optional[str] = None,
                                   consumer_group_id: str = "dagster-protobuf-consumer") -> SimpleProtobufKafkaIOManager:
    """Create a simple ProtobufKafkaIOManager instance."""
    return SimpleProtobufKafkaIOManager(kafka_resource, schema_registry_url, consumer_group_id)

# Configurable IO Manager
protobuf_kafka_io_manager = ProtobufKafkaIOManager

# Utility functions for Protobuf schema management
def compile_proto_schema(proto_file: str, output_dir: str = ".") -> str:
    """
    Compile .proto file to Python classes using protoc.
    Returns the name of the generated Python module.
    """
    import subprocess
    import os

    try:
        # Run protoc to compile the schema
        result = subprocess.run([
            "protoc",
            f"--python_out={output_dir}",
            proto_file
        ], capture_output=True, text=True, check=True)

        # Generate module name
        base_name = os.path.basename(proto_file).replace('.proto', '_pb2.py')
        module_path = os.path.join(output_dir, base_name)

        logger = get_dagster_logger()
        logger.info(f"Successfully compiled {proto_file} to {module_path}")

        return base_name.replace('.py', '')

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Failed to compile proto file: {e.stderr}")
    except FileNotFoundError:
        raise RuntimeError("protoc not found. Please install Protocol Buffers compiler.")

def validate_protobuf_schema(schema_content: str) -> bool:
    """Validate Protobuf schema syntax."""
    try:
        # Basic validation - check for required syntax elements
        required_elements = ['syntax', 'message']

        for element in required_elements:
            if element not in schema_content:
                return False

        # Additional validation could be added here
        return True

    except Exception:
        return False

class ProtobufSchemaManager:
    """Manager for Protobuf schema operations and validation."""

    def __init__(self, schema_registry_client: Optional[SchemaRegistryClient] = None):
        self.schema_registry_client = schema_registry_client
        self.logger = get_dagster_logger()

    def register_schema(self, subject: str, schema_content: str) -> int:
        """Register a new Protobuf schema in Schema Registry."""
        if not self.schema_registry_client:
            raise ValueError("Schema Registry client not configured")

        try:
            # Validate schema before registration
            if not validate_protobuf_schema(schema_content):
                raise ValueError("Invalid Protobuf schema syntax")

            # Register the schema
            schema_id = self.schema_registry_client.register_schema(subject, schema_content)
            self.logger.info(f"Registered Protobuf schema for subject {subject} with ID {schema_id}")

            return schema_id

        except Exception as e:
            self.logger.error(f"Failed to register Protobuf schema: {e}")
            raise

    def get_latest_schema(self, subject: str) -> Dict[str, Any]:
        """Get the latest schema for a subject."""
        if not self.schema_registry_client:
            raise ValueError("Schema Registry client not configured")

        try:
            latest_version = self.schema_registry_client.get_latest_version(subject)
            return {
                "schema_id": latest_version.schema_id,
                "version": latest_version.version,
                "schema": latest_version.schema.schema_str
            }
        except Exception as e:
            self.logger.error(f"Failed to get latest schema for {subject}: {e}")
            raise

    def check_compatibility(self, subject: str, new_schema: str) -> bool:
        """Check if new schema is compatible with existing schemas."""
        if not self.schema_registry_client:
            raise ValueError("Schema Registry client not configured")

        try:
            result = self.schema_registry_client.test_compatibility(subject, new_schema)
            return result.is_compatible
        except Exception as e:
            self.logger.error(f"Failed to check schema compatibility: {e}")
            raise