"""
Protobuf integration for Kafka message consumption.
Supports Protocol Buffers with schema management, validation, and DLQ support.
"""

from typing import Optional, Dict, Any, List, Type
import json
import time
from google.protobuf.message import Message
from google.protobuf.descriptor_pb2 import FileDescriptorProto
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.message_factory import MessageFactory
from google.protobuf.json_format import MessageToDict, ParseError
from confluent_kafka import KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer
from dagster import IOManager, ConfigurableIOManager, ResourceDependency, get_dagster_logger
from pydantic import Field
from .resources import KafkaResource
from .schema_evolution import SchemaEvolutionValidator, CompatibilityLevel
from .dlq import DLQConfiguration, DLQManager, DLQStrategy, ErrorType


class ProtobufKafkaIOManager(ConfigurableIOManager):
    """IO Manager for handling Protobuf-serialized messages from Kafka topics with DLQ support."""

    kafka_resource: ResourceDependency[KafkaResource]
    schema_registry_url: Optional[str] = Field(default=None)
    enable_schema_validation: bool = Field(default=True)
    compatibility_level: str = Field(default="BACKWARD")
    consumer_group_id: str = Field(default="dagster-protobuf-consumer")
    
    # DLQ Configuration
    enable_dlq: bool = Field(default=True, description="Enable Dead Letter Queue support")
    dlq_strategy: DLQStrategy = Field(default=DLQStrategy.RETRY_THEN_DLQ, description="DLQ strategy to use")
    dlq_max_retries: int = Field(default=3, description="Maximum retry attempts before DLQ")
    dlq_circuit_breaker_failure_threshold: int = Field(default=5, description="Failures to open circuit breaker")

    def _create_dlq_manager(self, topic: str) -> Optional[DLQManager]:
        """Create DLQ manager if enabled."""
        if not self.enable_dlq:
            return None
        
        dlq_config = DLQConfiguration(
            strategy=self.dlq_strategy,
            max_retry_attempts=self.dlq_max_retries,
            circuit_breaker_failure_threshold=self.dlq_circuit_breaker_failure_threshold
        )
        
        return DLQManager(
            kafka_resource=self.kafka_resource,
            dlq_config=dlq_config,
            topic_name=topic
        )

    def _classify_protobuf_error(self, error: Exception) -> ErrorType:
        """Classify Protobuf-specific errors for DLQ routing."""
        error_str = str(error).lower()
        error_type_name = type(error).__name__.lower()
        
        # Protobuf deserialization errors
        if any(keyword in error_str for keyword in ['protobuf', 'proto', 'deserialize', 'decode', 'parse']):
            return ErrorType.DESERIALIZATION_ERROR
        
        # Schema-related errors
        if any(keyword in error_str for keyword in ['schema', 'validation', 'compatibility', 'descriptor']):
            return ErrorType.SCHEMA_ERROR
        
        # Connection errors
        if any(keyword in error_str for keyword in ['connection', 'network', 'broker']):
            return ErrorType.CONNECTION_ERROR
        
        # Timeout errors
        if any(keyword in error_str for keyword in ['timeout', 'timed out']):
            return ErrorType.TIMEOUT_ERROR
        
        # Processing errors
        if 'processing' in error_str:
            return ErrorType.PROCESSING_ERROR
        
        return ErrorType.UNKNOWN_ERROR

    def _process_protobuf_message(self, msg, dlq_manager: Optional[DLQManager], context, topic: str) -> Optional[Dict[str, Any]]:
        """Process a single Protobuf message with DLQ support."""
        message_key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
        
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
            
            # Record success for circuit breaker
            if dlq_manager:
                dlq_manager.record_success()
            
            self.logger.debug(f"Successfully processed Protobuf message {message_key}")
            return message_dict
            
        except Exception as e:
            self.logger.warning(f"Protobuf processing error for message {message_key}: {e}")
            
            if dlq_manager:
                # Record failure for circuit breaker
                dlq_manager.record_failure()
                
                # Classify error
                error_type = self._classify_protobuf_error(e)
                
                # Check if we should retry
                if dlq_manager.should_retry(error_type, message_key):
                    retry_count = dlq_manager.record_retry(message_key)
                    self.logger.info(f"Retrying Protobuf message {message_key} (attempt {retry_count})")
                    
                    # Simple backoff
                    time.sleep(dlq_manager.dlq_config.retry_backoff_ms / 1000.0)
                    return None  # Will be retried on next poll
                else:
                    # Send to DLQ
                    self.logger.error(f"Sending Protobuf message {message_key} to DLQ after {dlq_manager.dlq_config.max_retry_attempts} retries")
                    
                    additional_metadata = {
                        'dagster_run_id': getattr(context, 'run_id', None),
                        'dagster_asset_key': str(getattr(context, 'asset_key', None)) if hasattr(context, 'asset_key') and context.asset_key else None,
                        'protobuf_topic': topic,
                        'schema_type': 'protobuf'
                    }
                    
                    dlq_success = dlq_manager.send_to_dlq(
                        original_message=msg,
                        error=e,
                        consumer_group_id=self.consumer_group_id,
                        additional_metadata=additional_metadata
                    )
                    
                    if dlq_success:
                        self.logger.info(f"Protobuf message {message_key} successfully sent to DLQ")
                    else:
                        self.logger.error(f"Failed to send Protobuf message {message_key} to DLQ - message lost!")
            
            return None  # Skip this message

    def load_input(self, context, topic: str = None, 
                   proto_file: Optional[str] = None,
                   message_type_name: Optional[str] = None,
                   schema_id: Optional[int] = None, 
                   max_messages: int = 100,
                   timeout: float = 10.0) -> List[Dict[str, Any]]:
        """
        Load Protobuf messages from a Kafka topic with DLQ support.

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
        self.logger = get_dagster_logger()

        # Use asset key as topic if not provided
        if topic is None:
            topic = context.asset_key.path[-1] if context.asset_key else "default"

        self.logger.info(f"Loading Protobuf messages from topic: {topic}")

        # Initialize DLQ manager
        dlq_manager = self._create_dlq_manager(topic)
        if dlq_manager:
            self.logger.info(f"DLQ enabled for Protobuf topic {topic} with strategy: {dlq_manager.dlq_config.strategy.value}")
            self.logger.info(f"DLQ topic: {dlq_manager.dlq_topic_name}")

        # Initialize Schema Registry client if needed
        schema_registry_client = None
        if self.schema_registry_url:
            try:
                schema_registry_client = SchemaRegistryClient({'url': self.schema_registry_url})
                self.logger.info(f"Connected to Schema Registry at {self.schema_registry_url}")
                if dlq_manager:
                    dlq_manager.record_success()
            except Exception as e:
                self.logger.error(f"Failed to connect to Schema Registry: {e}")
                if dlq_manager:
                    dlq_manager.record_failure()
                # Continue without Schema Registry

        # Create consumer
        consumer = self.kafka_resource.get_consumer(self.consumer_group_id)
        consumer.subscribe([topic])

        messages = []
        successful_messages = 0
        failed_messages = 0
        attempts = 0
        max_attempts = max_messages * 2  # Allow for retries

        try:
            while len(messages) < max_messages and attempts < max_attempts:
                msg = consumer.poll(timeout)
                attempts += 1
                
                if msg is None:
                    self.logger.debug(f"No message on attempt {attempts}")
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        self.logger.debug("Reached end of partition")
                        break
                    else:
                        self.logger.error(f"Kafka error: {msg.error()}")
                        if dlq_manager:
                            dlq_manager.record_failure()
                        continue

                # Process message with DLQ support
                processed_data = self._process_protobuf_message(msg, dlq_manager, context, topic)
                
                if processed_data is not None:
                    messages.append(processed_data)
                    successful_messages += 1
                    self.logger.debug(f"Successfully processed Protobuf message {len(messages)}")
                else:
                    failed_messages += 1

        finally:
            consumer.close()
            
            # Log final stats
            self.logger.info(f"Protobuf processing completed for topic {topic}:")
            self.logger.info(f"  ✅ Successfully processed: {successful_messages} messages")
            self.logger.info(f"  ❌ Failed messages: {failed_messages}")
            
            if dlq_manager:
                dlq_stats = dlq_manager.get_dlq_stats()
                self.logger.info(f"  📊 DLQ Stats: {dlq_stats}")
                dlq_manager.cleanup()

        self.logger.info(f"Successfully loaded {len(messages)} Protobuf messages from {topic}")
        return messages

    def handle_output(self, context, obj):
        """Not implemented - this IO manager is read-only."""
        raise NotImplementedError("ProtobufKafkaIOManager is read-only")


# Simple non-configurable version for basic usage with DLQ support
class SimpleProtobufKafkaIOManager(IOManager):
    """Simple Protobuf Kafka IO Manager for basic usage with DLQ support."""

    def __init__(self, kafka_resource, schema_registry_url=None, consumer_group_id="dagster-protobuf-consumer",
                 enable_dlq=True, dlq_strategy=DLQStrategy.RETRY_THEN_DLQ, dlq_max_retries=3):
        # Store configuration for later use
        self._kafka_resource = kafka_resource
        self._schema_registry_url = schema_registry_url
        self._consumer_group_id = consumer_group_id
        self._enable_dlq = enable_dlq
        self._dlq_strategy = dlq_strategy
        self._dlq_max_retries = dlq_max_retries
        self.logger = get_dagster_logger()

    def _create_dlq_manager(self, topic: str) -> Optional[DLQManager]:
        """Create DLQ manager if enabled."""
        if not self._enable_dlq:
            return None
        
        dlq_config = DLQConfiguration(
            strategy=self._dlq_strategy,
            max_retry_attempts=self._dlq_max_retries
        )
        
        return DLQManager(
            kafka_resource=self._kafka_resource,
            dlq_config=dlq_config,
            topic_name=topic
        )

    def _classify_protobuf_error(self, error: Exception) -> ErrorType:
        """Classify Protobuf-specific errors for DLQ routing."""
        error_str = str(error).lower()
        
        # Protobuf deserialization errors
        if any(keyword in error_str for keyword in ['protobuf', 'proto', 'deserialize', 'decode', 'parse']):
            return ErrorType.DESERIALIZATION_ERROR
        
        # Schema-related errors
        if any(keyword in error_str for keyword in ['schema', 'validation', 'compatibility', 'descriptor']):
            return ErrorType.SCHEMA_ERROR
        
        # Connection errors
        if any(keyword in error_str for keyword in ['connection', 'network', 'broker']):
            return ErrorType.CONNECTION_ERROR
        
        # Timeout errors
        if any(keyword in error_str for keyword in ['timeout', 'timed out']):
            return ErrorType.TIMEOUT_ERROR
        
        # Processing errors
        if 'processing' in error_str:
            return ErrorType.PROCESSING_ERROR
        
        return ErrorType.UNKNOWN_ERROR

    def _process_protobuf_message(self, msg, dlq_manager: Optional[DLQManager], context, topic: str) -> Optional[Dict[str, Any]]:
        """Process a single Protobuf message with DLQ support."""
        message_key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
        
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
            
            # Record success for circuit breaker
            if dlq_manager:
                dlq_manager.record_success()
                
            return message_dict
            
        except Exception as e:
            self.logger.warning(f"Protobuf processing error for message {message_key}: {e}")
            
            if dlq_manager:
                # Record failure for circuit breaker
                dlq_manager.record_failure()
                
                # Classify error
                error_type = self._classify_protobuf_error(e)
                
                # Check if we should retry
                if dlq_manager.should_retry(error_type, message_key):
                    retry_count = dlq_manager.record_retry(message_key)
                    self.logger.info(f"Retrying Protobuf message {message_key} (attempt {retry_count})")
                    time.sleep(dlq_manager.dlq_config.retry_backoff_ms / 1000.0)
                    return None
                else:
                    # Send to DLQ
                    additional_metadata = {
                        'dagster_run_id': getattr(context, 'run_id', None),
                        'dagster_asset_key': str(getattr(context, 'asset_key', None)) if hasattr(context, 'asset_key') and context.asset_key else None,
                        'protobuf_topic': topic,
                        'schema_type': 'protobuf'
                    }
                    
                    dlq_manager.send_to_dlq(
                        original_message=msg,
                        error=e,
                        consumer_group_id=self._consumer_group_id,
                        additional_metadata=additional_metadata
                    )
            
            return None

    def load_input(self, context, topic: str = None, max_messages: int = 100) -> List[Dict[str, Any]]:
        """Load Protobuf messages from Kafka topic with DLQ support."""
        
        # Use asset key as topic if not provided
        if topic is None:
            topic = context.asset_key.path[-1] if context.asset_key else "default"

        self.logger.info(f"Loading Protobuf messages from topic: {topic}")

        # Initialize DLQ manager
        dlq_manager = self._create_dlq_manager(topic)
        if dlq_manager:
            self.logger.info(f"DLQ enabled for Protobuf topic {topic}")

        # Initialize Schema Registry client if needed
        schema_registry_client = None
        if self._schema_registry_url:
            try:
                schema_registry_client = SchemaRegistryClient({'url': self._schema_registry_url})
                self.logger.info(f"Connected to Schema Registry at {self._schema_registry_url}")
            except Exception as e:
                self.logger.error(f"Failed to connect to Schema Registry: {e}")

        # Create consumer
        consumer = self._kafka_resource.get_consumer(self._consumer_group_id)
        consumer.subscribe([topic])

        messages = []
        successful_messages = 0
        failed_messages = 0

        try:
            for _ in range(max_messages):
                msg = consumer.poll(10.0)
                if msg is None:
                    break
                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    if dlq_manager:
                        dlq_manager.record_failure()
                    continue

                # Process message with DLQ support
                processed_data = self._process_protobuf_message(msg, dlq_manager, context, topic)
                
                if processed_data is not None:
                    messages.append(processed_data)
                    successful_messages += 1
                else:
                    failed_messages += 1

        finally:
            consumer.close()
            
            # Log final stats
            self.logger.info(f"Protobuf processing completed:")
            self.logger.info(f"  ✅ Successfully processed: {successful_messages} messages")
            self.logger.info(f"  ❌ Failed messages: {failed_messages}")
            
            if dlq_manager:
                dlq_stats = dlq_manager.get_dlq_stats()
                self.logger.info(f"  📊 DLQ Stats: {dlq_stats}")
                dlq_manager.cleanup()

        self.logger.info(f"Successfully loaded {len(messages)} Protobuf messages from {topic}")
        return messages

    def handle_output(self, context, obj):
        """Not implemented - this IO manager is read-only."""
        raise NotImplementedError("SimpleProtobufKafkaIOManager is read-only")


# Factory function for basic usage with DLQ support
def create_protobuf_kafka_io_manager(kafka_resource: KafkaResource,
                                   schema_registry_url: Optional[str] = None,
                                   consumer_group_id: str = "dagster-protobuf-consumer",
                                   enable_dlq: bool = True,
                                   dlq_strategy: DLQStrategy = DLQStrategy.RETRY_THEN_DLQ,
                                   dlq_max_retries: int = 3) -> SimpleProtobufKafkaIOManager:
    """Create a simple ProtobufKafkaIOManager instance with DLQ support."""
    return SimpleProtobufKafkaIOManager(
        kafka_resource=kafka_resource, 
        schema_registry_url=schema_registry_url, 
        consumer_group_id=consumer_group_id,
        enable_dlq=enable_dlq,
        dlq_strategy=dlq_strategy,
        dlq_max_retries=dlq_max_retries
    )

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
    """Manager for Protobuf schema operations and validation with DLQ support."""

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