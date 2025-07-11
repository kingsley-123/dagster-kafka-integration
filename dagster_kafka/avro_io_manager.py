from typing import Optional, Dict, Any, List
import json
import io
import fastavro
from confluent_kafka.schema_registry import SchemaRegistryClient
from dagster import IOManager, io_manager, get_dagster_logger
from .resources import KafkaResource


class AvroKafkaIOManager(IOManager):
    """IO Manager for handling Avro-serialized messages from Kafka topics."""
    
    def __init__(self, kafka_resource: KafkaResource, schema_registry_url: Optional[str] = None):
        self.kafka_resource = kafka_resource
        self.schema_registry_client = None
        self.logger = get_dagster_logger()
        
        if schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
            self.logger.info(f"Connected to Schema Registry at {schema_registry_url}")
    
    def load_input(self, context, topic: str, schema_file: Optional[str] = None, 
                   schema_id: Optional[int] = None, max_messages: int = 100,
                   timeout: float = 10.0) -> List[Dict[str, Any]]:
        """
        Load Avro messages from a Kafka topic.
        
        Args:
            topic: Kafka topic name
            schema_file: Path to local Avro schema file (.avsc)
            schema_id: Schema ID from Schema Registry
            max_messages: Maximum number of messages to consume
            timeout: Consumer timeout in seconds
            
        Returns:
            List of deserialized Avro messages as dictionaries
        """
        self.logger.info(f"Loading Avro messages from topic: {topic}")
        
        # Get schema
        schema = self._get_schema(schema_file, schema_id)
        
        # Create consumer
        consumer = self.kafka_resource.get_consumer()
        consumer.subscribe([topic])
        
        messages = []
        try:
            for _ in range(max_messages):
                msg = consumer.poll(timeout)
                if msg is None:
                    break
                if msg.error():
                    self.logger.error(f"Consumer error: {msg.error()}")
                    continue
                
                # Deserialize Avro message
                try:
                    deserialized_msg = self._deserialize_avro_message(msg.value(), schema)
                    messages.append(deserialized_msg)
                    self.logger.debug(f"Deserialized message: {deserialized_msg}")
                except Exception as e:
                    self.logger.error(f"Failed to deserialize message: {e}")
                    continue
                    
        finally:
            consumer.close()
            
        self.logger.info(f"Successfully loaded {len(messages)} Avro messages from {topic}")
        return messages
    
    def _get_schema(self, schema_file: Optional[str], schema_id: Optional[int]):
        """Get Avro schema from file or Schema Registry."""
        if schema_file:
            self.logger.info(f"Loading schema from file: {schema_file}")
            with open(schema_file, 'r') as f:
                schema_dict = json.load(f)
                return fastavro.parse_schema(schema_dict)
        
        elif schema_id and self.schema_registry_client:
            self.logger.info(f"Loading schema from registry with ID: {schema_id}")
            schema = self.schema_registry_client.get_schema(schema_id)
            schema_dict = json.loads(schema.schema_str)
            return fastavro.parse_schema(schema_dict)
        
        else:
            raise ValueError("Must provide either schema_file or schema_id with schema_registry_url")
    
    def _deserialize_avro_message(self, message_value: bytes, schema) -> Dict[str, Any]:
        """Deserialize Avro binary message using schema."""
        bytes_reader = io.BytesIO(message_value)
        return fastavro.schemaless_reader(bytes_reader, schema)
    
    def handle_output(self, context, obj):
        """Not implemented - this IO manager is read-only."""
        raise NotImplementedError("AvroKafkaIOManager is read-only")


@io_manager(required_resource_keys={"kafka"})
def avro_kafka_io_manager(context) -> AvroKafkaIOManager:
    """Factory function for AvroKafkaIOManager."""
    kafka_resource = context.resources.kafka
    schema_registry_url = context.op_config.get("schema_registry_url")
    return AvroKafkaIOManager(kafka_resource, schema_registry_url)