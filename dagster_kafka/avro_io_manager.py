from typing import Optional, Dict, Any, List
import json
import io
import fastavro
from confluent_kafka.schema_registry import SchemaRegistryClient
from dagster import IOManager, io_manager, get_dagster_logger
from .resources import KafkaResource
from .schema_evolution import SchemaEvolutionValidator, CompatibilityLevel


class AvroKafkaIOManager(IOManager):
    """IO Manager for handling Avro-serialized messages from Kafka topics with schema evolution validation."""
    
    def __init__(self, 
                 kafka_resource: KafkaResource, 
                 schema_registry_url: Optional[str] = None,
                 enable_schema_validation: bool = True,
                 compatibility_level: CompatibilityLevel = CompatibilityLevel.BACKWARD):
        self.kafka_resource = kafka_resource
        self.schema_registry_client = None
        self.schema_validator = None
        self.enable_schema_validation = enable_schema_validation
        self.compatibility_level = compatibility_level
        self.logger = get_dagster_logger()
        
        if schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
            self.schema_validator = SchemaEvolutionValidator(self.schema_registry_client)
            self.logger.info(f"Connected to Schema Registry at {schema_registry_url}")
            if enable_schema_validation:
                self.logger.info(f"Schema evolution validation enabled with {compatibility_level.value} compatibility")
    
    def load_input(self, context, topic: str, schema_file: Optional[str] = None, 
                   schema_id: Optional[int] = None, max_messages: int = 100,
                   timeout: float = 10.0, validate_evolution: bool = None) -> List[Dict[str, Any]]:
        """
        Load Avro messages from a Kafka topic with optional schema evolution validation.
        
        Args:
            topic: Kafka topic name
            schema_file: Path to local Avro schema file (.avsc)
            schema_id: Schema ID from Schema Registry
            max_messages: Maximum number of messages to consume
            timeout: Consumer timeout in seconds
            validate_evolution: Override global schema validation setting
            
        Returns:
            List of deserialized Avro messages as dictionaries
        """
        self.logger.info(f"Loading Avro messages from topic: {topic}")
        
        # Get schema with validation
        schema = self._get_schema_with_validation(schema_file, schema_id, topic, validate_evolution)
        
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
    
    def _get_schema_with_validation(self, schema_file: Optional[str], schema_id: Optional[int], 
                                   topic: str, validate_evolution: Optional[bool]) -> Any:
        """Get Avro schema with optional evolution validation."""
        
        # Determine if we should validate
        should_validate = validate_evolution if validate_evolution is not None else self.enable_schema_validation
        
        if schema_file:
            self.logger.info(f"Loading schema from file: {schema_file}")
            with open(schema_file, 'r') as f:
                schema_dict = json.load(f)
                schema_str = json.dumps(schema_dict)
                
            # Validate evolution if enabled and registry is available
            if should_validate and self.schema_validator:
                self._validate_schema_evolution(f"{topic}-value", schema_str)
                
            return fastavro.parse_schema(schema_dict)
        
        elif schema_id and self.schema_registry_client:
            self.logger.info(f"Loading schema from registry with ID: {schema_id}")
            schema = self.schema_registry_client.get_schema(schema_id)
            schema_dict = json.loads(schema.schema_str)
            
            # Registry schemas are already validated by the registry
            self.logger.info("Using registry schema - evolution already validated by Schema Registry")
            
            return fastavro.parse_schema(schema_dict)
        
        else:
            raise ValueError("Must provide either schema_file or schema_id with schema_registry_url")
    
    def _validate_schema_evolution(self, subject: str, new_schema_str: str):
        """Validate schema evolution before using the schema."""
        if not self.schema_validator:
            return
            
        self.logger.info(f"Validating schema evolution for subject: {subject}")
        
        validation_result = self.schema_validator.validate_schema_compatibility(
            subject, new_schema_str, self.compatibility_level
        )
        
        if not validation_result["compatible"]:
            error_msg = f"Schema evolution validation failed: {validation_result['reason']}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        
        self.logger.info(f"Schema evolution validation passed: {validation_result['reason']}")
        
        # Log any breaking changes for awareness
        if validation_result.get("version"):
            breaking_changes = self.schema_validator.validate_breaking_changes(
                self.schema_registry_client.get_version(subject, validation_result["version"]).schema.schema_str,
                new_schema_str
            )
            
            if breaking_changes["breaking_changes"]:
                self.logger.warning(f"Breaking changes detected: {breaking_changes['breaking_changes']}")
            if breaking_changes["safe_changes"]:
                self.logger.info(f"Safe changes detected: {breaking_changes['safe_changes']}")
    
    def get_schema_evolution_history(self, topic: str) -> List[Dict[str, Any]]:
        """Get schema evolution history for a topic."""
        if not self.schema_validator:
            raise ValueError("Schema Registry not configured - cannot get evolution history")
            
        subject = f"{topic}-value"
        return self.schema_validator.get_schema_evolution_history(subject)
    
    def _get_schema(self, schema_file: Optional[str], schema_id: Optional[int]):
        """Get Avro schema from file or Schema Registry (legacy method)."""
        # Keeping for backward compatibility
        return self._get_schema_with_validation(schema_file, schema_id, "unknown", False)
    
    def _deserialize_avro_message(self, message_value: bytes, schema) -> Dict[str, Any]:
        """Deserialize Avro binary message using schema."""
        bytes_reader = io.BytesIO(message_value)
        return fastavro.schemaless_reader(bytes_reader, schema)
    
    def handle_output(self, context, obj):
        """Not implemented - this IO manager is read-only."""
        raise NotImplementedError("AvroKafkaIOManager is read-only")


@io_manager(required_resource_keys={"kafka"})
def avro_kafka_io_manager(context) -> AvroKafkaIOManager:
    """Factory function for AvroKafkaIOManager with schema evolution validation."""
    kafka_resource = context.resources.kafka
    schema_registry_url = context.op_config.get("schema_registry_url")
    enable_validation = context.op_config.get("enable_schema_validation", True)
    compatibility_level_str = context.op_config.get("compatibility_level", "BACKWARD")
    
    # Convert string to enum
    compatibility_level = CompatibilityLevel(compatibility_level_str)
    
    return AvroKafkaIOManager(
        kafka_resource, 
        schema_registry_url,
        enable_validation,
        compatibility_level
    )