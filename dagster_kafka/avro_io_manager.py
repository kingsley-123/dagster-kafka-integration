from typing import Optional, Dict, Any, List
import json
import io
import time
import fastavro
from confluent_kafka import KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from dagster import IOManager, io_manager, get_dagster_logger
from .resources import KafkaResource
from .schema_evolution import SchemaEvolutionValidator, CompatibilityLevel
from .dlq import DLQConfiguration, DLQManager, DLQStrategy, ErrorType


class AvroKafkaIOManager(IOManager):
    """IO Manager for handling Avro-serialized messages from Kafka topics with schema evolution validation and DLQ support."""
    
    def __init__(self, 
                 kafka_resource: KafkaResource, 
                 schema_registry_url: Optional[str] = None,
                 enable_schema_validation: bool = True,
                 compatibility_level: CompatibilityLevel = CompatibilityLevel.BACKWARD,
                 # DLQ Configuration
                 enable_dlq: bool = True,
                 dlq_strategy: DLQStrategy = DLQStrategy.RETRY_THEN_DLQ,
                 dlq_max_retries: int = 3,
                 dlq_circuit_breaker_failure_threshold: int = 5,
                 consumer_group_id: str = "avro-dagster-consumer"):
        self.kafka_resource = kafka_resource
        self.schema_registry_client = None
        self.schema_validator = None
        self.enable_schema_validation = enable_schema_validation
        self.compatibility_level = compatibility_level
        self.consumer_group_id = consumer_group_id
        self.logger = get_dagster_logger()
        
        # DLQ Configuration
        self.enable_dlq = enable_dlq
        self.dlq_strategy = dlq_strategy
        self.dlq_max_retries = dlq_max_retries
        self.dlq_circuit_breaker_failure_threshold = dlq_circuit_breaker_failure_threshold
        
        if schema_registry_url:
            self.schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
            self.schema_validator = SchemaEvolutionValidator(self.schema_registry_client)
            self.logger.info(f"Connected to Schema Registry at {schema_registry_url}")
            if enable_schema_validation:
                self.logger.info(f"Schema evolution validation enabled with {compatibility_level.value} compatibility")
        
        self.logger.info(f"AvroKafkaIOManager initialized with DLQ {'enabled' if enable_dlq else 'disabled'}")
    
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
    
    def _process_avro_message(self, msg, schema, dlq_manager: Optional[DLQManager], 
                             context, topic: str) -> Optional[Dict[str, Any]]:
        """Process a single Avro message with DLQ support."""
        message_key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
        
        try:
            # Deserialize Avro message
            deserialized_msg = self._deserialize_avro_message(msg.value(), schema)
            
            # Record success for circuit breaker
            if dlq_manager:
                dlq_manager.record_success()
            
            self.logger.debug(f"Successfully deserialized Avro message {message_key}")
            return deserialized_msg
            
        except Exception as e:
            self.logger.warning(f"Avro processing error for message {message_key}: {e}")
            
            if dlq_manager:
                # Record failure for circuit breaker
                dlq_manager.record_failure()
                
                # Classify error - Avro-specific classification
                error_type = self._classify_avro_error(e)
                
                # Check if we should retry
                if dlq_manager.should_retry(error_type, message_key):
                    retry_count = dlq_manager.record_retry(message_key)
                    self.logger.info(f"Retrying Avro message {message_key} (attempt {retry_count})")
                    
                    # Simple backoff
                    time.sleep(dlq_manager.dlq_config.retry_backoff_ms / 1000.0)
                    return None  # Will be retried on next poll
                else:
                    # Send to DLQ
                    self.logger.error(f"Sending Avro message {message_key} to DLQ after {dlq_manager.dlq_config.max_retry_attempts} retries")
                    
                    additional_metadata = {
                        'dagster_run_id': getattr(context, 'run_id', None),
                        'dagster_asset_key': str(getattr(context, 'asset_key', None)) if hasattr(context, 'asset_key') and context.asset_key else None,
                        'avro_topic': topic,
                        'schema_type': 'avro'
                    }
                    
                    dlq_success = dlq_manager.send_to_dlq(
                        original_message=msg,
                        error=e,
                        consumer_group_id=self.consumer_group_id,
                        additional_metadata=additional_metadata
                    )
                    
                    if dlq_success:
                        self.logger.info(f"Avro message {message_key} successfully sent to DLQ")
                    else:
                        self.logger.error(f"Failed to send Avro message {message_key} to DLQ - message lost!")
            
            return None  # Skip this message
    
    def _classify_avro_error(self, error: Exception) -> ErrorType:
        """Classify Avro-specific errors for DLQ routing."""
        error_str = str(error).lower()
        error_type_name = type(error).__name__.lower()
        
        # Avro deserialization errors
        if any(keyword in error_str for keyword in ['avro', 'deserialize', 'decode', 'parse']):
            return ErrorType.DESERIALIZATION_ERROR
        
        # Schema-related errors
        if any(keyword in error_str for keyword in ['schema', 'validation', 'compatibility']):
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
    
    def load_input(self, context, topic: str, schema_file: Optional[str] = None, 
                   schema_id: Optional[int] = None, max_messages: int = 100,
                   timeout: float = 10.0, validate_evolution: bool = None) -> List[Dict[str, Any]]:
        """
        Load Avro messages from a Kafka topic with optional schema evolution validation and DLQ support.
        
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
        
        # Initialize DLQ manager
        dlq_manager = self._create_dlq_manager(topic)
        if dlq_manager:
            self.logger.info(f"DLQ enabled for Avro topic {topic} with strategy: {dlq_manager.dlq_config.strategy.value}")
            self.logger.info(f"DLQ topic: {dlq_manager.dlq_topic_name}")
        
        try:
            # Get schema with validation
            schema = self._get_schema_with_validation(schema_file, schema_id, topic, validate_evolution, dlq_manager)
        except Exception as e:
            self.logger.error(f"Failed to get/validate Avro schema for topic {topic}: {e}")
            if dlq_manager:
                dlq_manager.record_failure()
            raise
        
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
                processed_data = self._process_avro_message(msg, schema, dlq_manager, context, topic)
                
                if processed_data is not None:
                    messages.append(processed_data)
                    successful_messages += 1
                    self.logger.debug(f"Successfully processed Avro message {len(messages)}")
                else:
                    failed_messages += 1
                    
        finally:
            consumer.close()
            
            # Log final stats
            self.logger.info(f"Avro processing completed for topic {topic}:")
            self.logger.info(f"  ✅ Successfully processed: {successful_messages} messages")
            self.logger.info(f"  ❌ Failed messages: {failed_messages}")
            
            if dlq_manager:
                dlq_stats = dlq_manager.get_dlq_stats()
                self.logger.info(f"  📊 DLQ Stats: {dlq_stats}")
                dlq_manager.cleanup()
            
        self.logger.info(f"Successfully loaded {len(messages)} Avro messages from {topic}")
        return messages
    
    def _get_schema_with_validation(self, schema_file: Optional[str], schema_id: Optional[int], 
                                   topic: str, validate_evolution: Optional[bool], 
                                   dlq_manager: Optional[DLQManager]) -> Any:
        """Get Avro schema with optional evolution validation and DLQ error handling."""
        
        # Determine if we should validate
        should_validate = validate_evolution if validate_evolution is not None else self.enable_schema_validation
        
        try:
            if schema_file:
                self.logger.info(f"Loading Avro schema from file: {schema_file}")
                with open(schema_file, 'r') as f:
                    schema_dict = json.load(f)
                    schema_str = json.dumps(schema_dict)
                    
                # Validate evolution if enabled and registry is available
                if should_validate and self.schema_validator:
                    self._validate_schema_evolution(f"{topic}-value", schema_str, dlq_manager)
                    
                return fastavro.parse_schema(schema_dict)
            
            elif schema_id and self.schema_registry_client:
                self.logger.info(f"Loading Avro schema from registry with ID: {schema_id}")
                schema = self.schema_registry_client.get_schema(schema_id)
                schema_dict = json.loads(schema.schema_str)
                
                # Registry schemas are already validated by the registry
                self.logger.info("Using registry schema - evolution already validated by Schema Registry")
                
                return fastavro.parse_schema(schema_dict)
            
            else:
                raise ValueError("Must provide either schema_file or schema_id with schema_registry_url")
                
        except Exception as e:
            self.logger.error(f"Schema loading/validation failed: {e}")
            if dlq_manager:
                dlq_manager.record_failure()
            raise
    
    def _validate_schema_evolution(self, subject: str, new_schema_str: str, dlq_manager: Optional[DLQManager]):
        """Validate schema evolution before using the schema with DLQ error handling."""
        if not self.schema_validator:
            return
            
        self.logger.info(f"Validating Avro schema evolution for subject: {subject}")
        
        try:
            validation_result = self.schema_validator.validate_schema_compatibility(
                subject, new_schema_str, self.compatibility_level
            )
            
            if not validation_result["compatible"]:
                error_msg = f"Avro schema evolution validation failed: {validation_result['reason']}"
                self.logger.error(error_msg)
                if dlq_manager:
                    dlq_manager.record_failure()
                raise ValueError(error_msg)
            
            self.logger.info(f"Avro schema evolution validation passed: {validation_result['reason']}")
            
            # Log any breaking changes for awareness
            if validation_result.get("version"):
                breaking_changes = self.schema_validator.validate_breaking_changes(
                    self.schema_registry_client.get_version(subject, validation_result["version"]).schema.schema_str,
                    new_schema_str
                )
                
                if breaking_changes["breaking_changes"]:
                    self.logger.warning(f"Breaking changes detected in Avro schema: {breaking_changes['breaking_changes']}")
                if breaking_changes["safe_changes"]:
                    self.logger.info(f"Safe changes detected in Avro schema: {breaking_changes['safe_changes']}")
            
            # Record success for circuit breaker
            if dlq_manager:
                dlq_manager.record_success()
                
        except Exception as e:
            self.logger.error(f"Avro schema evolution validation error: {e}")
            if dlq_manager:
                dlq_manager.record_failure()
            raise
    
    def get_schema_evolution_history(self, topic: str) -> List[Dict[str, Any]]:
        """Get schema evolution history for a topic."""
        if not self.schema_validator:
            raise ValueError("Schema Registry not configured - cannot get evolution history")
            
        subject = f"{topic}-value"
        return self.schema_validator.get_schema_evolution_history(subject)
    
    def _get_schema(self, schema_file: Optional[str], schema_id: Optional[int]):
        """Get Avro schema from file or Schema Registry (legacy method)."""
        # Keeping for backward compatibility
        return self._get_schema_with_validation(schema_file, schema_id, "unknown", False, None)
    
    def _deserialize_avro_message(self, message_value: bytes, schema) -> Dict[str, Any]:
        """Deserialize Avro binary message using schema."""
        bytes_reader = io.BytesIO(message_value)
        return fastavro.schemaless_reader(bytes_reader, schema)
    
    def handle_output(self, context, obj):
        """Not implemented - this IO manager is read-only."""
        raise NotImplementedError("AvroKafkaIOManager is read-only")


@io_manager(required_resource_keys={"kafka"})
def avro_kafka_io_manager(context) -> AvroKafkaIOManager:
    """Factory function for AvroKafkaIOManager with schema evolution validation and DLQ support."""
    kafka_resource = context.resources.kafka
    schema_registry_url = context.op_config.get("schema_registry_url")
    enable_validation = context.op_config.get("enable_schema_validation", True)
    compatibility_level_str = context.op_config.get("compatibility_level", "BACKWARD")
    
    # DLQ Configuration
    enable_dlq = context.op_config.get("enable_dlq", True)
    dlq_strategy_str = context.op_config.get("dlq_strategy", "retry_then_dlq")
    dlq_max_retries = context.op_config.get("dlq_max_retries", 3)
    dlq_circuit_breaker_failure_threshold = context.op_config.get("dlq_circuit_breaker_failure_threshold", 5)
    consumer_group_id = context.op_config.get("consumer_group_id", "avro-dagster-consumer")
    
    # Convert string to enum
    compatibility_level = CompatibilityLevel(compatibility_level_str)
    dlq_strategy = DLQStrategy(dlq_strategy_str)
    
    return AvroKafkaIOManager(
        kafka_resource=kafka_resource, 
        schema_registry_url=schema_registry_url,
        enable_schema_validation=enable_validation,
        compatibility_level=compatibility_level,
        enable_dlq=enable_dlq,
        dlq_strategy=dlq_strategy,
        dlq_max_retries=dlq_max_retries,
        dlq_circuit_breaker_failure_threshold=dlq_circuit_breaker_failure_threshold,
        consumer_group_id=consumer_group_id
    )