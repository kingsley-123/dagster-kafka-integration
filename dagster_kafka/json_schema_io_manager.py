"""
JSON Schema IO Manager for Kafka Integration
"""

from typing import Optional, Dict, Any, List
import json
import time
import jsonschema
from jsonschema import Draft7Validator, ValidationError
from confluent_kafka import KafkaError
from dagster import get_dagster_logger, InputContext
from pydantic import Field
from .resources import KafkaResource
from .dlq import DLQConfiguration, DLQManager, DLQStrategy, ErrorType
from .io_manager import KafkaIOManager


class JSONSchemaKafkaIOManager(KafkaIOManager):
    """IO Manager for handling JSON messages with schema validation."""
    
    # Schema Configuration
    schema_file: Optional[str] = Field(default=None, description="Path to JSON Schema file")
    schema_dict: Optional[Dict[str, Any]] = Field(default=None, description="Inline JSON Schema")
    enable_schema_validation: bool = Field(default=True, description="Enable JSON Schema validation")
    strict_validation: bool = Field(default=True, description="Fail on validation errors vs warning")
    
    def _get_schema_and_validator(self):
        """Get schema and validator (called when needed)."""
        if hasattr(self, '_schema_cache'):
            return self._schema_cache
        
        schema = None
        validator = None
        logger = get_dagster_logger()
        
        try:
            if self.schema_file:
                logger.info(f"Loading JSON Schema from file: {self.schema_file}")
                with open(self.schema_file, 'r') as f:
                    schema = json.load(f)
            elif self.schema_dict:
                logger.info("Using inline JSON Schema")
                schema = self.schema_dict
            else:
                if self.enable_schema_validation:
                    logger.warning("Schema validation enabled but no schema provided")
                
            if schema and self.enable_schema_validation:
                jsonschema.Draft7Validator.check_schema(schema)
                validator = Draft7Validator(schema)
                logger.info("JSON Schema validator initialized successfully")
                
        except Exception as e:
            logger.error(f"Failed to initialize JSON Schema: {e}")
            if self.strict_validation:
                raise
            else:
                logger.warning("Continuing without schema validation due to initialization error")
        
        # Cache the result
        self._schema_cache = (schema, validator)
        return self._schema_cache
    
    def _validate_json_schema(self, data: Dict[str, Any], context: InputContext) -> bool:
        """Validate JSON data against schema."""
        if not self.enable_schema_validation:
            return True
        
        schema, validator = self._get_schema_and_validator()
        if not validator:
            return True
        
        try:
            validator.validate(data)
            context.log.debug("JSON Schema validation passed")
            return True
            
        except ValidationError as e:
            validation_msg = f"JSON Schema validation failed: {e.message}"
            
            if self.strict_validation:
                context.log.error(validation_msg)
                raise e
            else:
                context.log.warning(f"{validation_msg} (continuing due to non-strict mode)")
                return False
                
        except Exception as e:
            context.log.error(f"Unexpected error during JSON Schema validation: {e}")
            if self.strict_validation:
                raise
            return False
    
    def _process_message(self, msg, context: InputContext, dlq_manager: Optional[DLQManager]) -> Optional[Dict[str, Any]]:
        """Process a single message with JSON Schema validation."""
        message_key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
        
        try:
            # First parse JSON (using parent logic)
            data = json.loads(msg.value().decode("utf-8"))
            context.log.debug(f"Successfully parsed JSON for message {message_key}")
            
            # Then validate against schema
            if self.enable_schema_validation:
                schema_valid = self._validate_json_schema(data, context)
                if not schema_valid and self.strict_validation:
                    raise ValidationError("JSON Schema validation failed")
            
            # Record success for circuit breaker
            if dlq_manager:
                dlq_manager.record_success()
            
            context.log.debug(f"Successfully processed message {message_key} with schema validation")
            return data
            
        except (json.JSONDecodeError, ValidationError) as e:
            context.log.warning(f"JSON/Schema error for message {message_key}: {e}")
            
            if dlq_manager:
                dlq_manager.record_failure()
                error_type = ErrorType.SCHEMA_ERROR if isinstance(e, ValidationError) else ErrorType.DESERIALIZATION_ERROR
                
                if dlq_manager.should_retry(error_type, message_key):
                    retry_count = dlq_manager.record_retry(message_key)
                    context.log.info(f"Retrying message {message_key} (attempt {retry_count})")
                    time.sleep(dlq_manager.dlq_config.retry_backoff_ms / 1000.0)
                    return None
                else:
                    context.log.error(f"Sending message {message_key} to DLQ")
                    dlq_manager.send_to_dlq(
                        original_message=msg,
                        error=e,
                        consumer_group_id=self.consumer_group_id,
                        additional_metadata={'schema_validation': True}
                    )
            
            return None
            
        except Exception as e:
            return super()._process_message(msg, context, dlq_manager)


def create_json_schema_kafka_io_manager(
    kafka_resource: KafkaResource,
    schema_file: Optional[str] = None,
    schema_dict: Optional[Dict[str, Any]] = None,
    consumer_group_id: str = "json-schema-consumer",
    max_messages: int = 100,
    enable_schema_validation: bool = True,
    strict_validation: bool = True,
    enable_dlq: bool = True,
    dlq_strategy: DLQStrategy = DLQStrategy.RETRY_THEN_DLQ,
    dlq_max_retries: int = 3
) -> JSONSchemaKafkaIOManager:
    """Create JSON Schema Kafka IO Manager with explicit parameters."""
    return JSONSchemaKafkaIOManager(
        kafka_resource=kafka_resource,
        consumer_group_id=consumer_group_id,
        max_messages=max_messages,
        schema_file=schema_file,
        schema_dict=schema_dict,
        enable_schema_validation=enable_schema_validation,
        strict_validation=strict_validation,
        enable_dlq=enable_dlq,
        dlq_strategy=dlq_strategy,
        dlq_max_retries=dlq_max_retries
    )