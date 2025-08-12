# io_manager
import json
import time
from typing import List, Dict, Any, Optional
from confluent_kafka import KafkaError
from dagster import ConfigurableIOManager, InputContext, OutputContext, ResourceDependency
from pydantic import Field
from dagster_kafka.resources import KafkaResource
from dagster_kafka.dlq import DLQConfiguration, DLQManager, DLQStrategy, ErrorType


class KafkaIOManager(ConfigurableIOManager):
    """IO Manager for reading JSON data from Kafka topics with DLQ support."""
    
    kafka_resource: ResourceDependency[KafkaResource]
    consumer_group_id: str = Field(default="dagster-consumer")
    max_messages: int = Field(default=100)
    
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
    
    def _process_message(self, msg, context: InputContext, dlq_manager: Optional[DLQManager]) -> Optional[Dict[str, Any]]:
        """Process a single message with DLQ support."""
        message_key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"
        
        try:
            # Parse JSON message
            data = json.loads(msg.value().decode("utf-8"))
            
            # Record success for circuit breaker
            if dlq_manager:
                dlq_manager.record_success()
            
            context.log.debug(f"Successfully parsed message {message_key}")
            return data
            
        except json.JSONDecodeError as e:
            context.log.warning(f"JSON decode error for message {message_key}: {e}")
            
            if dlq_manager:
                # Record failure for circuit breaker
                dlq_manager.record_failure()
                
                # Classify error
                error_type = dlq_manager.classify_error(e, msg)
                
                # Check if we should retry
                if dlq_manager.should_retry(error_type, message_key):
                    retry_count = dlq_manager.record_retry(message_key)
                    context.log.info(f"Retrying message {message_key} (attempt {retry_count})")
                    
                    # Simple backoff - wait before returning None to trigger next poll
                    time.sleep(dlq_manager.dlq_config.retry_backoff_ms / 1000.0)
                    return None  # Will be retried on next poll
                else:
                    # Send to DLQ
                    context.log.error(f"Sending message {message_key} to DLQ after {dlq_manager.dlq_config.max_retry_attempts} retries")
                    
                    additional_metadata = {
                        'dagster_run_id': context.run_id,
                        'dagster_asset_key': str(context.asset_key) if context.asset_key else None
                    }
                    
                    dlq_success = dlq_manager.send_to_dlq(
                        original_message=msg,
                        error=e,
                        consumer_group_id=self.consumer_group_id,
                        additional_metadata=additional_metadata
                    )
                    
                    if dlq_success:
                        context.log.info(f"Message {message_key} successfully sent to DLQ")
                    else:
                        context.log.error(f"Failed to send message {message_key} to DLQ - message lost!")
            
            return None  # Skip this message
            
        except Exception as e:
            context.log.error(f"Unexpected error processing message {message_key}: {e}")
            
            if dlq_manager:
                # Record failure for circuit breaker
                dlq_manager.record_failure()
                
                # Handle unexpected errors
                error_type = dlq_manager.classify_error(e, msg)
                
                if dlq_manager.should_retry(error_type, message_key):
                    retry_count = dlq_manager.record_retry(message_key)
                    context.log.info(f"Retrying message {message_key} for unexpected error (attempt {retry_count})")
                    time.sleep(dlq_manager.dlq_config.retry_backoff_ms / 1000.0)
                    return None
                else:
                    # Send to DLQ
                    context.log.error(f"Sending message {message_key} to DLQ due to unexpected error")
                    
                    additional_metadata = {
                        'dagster_run_id': context.run_id,
                        'dagster_asset_key': str(context.asset_key) if context.asset_key else None
                    }
                    
                    dlq_manager.send_to_dlq(
                        original_message=msg,
                        error=e,
                        consumer_group_id=self.consumer_group_id,
                        additional_metadata=additional_metadata
                    )
            
            return None
    
    def load_input(self, context: InputContext) -> List[Dict[str, Any]]:
        """Load JSON data from Kafka topic with DLQ support."""
        topic = context.asset_key.path[-1] if context.asset_key else "default"
        context.log.info(f"Starting Kafka consumption from topic: {topic}")
        
        # Initialize DLQ manager
        dlq_manager = self._create_dlq_manager(topic)
        if dlq_manager:
            context.log.info(f"DLQ enabled with strategy: {dlq_manager.dlq_config.strategy.value}")
            context.log.info(f"DLQ topic: {dlq_manager.dlq_topic_name}")
        
        consumer = self.kafka_resource.get_consumer(self.consumer_group_id)
        consumer.subscribe([topic])
        context.log.info("Subscribed to topic, waiting for assignment...")
        
        # Give consumer time to connect and get assignment
        time.sleep(2)
        
        messages = []
        attempts = 0
        successful_messages = 0
        failed_messages = 0
        dlq_messages = 0
        
        try:
            while len(messages) < self.max_messages and attempts < 20:
                msg = consumer.poll(timeout=2.0)
                attempts += 1
                
                if msg is None:
                    context.log.debug(f"No message on attempt {attempts}")
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        context.log.debug("Reached end of partition")
                        break
                    else:
                        context.log.error(f"Kafka error: {msg.error()}")
                        if dlq_manager:
                            dlq_manager.record_failure()
                        continue
                
                # Process message with DLQ support
                processed_data = self._process_message(msg, context, dlq_manager)
                
                if processed_data is not None:
                    messages.append(processed_data)
                    successful_messages += 1
                    context.log.info(f"Successfully processed message {len(messages)}: {processed_data}")
                else:
                    failed_messages += 1
                    # Note: DLQ counting is handled inside _process_message
            
        finally:
            consumer.close()
            
            # Log final stats
            context.log.info(f"Processing completed:")
            context.log.info(f"  ✅ Successfully processed: {successful_messages} messages")
            context.log.info(f"  ❌ Failed messages: {failed_messages}")
            
            if dlq_manager:
                dlq_stats = dlq_manager.get_dlq_stats()
                context.log.info(f"  📊 DLQ Stats: {dlq_stats}")
                dlq_manager.cleanup()
        
        context.log.info(f"Finished: loaded {len(messages)} messages from topic {topic}")
        return messages
    
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Kafka IO manager is read-only for now."""
        pass