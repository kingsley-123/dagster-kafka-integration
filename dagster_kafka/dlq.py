"""
Dead Letter Queue (DLQ) support for Kafka integration.

Provides enterprise-grade error handling with automatic routing of failed messages
to dead letter topics for debugging and reprocessing.
"""

from typing import Dict, Any, Optional, List, Union, Callable
from enum import Enum
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
import json
import traceback
from confluent_kafka import Producer, Consumer, Message
from dagster import get_dagster_logger
from pydantic import BaseModel, Field


class DLQStrategy(str, Enum):
    """Dead Letter Queue strategies for handling failed messages."""
    DISABLED = "disabled"                    # No DLQ processing
    IMMEDIATE = "immediate"                  # Send to DLQ immediately on failure
    RETRY_THEN_DLQ = "retry_then_dlq"      # Retry N times, then DLQ
    CIRCUIT_BREAKER = "circuit_breaker"     # Circuit breaker pattern with DLQ


class ErrorType(str, Enum):
    """Classification of error types for DLQ routing."""
    DESERIALIZATION_ERROR = "deserialization_error"    # Failed to deserialize message
    SCHEMA_ERROR = "schema_error"                       # Schema validation failed
    PROCESSING_ERROR = "processing_error"               # Business logic error
    CONNECTION_ERROR = "connection_error"               # Kafka connection issues
    TIMEOUT_ERROR = "timeout_error"                     # Message processing timeout
    UNKNOWN_ERROR = "unknown_error"                     # Unclassified errors


class CircuitBreakerState(str, Enum):
    """Circuit breaker states for CIRCUIT_BREAKER strategy."""
    CLOSED = "closed"        # Normal operation - allowing all requests
    OPEN = "open"           # Failing fast - rejecting all requests
    HALF_OPEN = "half_open" # Testing recovery - allowing limited requests


@dataclass
class DLQMessage:
    """Enriched message for Dead Letter Queue with error metadata."""
    
    # Original message data
    original_topic: str
    original_partition: int
    original_offset: int
    original_key: Optional[bytes]
    original_value: Optional[bytes]
    original_headers: Optional[Dict[str, bytes]]
    original_timestamp: Optional[int]
    
    # Error information
    error_type: ErrorType
    error_message: str
    error_traceback: Optional[str]
    failure_timestamp: datetime
    retry_count: int
    
    # Processing metadata
    consumer_group_id: str
    processing_host: Optional[str] = None
    dagster_run_id: Optional[str] = None
    dagster_asset_key: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert DLQ message to dictionary for JSON serialization."""
        return {
            "original_message": {
                "topic": self.original_topic,
                "partition": self.original_partition,
                "offset": self.original_offset,
                "key": self.original_key.decode('utf-8') if self.original_key else None,
                "value_hex": self.original_value.hex() if self.original_value else None,
                "headers": {k: v.decode('utf-8') if isinstance(v, bytes) else str(v) 
                           for k, v in (self.original_headers or {}).items()},
                "timestamp": self.original_timestamp
            },
            "error_info": {
                "type": self.error_type.value,
                "message": self.error_message,
                "traceback": self.error_traceback,
                "failure_timestamp": self.failure_timestamp.isoformat(),
                "retry_count": self.retry_count
            },
            "processing_metadata": {
                "consumer_group_id": self.consumer_group_id,
                "processing_host": self.processing_host,
                "dagster_run_id": self.dagster_run_id,
                "dagster_asset_key": self.dagster_asset_key
            },
            "dlq_metadata": {
                "dlq_version": "1.0",
                "created_by": "dagster-kafka-integration"
            }
        }
    
    def to_json(self) -> str:
        """Convert DLQ message to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class DLQConfiguration(BaseModel):
    """Configuration for Dead Letter Queue handling."""
    
    # DLQ Strategy
    strategy: DLQStrategy = Field(
        default=DLQStrategy.RETRY_THEN_DLQ,
        description="DLQ strategy for handling failed messages"
    )
    
    # Topic Configuration
    dlq_topic_suffix: str = Field(
        default="_dlq",
        description="Suffix to append to original topic name for DLQ topic"
    )
    
    dlq_topic_prefix: str = Field(
        default="",
        description="Prefix to add to DLQ topic names"
    )
    
    # Retry Configuration
    max_retry_attempts: int = Field(
        default=3,
        description="Maximum number of retry attempts before sending to DLQ"
    )
    
    retry_backoff_ms: int = Field(
        default=1000,
        description="Backoff time between retry attempts in milliseconds"
    )
    
    # Circuit Breaker Configuration
    circuit_breaker_failure_threshold: int = Field(
        default=5,
        description="Number of failures needed to open the circuit breaker"
    )
    
    circuit_breaker_recovery_timeout_ms: int = Field(
        default=30000,
        description="Time to wait before testing recovery (30 seconds)"
    )
    
    circuit_breaker_success_threshold: int = Field(
        default=2,
        description="Number of successes needed to close circuit in half-open state"
    )
    
    # Error Classification
    retry_on_errors: List[ErrorType] = Field(
        default=[ErrorType.CONNECTION_ERROR, ErrorType.TIMEOUT_ERROR],
        description="Error types that should trigger retries"
    )
    
    immediate_dlq_errors: List[ErrorType] = Field(
        default=[ErrorType.DESERIALIZATION_ERROR, ErrorType.SCHEMA_ERROR],
        description="Error types that should go directly to DLQ without retries"
    )
    
    # DLQ Topic Configuration
    dlq_topic_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "num_partitions": 1,
            "replication_factor": 1,
            "config": {
                "retention.ms": 604800000,  # 7 days
                "cleanup.policy": "delete"
            }
        },
        description="Configuration for DLQ topic creation"
    )
    
    # Monitoring
    enable_dlq_metrics: bool = Field(
        default=True,
        description="Enable DLQ metrics collection"
    )
    
    # Advanced Options
    include_original_headers: bool = Field(
        default=True,
        description="Include original message headers in DLQ message"
    )
    
    include_stack_trace: bool = Field(
        default=True,
        description="Include full stack trace in DLQ message"
    )
    
    custom_error_handler: Optional[Callable[[Exception, Message], ErrorType]] = Field(
        default=None,
        description="Custom function to classify error types"
    )


class DLQManager:
    """Manager for Dead Letter Queue operations with full circuit breaker support."""
    
    def __init__(self, 
                 kafka_resource,
                 dlq_config: DLQConfiguration,
                 topic_name: str):
        """
        Initialize DLQ Manager.
        
        Args:
            kafka_resource: KafkaResource instance
            dlq_config: DLQ configuration
            topic_name: Original topic name
        """
        self.kafka_resource = kafka_resource
        self.dlq_config = dlq_config
        self.topic_name = topic_name
        self.logger = get_dagster_logger()
        self.dlq_topic_name = self._generate_dlq_topic_name()
        
        # Initialize producer for DLQ messages
        self._producer = None
        self._retry_counts: Dict[str, int] = {}
        
        # Circuit breaker state
        self._circuit_breaker_state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._circuit_opened_time: Optional[datetime] = None
        
        self.logger.info(f"DLQ Manager initialized for topic {topic_name} with strategy {dlq_config.strategy.value}")
        
    def _generate_dlq_topic_name(self) -> str:
        """Generate DLQ topic name based on configuration."""
        dlq_name = f"{self.dlq_config.dlq_topic_prefix}{self.topic_name}{self.dlq_config.dlq_topic_suffix}"
        self.logger.info(f"Generated DLQ topic name: {dlq_name}")
        return dlq_name
    
    def _get_producer(self) -> Producer:
        """Get or create Kafka producer for DLQ messages."""
        if self._producer is None:
            producer_config = self.kafka_resource.get_producer_config()
            producer_config.update({
                "acks": "all",
                "retries": 3,
                "retry.backoff.ms": 1000
            })
            self._producer = Producer(producer_config)
            self.logger.info("Created DLQ producer")
        return self._producer
    
    def _check_circuit_breaker_state(self):
        """Check and update circuit breaker state based on current conditions."""
        if self.dlq_config.strategy != DLQStrategy.CIRCUIT_BREAKER:
            return
        
        current_time = datetime.now(timezone.utc)
        
        # Handle OPEN -> HALF_OPEN transition
        if (self._circuit_breaker_state == CircuitBreakerState.OPEN and 
            self._circuit_opened_time and
            current_time - self._circuit_opened_time >= timedelta(milliseconds=self.dlq_config.circuit_breaker_recovery_timeout_ms)):
            
            self._circuit_breaker_state = CircuitBreakerState.HALF_OPEN
            self._success_count = 0
            self.logger.info(f"Circuit breaker transitioned to HALF_OPEN for topic {self.topic_name}")
    
    def record_success(self):
        """Record a successful processing operation for circuit breaker."""
        if self.dlq_config.strategy != DLQStrategy.CIRCUIT_BREAKER:
            return
        
        if self._circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.dlq_config.circuit_breaker_success_threshold:
                # Close the circuit
                self._circuit_breaker_state = CircuitBreakerState.CLOSED
                self._failure_count = 0
                self._success_count = 0
                self._circuit_opened_time = None
                self.logger.info(f"Circuit breaker CLOSED for topic {self.topic_name} after {self._success_count} successes")
    
    def record_failure(self):
        """Record a failed processing operation for circuit breaker."""
        if self.dlq_config.strategy != DLQStrategy.CIRCUIT_BREAKER:
            return
        
        if self._circuit_breaker_state == CircuitBreakerState.CLOSED:
            self._failure_count += 1
            if self._failure_count >= self.dlq_config.circuit_breaker_failure_threshold:
                # Open the circuit
                self._circuit_breaker_state = CircuitBreakerState.OPEN
                self._circuit_opened_time = datetime.now(timezone.utc)
                self.logger.warning(f"Circuit breaker OPENED for topic {self.topic_name} after {self._failure_count} failures")
        
        elif self._circuit_breaker_state == CircuitBreakerState.HALF_OPEN:
            # Failure in half-open state - go back to open
            self._circuit_breaker_state = CircuitBreakerState.OPEN
            self._circuit_opened_time = datetime.now(timezone.utc)
            self._success_count = 0
            self.logger.warning(f"Circuit breaker returned to OPEN from HALF_OPEN for topic {self.topic_name}")
    
    def classify_error(self, error: Exception, message: Optional[Message] = None) -> ErrorType:
        """
        Classify error type for appropriate DLQ handling.
        
        Args:
            error: The exception that occurred
            message: Original Kafka message (if available)
            
        Returns:
            ErrorType classification
        """
        # Use custom error handler if provided
        if self.dlq_config.custom_error_handler and message:
            try:
                return self.dlq_config.custom_error_handler(error, message)
            except Exception as e:
                self.logger.warning(f"Custom error handler failed: {e}")
        
        # Default error classification logic
        error_str = str(error).lower()
        
        # FIXED: Check deserialization errors first (more specific)
        if any(keyword in error_str for keyword in ['deserialize', 'decode', 'parse']):
            return ErrorType.DESERIALIZATION_ERROR
        
        # Schema and validation errors (check after deserialization)
        if any(keyword in error_str for keyword in ['schema', 'avro', 'protobuf', 'json']):
            return ErrorType.SCHEMA_ERROR
        
        # Connection errors
        if any(keyword in error_str for keyword in ['connection', 'network', 'broker']):
            return ErrorType.CONNECTION_ERROR
        
        # Timeout errors
        if any(keyword in error_str for keyword in ['timeout', 'timed out']):
            return ErrorType.TIMEOUT_ERROR
        
        # Processing errors (business logic)
        if 'processing' in error_str:
            return ErrorType.PROCESSING_ERROR
        
        return ErrorType.UNKNOWN_ERROR
    
    def should_retry(self, error_type: ErrorType, message_key: str) -> bool:
        """
        Determine if message should be retried based on error type and retry count.
        
        Args:
            error_type: Type of error that occurred
            message_key: Unique key for tracking retry count
            
        Returns:
            True if message should be retried
        """
        # Check circuit breaker state first
        self._check_circuit_breaker_state()
        
        if self.dlq_config.strategy == DLQStrategy.DISABLED:
            return False
        
        if self.dlq_config.strategy == DLQStrategy.IMMEDIATE:
            return False
        
        # Circuit breaker logic
        if self.dlq_config.strategy == DLQStrategy.CIRCUIT_BREAKER:
            if self._circuit_breaker_state == CircuitBreakerState.OPEN:
                self.logger.info(f"Circuit breaker OPEN - not retrying message {message_key}")
                return False
        
        # Check if error type allows retries
        if error_type in self.dlq_config.immediate_dlq_errors:
            return False
        
        if error_type not in self.dlq_config.retry_on_errors:
            return False
        
        # Check retry count
        current_retries = self._retry_counts.get(message_key, 0)
        return current_retries < self.dlq_config.max_retry_attempts
    
    def record_retry(self, message_key: str) -> int:
        """
        Record a retry attempt for a message.
        
        Args:
            message_key: Unique key for the message
            
        Returns:
            Current retry count after increment
        """
        current_count = self._retry_counts.get(message_key, 0) + 1
        self._retry_counts[message_key] = current_count
        self.logger.info(f"Retry attempt {current_count} for message {message_key}")
        return current_count
    
    def send_to_dlq(self, 
                    original_message: Message,
                    error: Exception,
                    consumer_group_id: str,
                    additional_metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Send a failed message to the Dead Letter Queue.
        
        Args:
            original_message: Original Kafka message that failed
            error: Exception that caused the failure
            consumer_group_id: Consumer group ID
            additional_metadata: Additional metadata to include
            
        Returns:
            True if message was successfully sent to DLQ
        """
        try:
            # Classify the error
            error_type = self.classify_error(error, original_message)
            
            # Generate message key for retry tracking
            message_key = f"{original_message.topic()}:{original_message.partition()}:{original_message.offset()}"
            
            # Get current retry count
            retry_count = self._retry_counts.get(message_key, 0)
            
            # Create DLQ message with rich metadata
            dlq_message = DLQMessage(
                original_topic=original_message.topic(),
                original_partition=original_message.partition(),
                original_offset=original_message.offset(),
                original_key=original_message.key(),
                original_value=original_message.value(),
                original_headers=dict(original_message.headers() or {}),
                original_timestamp=original_message.timestamp()[1] if original_message.timestamp()[0] else None,
                error_type=error_type,
                error_message=str(error),
                error_traceback=traceback.format_exc() if self.dlq_config.include_stack_trace else None,
                failure_timestamp=datetime.now(timezone.utc),
                retry_count=retry_count,
                consumer_group_id=consumer_group_id
            )
            
            # Add additional metadata if provided
            if additional_metadata:
                dlq_message.processing_host = additional_metadata.get('processing_host')
                dlq_message.dagster_run_id = additional_metadata.get('dagster_run_id')
                dlq_message.dagster_asset_key = additional_metadata.get('dagster_asset_key')
            
            # Send to DLQ topic
            producer = self._get_producer()
            
            # Use original message key if available, otherwise generate one
            dlq_key = original_message.key() or f"dlq_{message_key}".encode('utf-8')
            
            producer.produce(
                topic=self.dlq_topic_name,
                key=dlq_key,
                value=dlq_message.to_json().encode('utf-8'),
                headers={
                    'dlq-original-topic': original_message.topic().encode('utf-8'),
                    'dlq-error-type': error_type.value.encode('utf-8'),
                    'dlq-retry-count': str(retry_count).encode('utf-8'),
                    'dlq-timestamp': dlq_message.failure_timestamp.isoformat().encode('utf-8')
                }
            )
            
            # Flush to ensure delivery
            producer.flush(timeout=5.0)
            
            # Clean up retry tracking for this message
            if message_key in self._retry_counts:
                del self._retry_counts[message_key]
            
            self.logger.info(f"Successfully sent message to DLQ: {self.dlq_topic_name} "
                           f"(error_type: {error_type.value}, retry_count: {retry_count})")
            
            return True
            
        except Exception as dlq_error:
            self.logger.error(f"Failed to send message to DLQ: {dlq_error}")
            return False
    
    def get_dlq_stats(self) -> Dict[str, Any]:
        """Get statistics about DLQ operations."""
        return {
            "dlq_topic": self.dlq_topic_name,
            "active_retries": len(self._retry_counts),
            "retry_counts": dict(self._retry_counts),
            "dlq_strategy": self.dlq_config.strategy.value,
            "max_retry_attempts": self.dlq_config.max_retry_attempts,
            "circuit_breaker": {
                "state": self._circuit_breaker_state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "opened_time": self._circuit_opened_time.isoformat() if self._circuit_opened_time else None
            }
        }
    
    def cleanup(self):
        """Clean up DLQ manager resources."""
        if self._producer:
            self._producer.flush()
            self._producer = None
        self.logger.info("DLQ Manager cleaned up")


def create_dlq_manager(kafka_resource, 
                      topic_name: str,
                      dlq_strategy: DLQStrategy = DLQStrategy.RETRY_THEN_DLQ,
                      max_retries: int = 3) -> DLQManager:
    """
    Factory function to create a DLQ manager with sensible defaults.
    
    Args:
        kafka_resource: KafkaResource instance
        topic_name: Original topic name
        dlq_strategy: DLQ strategy to use
        max_retries: Maximum retry attempts
        
    Returns:
        Configured DLQManager instance
    """
    dlq_config = DLQConfiguration(
        strategy=dlq_strategy,
        max_retry_attempts=max_retries
    )
    
    return DLQManager(
        kafka_resource=kafka_resource,
        dlq_config=dlq_config,
        topic_name=topic_name
    )