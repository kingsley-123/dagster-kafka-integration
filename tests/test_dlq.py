"""
Comprehensive tests for Dead Letter Queue (DLQ) functionality.
"""

import pytest
import json
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, MagicMock, patch
from confluent_kafka import Message

from dagster_kafka.dlq import (
    DLQStrategy, ErrorType, CircuitBreakerState,
    DLQConfiguration, DLQMessage, DLQManager, create_dlq_manager
)


class TestDLQConfiguration:
    """Test DLQ configuration validation and defaults."""
    
    def test_default_configuration(self):
        """Test default DLQ configuration values."""
        config = DLQConfiguration()
        
        assert config.strategy == DLQStrategy.RETRY_THEN_DLQ
        assert config.dlq_topic_suffix == "_dlq"
        assert config.dlq_topic_prefix == ""
        assert config.max_retry_attempts == 3
        assert config.retry_backoff_ms == 1000
        assert config.circuit_breaker_failure_threshold == 5
        assert config.circuit_breaker_recovery_timeout_ms == 30000
        assert config.circuit_breaker_success_threshold == 2
        
    def test_custom_configuration(self):
        """Test custom DLQ configuration."""
        config = DLQConfiguration(
            strategy=DLQStrategy.CIRCUIT_BREAKER,
            dlq_topic_suffix="_failed",
            dlq_topic_prefix="dlq_",
            max_retry_attempts=5,
            circuit_breaker_failure_threshold=3
        )
        
        assert config.strategy == DLQStrategy.CIRCUIT_BREAKER
        assert config.dlq_topic_suffix == "_failed"
        assert config.dlq_topic_prefix == "dlq_"
        assert config.max_retry_attempts == 5
        assert config.circuit_breaker_failure_threshold == 3
    
    def test_error_classification_defaults(self):
        """Test default error classification."""
        config = DLQConfiguration()
        
        assert ErrorType.CONNECTION_ERROR in config.retry_on_errors
        assert ErrorType.TIMEOUT_ERROR in config.retry_on_errors
        assert ErrorType.DESERIALIZATION_ERROR in config.immediate_dlq_errors
        assert ErrorType.SCHEMA_ERROR in config.immediate_dlq_errors


class TestDLQMessage:
    """Test DLQ message creation and serialization."""
    
    def test_dlq_message_creation(self):
        """Test creating DLQ message with metadata."""
        failure_time = datetime.now(timezone.utc)
        
        dlq_msg = DLQMessage(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key=b"test-key",
            original_value=b'{"invalid": json}',
            original_headers={"header1": b"value1"},
            original_timestamp=1640995200000,
            error_type=ErrorType.DESERIALIZATION_ERROR,
            error_message="JSON decode error",
            error_traceback="Traceback...",
            failure_timestamp=failure_time,
            retry_count=2,
            consumer_group_id="test-group",
            processing_host="host-1",
            dagster_run_id="run-123",
            dagster_asset_key="asset-key"
        )
        
        assert dlq_msg.original_topic == "test-topic"
        assert dlq_msg.error_type == ErrorType.DESERIALIZATION_ERROR
        assert dlq_msg.retry_count == 2
        assert dlq_msg.consumer_group_id == "test-group"
    
    def test_dlq_message_to_dict(self):
        """Test DLQ message serialization to dictionary."""
        failure_time = datetime.now(timezone.utc)
        
        dlq_msg = DLQMessage(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key=b"test-key",
            original_value=b'{"test": "data"}',
            original_headers={"header1": b"value1"},
            original_timestamp=1640995200000,
            error_type=ErrorType.SCHEMA_ERROR,
            error_message="Schema validation failed",
            error_traceback=None,
            failure_timestamp=failure_time,
            retry_count=1,
            consumer_group_id="test-group"
        )
        
        data = dlq_msg.to_dict()
        
        assert data["original_message"]["topic"] == "test-topic"
        assert data["original_message"]["partition"] == 0
        assert data["original_message"]["offset"] == 123
        assert data["original_message"]["key"] == "test-key"
        assert data["error_info"]["type"] == "schema_error"
        assert data["error_info"]["message"] == "Schema validation failed"
        assert data["error_info"]["retry_count"] == 1
        assert data["processing_metadata"]["consumer_group_id"] == "test-group"
        assert data["dlq_metadata"]["dlq_version"] == "1.0"
    
    def test_dlq_message_to_json(self):
        """Test DLQ message JSON serialization."""
        failure_time = datetime.now(timezone.utc)
        
        dlq_msg = DLQMessage(
            original_topic="test-topic",
            original_partition=0,
            original_offset=123,
            original_key=None,
            original_value=b'invalid json',
            original_headers=None,
            original_timestamp=None,
            error_type=ErrorType.DESERIALIZATION_ERROR,
            error_message="Parse error",
            error_traceback="Full traceback...",
            failure_timestamp=failure_time,
            retry_count=0,
            consumer_group_id="test-group"
        )
        
        json_str = dlq_msg.to_json()
        parsed = json.loads(json_str)
        
        assert parsed["original_message"]["topic"] == "test-topic"
        assert parsed["original_message"]["key"] is None
        assert parsed["error_info"]["type"] == "deserialization_error"


class TestDLQManager:
    """Test DLQ Manager functionality."""
    
    @pytest.fixture
    def mock_kafka_resource(self):
        """Mock Kafka resource for testing."""
        mock_resource = Mock()
        mock_resource.get_producer_config.return_value = {
            "bootstrap.servers": "localhost:9092"
        }
        return mock_resource
    
    @pytest.fixture
    def dlq_config(self):
        """Default DLQ configuration for testing."""
        return DLQConfiguration(
            strategy=DLQStrategy.RETRY_THEN_DLQ,
            max_retry_attempts=3,
            retry_backoff_ms=100
        )
    
    @pytest.fixture
    def dlq_manager(self, mock_kafka_resource, dlq_config):
        """DLQ manager instance for testing."""
        return DLQManager(
            kafka_resource=mock_kafka_resource,
            dlq_config=dlq_config,
            topic_name="test-topic"
        )
    
    def test_dlq_manager_initialization(self, dlq_manager):
        """Test DLQ manager initialization."""
        assert dlq_manager.topic_name == "test-topic"
        assert dlq_manager.dlq_topic_name == "test-topic_dlq"
        assert dlq_manager._circuit_breaker_state == CircuitBreakerState.CLOSED
        assert dlq_manager._failure_count == 0
    
    def test_dlq_topic_name_generation(self, mock_kafka_resource):
        """Test DLQ topic name generation with custom prefix/suffix."""
        config = DLQConfiguration(
            dlq_topic_prefix="failed_",
            dlq_topic_suffix="_v1"
        )
        
        manager = DLQManager(
            kafka_resource=mock_kafka_resource,
            dlq_config=config,
            topic_name="orders"
        )
        
        assert manager.dlq_topic_name == "failed_orders_v1"
    
    def test_error_classification(self, dlq_manager):
        """Test error type classification."""
        # JSON/Schema errors
        json_error = ValueError("JSON decode error")
        assert dlq_manager.classify_error(json_error) == ErrorType.DESERIALIZATION_ERROR
        
        schema_error = Exception("Avro schema validation failed")
        assert dlq_manager.classify_error(schema_error) == ErrorType.SCHEMA_ERROR
        
        # Connection errors
        conn_error = Exception("Connection failed to broker")
        assert dlq_manager.classify_error(conn_error) == ErrorType.CONNECTION_ERROR
        
        # Timeout errors
        timeout_error = Exception("Request timed out")
        assert dlq_manager.classify_error(timeout_error) == ErrorType.TIMEOUT_ERROR
        
        # Processing errors
        proc_error = Exception("Processing failed")
        assert dlq_manager.classify_error(proc_error) == ErrorType.PROCESSING_ERROR
        
        # Unknown errors
        unknown_error = Exception("Something went wrong")
        assert dlq_manager.classify_error(unknown_error) == ErrorType.UNKNOWN_ERROR
    
    def test_should_retry_logic(self, dlq_manager):
        """Test retry decision logic."""
        message_key = "test:0:123"
        
        # Should retry connection errors
        assert dlq_manager.should_retry(ErrorType.CONNECTION_ERROR, message_key) is True
        
        # Should not retry deserialization errors (immediate DLQ)
        assert dlq_manager.should_retry(ErrorType.DESERIALIZATION_ERROR, message_key) is False
        
        # Should not retry schema errors (immediate DLQ)
        assert dlq_manager.should_retry(ErrorType.SCHEMA_ERROR, message_key) is False
        
        # Should not retry processing errors (not in retry_on_errors)
        assert dlq_manager.should_retry(ErrorType.PROCESSING_ERROR, message_key) is False
    
    def test_retry_count_tracking(self, dlq_manager):
        """Test retry count tracking and limits."""
        message_key = "test:0:123"
        
        # First few retries should be allowed
        for i in range(3):
            assert dlq_manager.should_retry(ErrorType.CONNECTION_ERROR, message_key) is True
            dlq_manager.record_retry(message_key)
        
        # After max retries, should not retry
        assert dlq_manager.should_retry(ErrorType.CONNECTION_ERROR, message_key) is False
    
    def test_disabled_strategy(self, mock_kafka_resource):
        """Test DISABLED strategy prevents all retries."""
        config = DLQConfiguration(strategy=DLQStrategy.DISABLED)
        manager = DLQManager(mock_kafka_resource, config, "test-topic")
        
        assert manager.should_retry(ErrorType.CONNECTION_ERROR, "test:0:123") is False
        assert manager.should_retry(ErrorType.TIMEOUT_ERROR, "test:0:456") is False
    
    def test_immediate_strategy(self, mock_kafka_resource):
        """Test IMMEDIATE strategy prevents all retries."""
        config = DLQConfiguration(strategy=DLQStrategy.IMMEDIATE)
        manager = DLQManager(mock_kafka_resource, config, "test-topic")
        
        assert manager.should_retry(ErrorType.CONNECTION_ERROR, "test:0:123") is False
        assert manager.should_retry(ErrorType.TIMEOUT_ERROR, "test:0:456") is False


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    @pytest.fixture
    def circuit_breaker_config(self):
        """Circuit breaker configuration for testing."""
        return DLQConfiguration(
            strategy=DLQStrategy.CIRCUIT_BREAKER,
            circuit_breaker_failure_threshold=3,
            circuit_breaker_recovery_timeout_ms=1000,  # 1 second for testing
            circuit_breaker_success_threshold=2
        )
    
    @pytest.fixture
    def circuit_breaker_manager(self, circuit_breaker_config):
        """DLQ manager with circuit breaker for testing."""
        mock_resource = Mock()
        mock_resource.get_producer_config.return_value = {"bootstrap.servers": "localhost:9092"}
        
        return DLQManager(
            kafka_resource=mock_resource,
            dlq_config=circuit_breaker_config,
            topic_name="test-topic"
        )
    
    def test_circuit_breaker_initial_state(self, circuit_breaker_manager):
        """Test circuit breaker starts in CLOSED state."""
        assert circuit_breaker_manager._circuit_breaker_state == CircuitBreakerState.CLOSED
        assert circuit_breaker_manager._failure_count == 0
        assert circuit_breaker_manager._success_count == 0
    
    def test_circuit_breaker_opens_after_failures(self, circuit_breaker_manager):
        """Test circuit breaker opens after threshold failures."""
        # Record failures up to threshold
        for i in range(3):
            circuit_breaker_manager.record_failure()
            if i < 2:  # Before threshold
                assert circuit_breaker_manager._circuit_breaker_state == CircuitBreakerState.CLOSED
        
        # Should be open after threshold
        assert circuit_breaker_manager._circuit_breaker_state == CircuitBreakerState.OPEN
        assert circuit_breaker_manager._circuit_opened_time is not None
    
    def test_circuit_breaker_prevents_retries_when_open(self, circuit_breaker_manager):
        """Test circuit breaker prevents retries when OPEN."""
        # Open the circuit
        for _ in range(3):
            circuit_breaker_manager.record_failure()
        
        assert circuit_breaker_manager._circuit_breaker_state == CircuitBreakerState.OPEN
        
        # Should not retry even for retryable errors
        assert circuit_breaker_manager.should_retry(ErrorType.CONNECTION_ERROR, "test:0:123") is False
        assert circuit_breaker_manager.should_retry(ErrorType.TIMEOUT_ERROR, "test:0:456") is False
    
    def test_circuit_breaker_transitions_to_half_open(self, circuit_breaker_manager):
        """Test circuit breaker transitions to HALF_OPEN after timeout."""
        # Open the circuit
        for _ in range(3):
            circuit_breaker_manager.record_failure()
        
        assert circuit_breaker_manager._circuit_breaker_state == CircuitBreakerState.OPEN
        
        # Manually set opened time to past
        circuit_breaker_manager._circuit_opened_time = datetime.now(timezone.utc) - timedelta(seconds=2)
        
        # Check state should transition to HALF_OPEN
        circuit_breaker_manager._check_circuit_breaker_state()
        assert circuit_breaker_manager._circuit_breaker_state == CircuitBreakerState.HALF_OPEN
    
    def test_circuit_breaker_closes_after_successes(self, circuit_breaker_manager):
        """Test circuit breaker closes after successful operations in HALF_OPEN."""
        # Open the circuit and transition to HALF_OPEN
        for _ in range(3):
            circuit_breaker_manager.record_failure()
        
        circuit_breaker_manager._circuit_breaker_state = CircuitBreakerState.HALF_OPEN
        circuit_breaker_manager._success_count = 0
        
        # Record successes
        circuit_breaker_manager.record_success()
        assert circuit_breaker_manager._circuit_breaker_state == CircuitBreakerState.HALF_OPEN
        
        circuit_breaker_manager.record_success()  # Second success should close
        assert circuit_breaker_manager._circuit_breaker_state == CircuitBreakerState.CLOSED
        assert circuit_breaker_manager._failure_count == 0
        assert circuit_breaker_manager._success_count == 0
    
    def test_circuit_breaker_returns_to_open_on_failure_in_half_open(self, circuit_breaker_manager):
        """Test circuit breaker returns to OPEN on failure in HALF_OPEN state."""
        # Set to HALF_OPEN state
        circuit_breaker_manager._circuit_breaker_state = CircuitBreakerState.HALF_OPEN
        circuit_breaker_manager._success_count = 1  # Had some success
        
        # Record failure
        circuit_breaker_manager.record_failure()
        
        # Should return to OPEN
        assert circuit_breaker_manager._circuit_breaker_state == CircuitBreakerState.OPEN
        assert circuit_breaker_manager._success_count == 0
        assert circuit_breaker_manager._circuit_opened_time is not None


class TestSendToDLQ:
    """Test sending messages to DLQ."""
    
    @pytest.fixture
    def mock_message(self):
        """Mock Kafka message for testing."""
        msg = Mock(spec=Message)
        msg.topic.return_value = "test-topic"
        msg.partition.return_value = 0
        msg.offset.return_value = 123
        msg.key.return_value = b"test-key"
        msg.value.return_value = b'{"invalid": json}'
        msg.headers.return_value = [("header1", b"value1")]
        msg.timestamp.return_value = (1, 1640995200000)  # (timestamp_type, timestamp)
        return msg
    
    @patch('dagster_kafka.dlq.Producer')
    def test_send_to_dlq_success(self, mock_producer_class, mock_message):
        """Test successful DLQ message sending."""
        # Setup mocks
        mock_kafka_resource = Mock()
        mock_kafka_resource.get_producer_config.return_value = {"bootstrap.servers": "localhost:9092"}
        
        dlq_config = DLQConfiguration()
        dlq_manager = DLQManager(mock_kafka_resource, dlq_config, "test-topic")
        
        # Setup mock producer
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        # Test sending to DLQ
        error = ValueError("JSON decode error")
        result = dlq_manager.send_to_dlq(
            original_message=mock_message,
            error=error,
            consumer_group_id="test-group",
            additional_metadata={"dagster_run_id": "run-123"}
        )
        
        assert result is True
        
        # Verify producer was called
        mock_producer.produce.assert_called_once()
        args, kwargs = mock_producer.produce.call_args
        
        assert kwargs["topic"] == "test-topic_dlq"
        assert kwargs["key"] == b"test-key"
        
        # Verify message content
        dlq_message_json = kwargs["value"].decode("utf-8")
        dlq_data = json.loads(dlq_message_json)
        
        assert dlq_data["original_message"]["topic"] == "test-topic"
        assert dlq_data["error_info"]["type"] == "deserialization_error"
        assert dlq_data["error_info"]["message"] == "JSON decode error"
    
    @patch('dagster_kafka.dlq.Producer')
    def test_send_to_dlq_cleans_retry_count(self, mock_producer_class, mock_message):
        """Test that successful DLQ send cleans up retry tracking."""
        # Setup mocks
        mock_kafka_resource = Mock()
        mock_kafka_resource.get_producer_config.return_value = {"bootstrap.servers": "localhost:9092"}
        
        dlq_config = DLQConfiguration()
        dlq_manager = DLQManager(mock_kafka_resource, dlq_config, "test-topic")
        
        # Setup mock producer
        mock_producer = Mock()
        mock_producer_class.return_value = mock_producer
        
        # Add retry count
        message_key = "test-topic:0:123"
        dlq_manager._retry_counts[message_key] = 2
        
        # Send to DLQ
        error = ValueError("Test error")
        dlq_manager.send_to_dlq(mock_message, error, "test-group")
        
        # Retry count should be cleaned up
        assert message_key not in dlq_manager._retry_counts


class TestCreateDLQManager:
    """Test DLQ manager factory function."""
    
    def test_create_dlq_manager_defaults(self):
        """Test factory function with default parameters."""
        mock_kafka_resource = Mock()
        
        manager = create_dlq_manager(
            kafka_resource=mock_kafka_resource,
            topic_name="test-topic"
        )
        
        assert isinstance(manager, DLQManager)
        assert manager.topic_name == "test-topic"
        assert manager.dlq_config.strategy == DLQStrategy.RETRY_THEN_DLQ
        assert manager.dlq_config.max_retry_attempts == 3
    
    def test_create_dlq_manager_custom_params(self):
        """Test factory function with custom parameters."""
        mock_kafka_resource = Mock()
        
        manager = create_dlq_manager(
            kafka_resource=mock_kafka_resource,
            topic_name="orders",
            dlq_strategy=DLQStrategy.CIRCUIT_BREAKER,
            max_retries=5
        )
        
        assert manager.topic_name == "orders"
        assert manager.dlq_config.strategy == DLQStrategy.CIRCUIT_BREAKER
        assert manager.dlq_config.max_retry_attempts == 5


class TestDLQStats:
    """Test DLQ statistics and monitoring."""
    
    def test_dlq_stats_basic(self):
        """Test basic DLQ statistics."""
        mock_kafka_resource = Mock()
        mock_kafka_resource.get_producer_config.return_value = {"bootstrap.servers": "localhost:9092"}
        
        dlq_config = DLQConfiguration()
        dlq_manager = DLQManager(mock_kafka_resource, dlq_config, "test-topic")
        
        stats = dlq_manager.get_dlq_stats()
        
        assert stats["dlq_topic"] == "test-topic_dlq"
        assert stats["active_retries"] == 0
        assert stats["dlq_strategy"] == "retry_then_dlq"
        assert stats["max_retry_attempts"] == 3
        assert "circuit_breaker" in stats
        assert stats["circuit_breaker"]["state"] == "closed"
    
    def test_dlq_stats_with_retries(self):
        """Test DLQ statistics with active retries."""
        mock_kafka_resource = Mock()
        mock_kafka_resource.get_producer_config.return_value = {"bootstrap.servers": "localhost:9092"}
        
        dlq_config = DLQConfiguration()
        dlq_manager = DLQManager(mock_kafka_resource, dlq_config, "test-topic")
        
        # Add some retry counts
        dlq_manager._retry_counts["test:0:123"] = 2
        dlq_manager._retry_counts["test:0:456"] = 1
        
        stats = dlq_manager.get_dlq_stats()
        
        assert stats["active_retries"] == 2
        assert "test:0:123" in stats["retry_counts"]
        assert stats["retry_counts"]["test:0:123"] == 2
    
    def test_dlq_stats_circuit_breaker(self):
        """Test circuit breaker statistics."""
        mock_kafka_resource = Mock()
        mock_kafka_resource.get_producer_config.return_value = {"bootstrap.servers": "localhost:9092"}
        
        dlq_config = DLQConfiguration()
        dlq_manager = DLQManager(mock_kafka_resource, dlq_config, "test-topic")
        
        # Simulate some failures
        dlq_manager._failure_count = 2
        dlq_manager._success_count = 1
        
        stats = dlq_manager.get_dlq_stats()
        
        assert stats["circuit_breaker"]["failure_count"] == 2
        assert stats["circuit_breaker"]["success_count"] == 1
        assert stats["circuit_breaker"]["state"] == "closed"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])