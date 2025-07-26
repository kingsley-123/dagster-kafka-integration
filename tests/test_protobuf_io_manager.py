"""
Tests for Protobuf IO Manager functionality.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from dagster import build_asset_context, asset, Definitions, materialize
from dagster_kafka import KafkaResource
from dagster_kafka.protobuf_io_manager import (
    ProtobufKafkaIOManager,
    SimpleProtobufKafkaIOManager,
    create_protobuf_kafka_io_manager,
    ProtobufSchemaManager
)

class TestProtobufIOManagerBasics:
    """Test basic Protobuf IO Manager functionality."""
    
    def test_imports(self):
        """Test that all Protobuf components can be imported."""
        # This should pass if our previous setup worked
        assert ProtobufKafkaIOManager is not None
        assert SimpleProtobufKafkaIOManager is not None
        assert create_protobuf_kafka_io_manager is not None
        assert ProtobufSchemaManager is not None
        print("All Protobuf imports successful!")

    def test_create_simple_manager(self):
        """Test creating a simple Protobuf IO manager."""
        kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
        manager = create_protobuf_kafka_io_manager(kafka_resource)
        
        assert isinstance(manager, SimpleProtobufKafkaIOManager)
        assert manager._kafka_resource == kafka_resource
        assert manager._schema_registry_url is None
        assert manager._consumer_group_id == "dagster-protobuf-consumer"
        print("Simple Protobuf IO Manager creation test passed!")


class TestProtobufConfigurableIOManager:
    """Test configurable Protobuf IO Manager."""
    
    def test_configurable_manager_creation(self):
        """Test creating configurable Protobuf IO Manager."""
        # Test that we can create the configurable version
        manager_class = ProtobufKafkaIOManager
        
        assert manager_class is not None
        
        # For ConfigurableIOManager, check the field annotations instead
        annotations = getattr(manager_class, '__annotations__', {})
        assert 'kafka_resource' in annotations
        assert 'schema_registry_url' in annotations  
        assert 'consumer_group_id' in annotations
        
        # Also check that it's a ConfigurableIOManager
        from dagster import ConfigurableIOManager
        assert issubclass(manager_class, ConfigurableIOManager)
        
        print("Configurable Protobuf IO Manager structure test passed!")
    
    def test_simple_manager_with_schema_registry(self):
        """Test creating simple manager with Schema Registry URL."""
        kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
        schema_registry_url = "http://localhost:8081"
        
        manager = create_protobuf_kafka_io_manager(
            kafka_resource=kafka_resource,
            schema_registry_url=schema_registry_url,
            consumer_group_id="test-consumer"
        )
        
        assert isinstance(manager, SimpleProtobufKafkaIOManager)
        assert manager._kafka_resource == kafka_resource
        assert manager._schema_registry_url == schema_registry_url
        assert manager._consumer_group_id == "test-consumer"
        print("Simple Protobuf IO Manager with Schema Registry test passed!")

class TestProtobufSchemaManager:
    """Test Protobuf Schema Manager functionality."""
    
    def test_schema_manager_creation(self):
        """Test creating ProtobufSchemaManager."""
        # Test without Schema Registry client
        manager = ProtobufSchemaManager()
        assert manager.schema_registry_client is None
        
        # Test with mock Schema Registry client
        mock_client = Mock()
        manager_with_client = ProtobufSchemaManager(schema_registry_client=mock_client)
        assert manager_with_client.schema_registry_client == mock_client
        
        print("ProtobufSchemaManager creation test passed!")

    def test_validate_protobuf_schema(self):
        """Test Protobuf schema validation."""
        from dagster_kafka.protobuf_io_manager import validate_protobuf_schema
        
        # Valid schema
        valid_schema = """
        syntax = "proto3";
        
        message TestMessage {
            string name = 1;
            int32 id = 2;
        }
        """
        assert validate_protobuf_schema(valid_schema) == True
        
        # Invalid schema (missing syntax)
        invalid_schema = """
        message TestMessage {
            string name = 1;
        }
        """
        assert validate_protobuf_schema(invalid_schema) == False
        
        print("Protobuf schema validation test passed!")

class TestProtobufLoadInput:
    """Test Protobuf IO Manager load_input functionality with mocks."""
    
    @patch('dagster_kafka.protobuf_io_manager.get_dagster_logger')
    def test_load_input_with_mock_messages(self, mock_logger):
        """Test load_input with mocked Kafka messages."""
        # Create mock Kafka resource and consumer
        mock_kafka_resource = Mock()
        mock_consumer = Mock()
        mock_kafka_resource.get_consumer.return_value = mock_consumer
        
        # Create mock messages
        mock_msg1 = Mock()
        mock_msg1.value.return_value = b'\x08\x96\x01\x12\x04test'  # Sample protobuf bytes
        mock_msg1.error.return_value = None
        mock_msg1.topic.return_value = "test-topic"
        mock_msg1.partition.return_value = 0
        mock_msg1.offset.return_value = 123
        mock_msg1.timestamp.return_value = (1, 1640995200000)  # timestamp tuple
        mock_msg1.key.return_value = b'test-key'
        
        mock_msg2 = Mock()
        mock_msg2.value.return_value = b'\x08\x97\x01\x12\x05test2'  # Another sample
        mock_msg2.error.return_value = None
        mock_msg2.topic.return_value = "test-topic"
        mock_msg2.partition.return_value = 0
        mock_msg2.offset.return_value = 124
        mock_msg2.timestamp.return_value = (1, 1640995260000)
        mock_msg2.key.return_value = b'test-key-2'
        
        # Configure consumer to return messages then None
        mock_consumer.poll.side_effect = [mock_msg1, mock_msg2, None]
        
        # Create IO manager
        manager = create_protobuf_kafka_io_manager(mock_kafka_resource)
        
        # Create mock context
        mock_context = Mock()
        mock_context.asset_key.path = ["test-topic"]
        
        # Test load_input
        result = manager.load_input(mock_context, topic="test-topic", max_messages=3)
        
        # Verify results
        assert len(result) == 2
        assert result[0]["topic"] == "test-topic"
        assert result[0]["partition"] == 0
        assert result[0]["offset"] == 123
        assert result[0]["key"] == "test-key"
        assert result[0]["value_size"] == len(b'\x08\x96\x01\x12\x04test')
        
        assert result[1]["topic"] == "test-topic"
        assert result[1]["offset"] == 124
        assert result[1]["key"] == "test-key-2"
        
        # Verify consumer was called correctly
        mock_kafka_resource.get_consumer.assert_called_once_with("dagster-protobuf-consumer")
        mock_consumer.subscribe.assert_called_once_with(["test-topic"])
        mock_consumer.close.assert_called_once()
        
        print("Protobuf load_input with mock messages test passed!")

    def test_handle_output_not_implemented(self):
        """Test that handle_output raises NotImplementedError."""
        kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
        manager = create_protobuf_kafka_io_manager(kafka_resource)
        
        mock_context = Mock()
        
        with pytest.raises(NotImplementedError, match="read-only"):
            manager.handle_output(mock_context, {"test": "data"})
        
        print("Protobuf handle_output not implemented test passed!")