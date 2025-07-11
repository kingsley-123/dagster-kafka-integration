"""Tests for AvroKafkaIOManager functionality."""

import json
import tempfile
import pytest
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from unittest.mock import Mock, patch
from dagster_kafka.avro_io_manager import AvroKafkaIOManager
from dagster_kafka.resources import KafkaResource


class TestAvroKafkaIOManager:
    """Test cases for AvroKafkaIOManager."""
    
    def test_avro_io_manager_initialization(self):
        """Test AvroKafkaIOManager initializes correctly."""
        kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
        avro_manager = AvroKafkaIOManager(kafka_resource)
        
        assert avro_manager.kafka_resource == kafka_resource
        assert avro_manager.schema_registry_client is None
    
    def test_avro_io_manager_with_schema_registry(self):
        """Test AvroKafkaIOManager with schema registry URL."""
        kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
        
        with patch('dagster_kafka.avro_io_manager.SchemaRegistryClient'):
            avro_manager = AvroKafkaIOManager(
                kafka_resource, 
                schema_registry_url="http://localhost:8081"
            )
            assert avro_manager.schema_registry_client is not None
    
    def test_get_schema_from_file(self):
        """Test loading schema from local file."""
        kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
        avro_manager = AvroKafkaIOManager(kafka_resource)
        
        # Create temporary schema file
        schema = {
            "type": "record",
            "name": "TestUser",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc', delete=False) as f:
            json.dump(schema, f)
            schema_file = f.name
        
        try:
            parsed_schema = avro_manager._get_schema(schema_file, None)
            assert parsed_schema is not None
        finally:
            os.unlink(schema_file)
    
    def test_get_schema_invalid_params(self):
        """Test error handling for invalid schema parameters."""
        kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
        avro_manager = AvroKafkaIOManager(kafka_resource)
        
        with pytest.raises(ValueError, match="Must provide either schema_file or schema_id"):
            avro_manager._get_schema(None, None)
    
    def test_handle_output_not_implemented(self):
        """Test that handle_output raises NotImplementedError."""
        kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
        avro_manager = AvroKafkaIOManager(kafka_resource)
        
        with pytest.raises(NotImplementedError, match="read-only"):
            avro_manager.handle_output(None, None)
    
    @patch('dagster_kafka.avro_io_manager.fastavro.schemaless_reader')
    def test_deserialize_avro_message(self, mock_reader):
        """Test Avro message deserialization."""
        kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
        avro_manager = AvroKafkaIOManager(kafka_resource)
        
        # Mock the fastavro reader
        mock_reader.return_value = {"id": 123, "name": "test"}
        
        schema = {"type": "record", "name": "Test", "fields": []}
        result = avro_manager._deserialize_avro_message(b"test_data", schema)
        
        assert result == {"id": 123, "name": "test"}
        mock_reader.assert_called_once()


def test_avro_imports():
    """Test that Avro components import correctly."""
    from dagster_kafka import AvroKafkaIOManager, avro_kafka_io_manager
    
    assert AvroKafkaIOManager is not None
    assert avro_kafka_io_manager is not None
    print("✅ All Avro imports working!")