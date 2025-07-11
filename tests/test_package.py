"""Test package imports and basic functionality."""

import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from dagster_kafka import KafkaResource, KafkaIOManager, AvroKafkaIOManager, avro_kafka_io_manager


def test_package_imports():
    """Test that all package components import correctly."""
    # Test JSON components
    assert KafkaResource is not None
    assert KafkaIOManager is not None
    
    # Test Avro components  
    assert AvroKafkaIOManager is not None
    assert avro_kafka_io_manager is not None
    print("✅ All imports working!")


def test_kafka_resource_creation():
    """Test KafkaResource can be created."""
    resource = KafkaResource(bootstrap_servers="localhost:9092")
    assert resource.bootstrap_servers == "localhost:9092"


def test_avro_kafka_io_manager_creation():
    """Test AvroKafkaIOManager can be created."""
    kafka_resource = KafkaResource(bootstrap_servers="localhost:9092")
    avro_manager = AvroKafkaIOManager(kafka_resource)
    assert avro_manager.kafka_resource == kafka_resource