"""Tests for schema evolution validation functionality."""

import json
import pytest
import sys
import os
from unittest.mock import Mock, patch

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dagster_kafka.schema_evolution import SchemaEvolutionValidator, CompatibilityLevel


class TestSchemaEvolutionValidator:
    """Test cases for SchemaEvolutionValidator."""
    
    @patch('dagster_kafka.schema_evolution.SchemaRegistryClient')
    def test_validator_initialization(self, mock_client_class):
        """Test SchemaEvolutionValidator initializes correctly."""
        mock_client = Mock()
        validator = SchemaEvolutionValidator(mock_client)
        
        assert validator.client == mock_client
        assert validator.logger is not None
    
    @patch('dagster_kafka.schema_evolution.SchemaRegistryClient')
    def test_validate_schema_compatibility_first_schema(self, mock_client_class):
        """Test validation when subject doesn't exist (first schema)."""
        mock_client = Mock()
        mock_client.get_versions.return_value = []
        
        validator = SchemaEvolutionValidator(mock_client)
        result = validator.validate_schema_compatibility(
            "test-subject", 
            '{"type": "record", "name": "Test", "fields": []}'
        )
        
        assert result["compatible"] is True
        assert "first schema" in result["reason"].lower()
        assert result["version"] is None
    
    @patch('dagster_kafka.schema_evolution.SchemaRegistryClient')
    def test_validate_schema_compatibility_existing_subject(self, mock_client_class):
        """Test validation with existing subject."""
        mock_client = Mock()
        mock_client.get_versions.return_value = [1, 2]
        
        # Mock latest version
        mock_version = Mock()
        mock_version.schema_id = 123
        mock_client.get_version.return_value = mock_version
        
        # Mock compatibility test
        mock_compatibility = Mock()
        mock_compatibility.is_compatible = True
        mock_client.test_compatibility.return_value = mock_compatibility
        
        validator = SchemaEvolutionValidator(mock_client)
        result = validator.validate_schema_compatibility(
            "test-subject",
            '{"type": "record", "name": "Test", "fields": []}'
        )
        
        assert result["compatible"] is True
        assert result["version"] == 2
        assert result["latest_schema_id"] == 123
    
    def test_compatibility_levels_enum(self):
        """Test that all compatibility levels are available."""
        expected_levels = [
            "BACKWARD", "FORWARD", "FULL",
            "BACKWARD_TRANSITIVE", "FORWARD_TRANSITIVE", "FULL_TRANSITIVE",
            "NONE"
        ]
        
        actual_levels = [level.value for level in CompatibilityLevel]
        
        for level in expected_levels:
            assert level in actual_levels
    
    def test_validate_breaking_changes(self):
        """Test breaking change detection."""
        validator = SchemaEvolutionValidator(Mock())
        
        old_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "email", "type": "string"}
            ]
        }
        
        # New schema removes email field (breaking change)
        new_schema = {
            "type": "record", 
            "name": "User",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        }
        
        result = validator.validate_breaking_changes(
            json.dumps(old_schema),
            json.dumps(new_schema)
        )
        
        assert len(result["breaking_changes"]) > 0
        assert any("email" in change for change in result["breaking_changes"])
    
    def test_validate_safe_changes(self):
        """Test safe change detection."""
        validator = SchemaEvolutionValidator(Mock())
        
        old_schema = {
            "type": "record",
            "name": "User", 
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"}
            ]
        }
        
        # New schema adds field with default (safe change)
        new_schema = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "created_at", "type": "long", "default": 0}
            ]
        }
        
        result = validator.validate_breaking_changes(
            json.dumps(old_schema),
            json.dumps(new_schema) 
        )
        
        assert len(result["safe_changes"]) > 0
        assert any("created_at" in change for change in result["safe_changes"])


def test_schema_evolution_imports():
    """Test that schema evolution components import correctly."""
    from dagster_kafka import SchemaEvolutionValidator, CompatibilityLevel
    
    assert SchemaEvolutionValidator is not None
    assert CompatibilityLevel is not None
    print("✅ Schema evolution imports working!")