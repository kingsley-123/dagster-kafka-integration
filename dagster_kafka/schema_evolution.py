"""
Schema Evolution Validation for Avro Kafka Integration.
Ensures schema compatibility and prevents breaking changes.
"""

from typing import Optional, Dict, Any, List
from enum import Enum
import json
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from dagster import get_dagster_logger


class CompatibilityLevel(Enum):
    """Schema compatibility levels."""
    BACKWARD = "BACKWARD"
    FORWARD = "FORWARD" 
    FULL = "FULL"
    BACKWARD_TRANSITIVE = "BACKWARD_TRANSITIVE"
    FORWARD_TRANSITIVE = "FORWARD_TRANSITIVE"
    FULL_TRANSITIVE = "FULL_TRANSITIVE"
    NONE = "NONE"


class SchemaEvolutionValidator:
    """Validates schema evolution and compatibility."""
    
    def __init__(self, schema_registry_client: SchemaRegistryClient):
        self.client = schema_registry_client
        self.logger = get_dagster_logger()
    
    def validate_schema_compatibility(
        self, 
        subject: str, 
        new_schema: str,
        compatibility_level: CompatibilityLevel = CompatibilityLevel.BACKWARD
    ) -> Dict[str, Any]:
        """
        Validate if new schema is compatible with existing schemas.
        
        Args:
            subject: Schema subject name (usually topic-value)
            new_schema: New Avro schema as JSON string
            compatibility_level: Required compatibility level
            
        Returns:
            Dict with validation results and details
        """
        try:
            # Check if subject exists
            versions = self.client.get_versions(subject)
            if not versions:
                self.logger.info(f"Subject {subject} does not exist - first schema")
                return {
                    "compatible": True,
                    "reason": "First schema for subject",
                    "version": None
                }
            
            # Get latest schema
            latest_version = max(versions)
            latest_schema = self.client.get_version(subject, latest_version)
            
            # Test compatibility
            compatibility_result = self.client.test_compatibility(
                subject, 
                new_schema
            )
            
            return {
                "compatible": compatibility_result.is_compatible,
                "reason": self._get_compatibility_reason(
                    compatibility_result, 
                    compatibility_level
                ),
                "version": latest_version,
                "latest_schema_id": latest_schema.schema_id
            }
            
        except Exception as e:
            self.logger.error(f"Schema compatibility check failed: {e}")
            return {
                "compatible": False,
                "reason": f"Validation error: {str(e)}",
                "version": None
            }
    
    def get_schema_evolution_history(self, subject: str) -> List[Dict[str, Any]]:
        """Get the evolution history of a schema subject."""
        try:
            versions = self.client.get_versions(subject)
            history = []
            
            for version in versions:
                schema_version = self.client.get_version(subject, version)
                history.append({
                    "version": version,
                    "schema_id": schema_version.schema_id,
                    "schema": schema_version.schema.schema_str
                })
            
            return history
            
        except Exception as e:
            self.logger.error(f"Failed to get schema history: {e}")
            return []
    
    def _get_compatibility_reason(
        self, 
        result, 
        required_level: CompatibilityLevel
    ) -> str:
        """Generate human-readable compatibility reason."""
        if result.is_compatible:
            return f"Schema is compatible with {required_level.value} compatibility"
        else:
            return f"Schema violates {required_level.value} compatibility requirements"
    
    def validate_breaking_changes(
        self, 
        old_schema_str: str, 
        new_schema_str: str
    ) -> Dict[str, Any]:
        """
        Detect specific breaking changes between schemas.
        
        Returns detailed analysis of what changed.
        """
        try:
            old_schema = json.loads(old_schema_str)
            new_schema = json.loads(new_schema_str)
            
            changes = {
                "breaking_changes": [],
                "safe_changes": [],
                "field_changes": []
            }
            
            # Compare field changes
            old_fields = {f["name"]: f for f in old_schema.get("fields", [])}
            new_fields = {f["name"]: f for f in new_schema.get("fields", [])}
            
            # Check for removed fields (breaking change)
            for field_name in old_fields:
                if field_name not in new_fields:
                    changes["breaking_changes"].append(
                        f"Removed field: {field_name}"
                    )
            
            # Check for added fields
            for field_name in new_fields:
                if field_name not in old_fields:
                    new_field = new_fields[field_name]
                    if "default" in new_field:
                        changes["safe_changes"].append(
                            f"Added field with default: {field_name}"
                        )
                    else:
                        changes["breaking_changes"].append(
                            f"Added field without default: {field_name}"
                        )
            
            # Check for type changes
            for field_name in old_fields:
                if field_name in new_fields:
                    old_type = old_fields[field_name]["type"]
                    new_type = new_fields[field_name]["type"]
                    if old_type != new_type:
                        changes["breaking_changes"].append(
                            f"Changed type for {field_name}: {old_type} -> {new_type}"
                        )
            
            return changes
            
        except Exception as e:
            self.logger.error(f"Breaking change analysis failed: {e}")
            return {
                "breaking_changes": ["Analysis failed"],
                "safe_changes": [],
                "field_changes": []
            }