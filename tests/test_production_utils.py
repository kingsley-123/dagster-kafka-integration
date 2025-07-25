"""Tests for production utilities."""

import pytest
import sys
import os
from unittest.mock import Mock, patch

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dagster_kafka.production_utils import (
    ProductionSchemaEvolutionManager, 
    RecoveryStrategy, 
    SchemaEvolutionMetrics,
    with_schema_evolution_monitoring
)
from dagster_kafka.schema_evolution import SchemaEvolutionValidator, CompatibilityLevel


class TestSchemaEvolutionMetrics:
    """Test the metrics tracking."""
    
    def test_metrics_initialization(self):
        """Test metrics start with zero values."""
        metrics = SchemaEvolutionMetrics()
        assert metrics.validation_attempts == 0
        assert metrics.validation_successes == 0
        assert metrics.success_rate == 0.0
        assert metrics.average_processing_time == 0.0
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        metrics = SchemaEvolutionMetrics()
        metrics.validation_attempts = 10
        metrics.validation_successes = 8
        
        assert metrics.success_rate == 80.0


class TestRecoveryStrategy:
    """Test recovery strategy enum."""
    
    def test_all_strategies_available(self):
        """Test all expected recovery strategies exist."""
        expected = ["fail_fast", "fallback_schema", "skip_validation", "retry_with_backoff", "graceful_degradation"]
        actual = [strategy.value for strategy in RecoveryStrategy]
        
        for strategy in expected:
            assert strategy in actual


class TestProductionSchemaEvolutionManager:
    """Test the production schema evolution manager."""
    
    def test_manager_initialization(self):
        """Test manager initializes with correct defaults."""
        mock_validator = Mock(spec=SchemaEvolutionValidator)
        
        manager = ProductionSchemaEvolutionManager(mock_validator)
        
        assert manager.validator == mock_validator
        assert manager.recovery_strategy == RecoveryStrategy.FALLBACK_SCHEMA
        assert manager.enable_caching is True
        assert manager.cache_ttl == 300
        assert manager.max_retries == 3
        assert isinstance(manager.metrics, SchemaEvolutionMetrics)
    
    def test_fallback_schema_registration(self):
        """Test registering fallback schemas."""
        mock_validator = Mock(spec=SchemaEvolutionValidator)
        manager = ProductionSchemaEvolutionManager(mock_validator)
        
        fallback_schemas = ["schema_v1", "schema_v2", "schema_v3"]
        manager.register_fallback_schemas("test-subject", fallback_schemas)
        
        assert "test-subject" in manager.fallback_schemas
        assert manager.fallback_schemas["test-subject"] == fallback_schemas
    
    def test_metrics_report_structure(self):
        """Test metrics report contains expected fields."""
        mock_validator = Mock(spec=SchemaEvolutionValidator)
        manager = ProductionSchemaEvolutionManager(mock_validator)
        
        report = manager.get_metrics_report()
        
        expected_fields = [
            "validation_attempts", "validation_successes", "validation_failures",
            "success_rate_percent", "fallback_used", "average_processing_time_ms",
            "total_processing_time_ms", "breaking_changes_detected", 
            "compatibility_checks", "cache_size", "registered_fallback_subjects"
        ]
        
        for field in expected_fields:
            assert field in report
    
    def test_cache_operations(self):
        """Test cache clearing functionality."""
        mock_validator = Mock(spec=SchemaEvolutionValidator)
        manager = ProductionSchemaEvolutionManager(mock_validator)
        
        # Add some dummy cache entries
        manager._schema_cache["test"] = {"data": "test"}
        manager._compatibility_cache["test"] = {"data": "test"}
        
        assert len(manager._schema_cache) == 1
        assert len(manager._compatibility_cache) == 1
        
        manager.clear_cache()
        
        assert len(manager._schema_cache) == 0
        assert len(manager._compatibility_cache) == 0


def test_monitoring_decorator():
    """Test the schema evolution monitoring decorator."""
    
    # Mock callback to track metrics
    metrics_collected = []
    def metrics_callback(metrics):
        metrics_collected.append(metrics)
    
    # Decorate a simple function
    @with_schema_evolution_monitoring(metrics_callback)
    def test_function():
        return "success"
    
    # Call the decorated function
    result = test_function()
    
    assert result == "success"
    assert len(metrics_collected) == 1
    assert metrics_collected[0]["status"] == "success"
    assert "processing_time" in metrics_collected[0]
    assert "asset_name" in metrics_collected[0]


def test_production_utils_imports():
    """Test that all production components import correctly."""
    from dagster_kafka.production_utils import (
        ProductionSchemaEvolutionManager,
        RecoveryStrategy,
        SchemaEvolutionMetrics,
        with_schema_evolution_monitoring
    )
    
    assert ProductionSchemaEvolutionManager is not None
    assert RecoveryStrategy is not None
    assert SchemaEvolutionMetrics is not None
    assert with_schema_evolution_monitoring is not None
    print("✅ All production utils imports working!")