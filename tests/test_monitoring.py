"""Tests for monitoring and alerting system."""

import pytest
import sys
import os
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dagster_kafka.monitoring import (
    SchemaEvolutionMonitor,
    AlertSeverity,
    MetricType,
    Alert,
    Metric,
    slack_alert_handler,
    email_alert_handler
)


class TestAlert:
    """Test Alert dataclass."""
    
    def test_alert_creation(self):
        """Test alert creation with all fields."""
        timestamp = datetime.now()
        alert = Alert(
            severity=AlertSeverity.WARNING,
            title="Test Alert",
            message="This is a test",
            timestamp=timestamp,
            subject="test-subject",
            schema_id=123,
            compatibility_level="BACKWARD",
            metadata={"key": "value"}
        )
        
        assert alert.severity == AlertSeverity.WARNING
        assert alert.title == "Test Alert"
        assert alert.message == "This is a test"
        assert alert.timestamp == timestamp
        assert alert.subject == "test-subject"
        assert alert.schema_id == 123
        assert alert.compatibility_level == "BACKWARD"
        assert alert.metadata == {"key": "value"}
    
    def test_alert_to_dict(self):
        """Test alert serialization to dictionary."""
        timestamp = datetime.now()
        alert = Alert(
            severity=AlertSeverity.ERROR,
            title="Test",
            message="Test message",
            timestamp=timestamp,
            subject="test"
        )
        
        alert_dict = alert.to_dict()
        
        assert alert_dict["severity"] == "error"
        assert alert_dict["title"] == "Test"
        assert alert_dict["message"] == "Test message"
        assert alert_dict["subject"] == "test"
        assert alert_dict["timestamp"] == timestamp.isoformat()


class TestMetric:
    """Test Metric dataclass."""
    
    def test_metric_creation(self):
        """Test metric creation."""
        timestamp = datetime.now()
        tags = {"subject": "test", "operation": "validate"}
        
        metric = Metric(
            name="validation_attempts",
            value=1.0,
            metric_type=MetricType.COUNTER,
            timestamp=timestamp,
            tags=tags
        )
        
        assert metric.name == "validation_attempts"
        assert metric.value == 1.0
        assert metric.metric_type == MetricType.COUNTER
        assert metric.timestamp == timestamp
        assert metric.tags == tags
    
    def test_metric_to_dict(self):
        """Test metric serialization."""
        timestamp = datetime.now()
        metric = Metric(
            name="test_metric",
            value=42.5,
            metric_type=MetricType.GAUGE,
            timestamp=timestamp,
            tags={"env": "test"}
        )
        
        metric_dict = metric.to_dict()
        
        assert metric_dict["name"] == "test_metric"
        assert metric_dict["value"] == 42.5
        assert metric_dict["type"] == "gauge"
        assert metric_dict["timestamp"] == timestamp.isoformat()
        assert metric_dict["tags"] == {"env": "test"}


class TestSchemaEvolutionMonitor:
    """Test SchemaEvolutionMonitor."""
    
    def test_monitor_initialization(self):
        """Test monitor initializes with correct defaults."""
        monitor = SchemaEvolutionMonitor()
        
        assert monitor.enable_alerts is True
        assert monitor.metric_retention_hours == 24
        assert len(monitor.alert_thresholds) > 0
        assert isinstance(monitor.metrics, list)
        assert isinstance(monitor.alerts, list)
        assert isinstance(monitor.alert_callbacks, list)
    
    def test_custom_alert_thresholds(self):
        """Test monitor with custom alert thresholds."""
        custom_thresholds = {
            "validation_failure_rate": {
                "warning": 5.0,
                "error": 15.0,
                "critical": 30.0
            }
        }
        
        monitor = SchemaEvolutionMonitor(alert_thresholds=custom_thresholds)
        
        assert monitor.alert_thresholds == custom_thresholds
    
    def test_add_alert_callback(self):
        """Test adding alert callbacks."""
        monitor = SchemaEvolutionMonitor()
        
        def test_callback(alert):
            pass
        
        monitor.add_alert_callback(test_callback)
        
        assert len(monitor.alert_callbacks) == 1
        assert monitor.alert_callbacks[0] == test_callback
    
    def test_record_validation_attempt_success(self):
        """Test recording successful validation attempt."""
        monitor = SchemaEvolutionMonitor(enable_alerts=False)  # Disable alerts for testing
        
        monitor.record_validation_attempt(
            subject="test-subject",
            schema_id=123,
            compatibility_level="BACKWARD",
            duration=2.5,
            success=True,
            breaking_changes_count=0,
            fallback_used=False
        )
        
        # Should have recorded multiple metrics
        assert len(monitor.metrics) > 0
        
        # Check specific metrics were recorded
        metric_names = [m.name for m in monitor.metrics]
        assert "validation_attempts_total" in metric_names
        assert "validation_successes_total" in metric_names
        assert "validation_duration_seconds" in metric_names
    
    def test_record_validation_attempt_with_breaking_changes(self):
        """Test recording validation with breaking changes."""
        monitor = SchemaEvolutionMonitor(enable_alerts=False)
        
        monitor.record_validation_attempt(
            subject="test-subject",
            success=True,
            breaking_changes_count=3
        )
        
        metric_names = [m.name for m in monitor.metrics]
        assert "breaking_changes_detected" in metric_names
        
        # Find the breaking changes metric
        breaking_changes_metric = next(
            m for m in monitor.metrics 
            if m.name == "breaking_changes_detected"
        )
        assert breaking_changes_metric.value == 3
    
    def test_record_schema_registry_operation(self):
        """Test recording Schema Registry operations."""
        monitor = SchemaEvolutionMonitor(enable_alerts=False)
        
        monitor.record_schema_registry_operation(
            operation="get_schema",
            subject="test-subject",
            duration=0.5,
            success=True
        )
        
        metric_names = [m.name for m in monitor.metrics]
        assert "schema_registry_operations_total" in metric_names
        assert "schema_registry_operation_duration_seconds" in metric_names
    
    def test_record_consumer_metrics(self):
        """Test recording Kafka consumer metrics."""
        monitor = SchemaEvolutionMonitor(enable_alerts=False)
        
        monitor.record_consumer_metrics(
            topic="test-topic",
            messages_consumed=100,
            processing_duration=5.0,
            deserialization_errors=2
        )
        
        metric_names = [m.name for m in monitor.metrics]
        assert "messages_consumed_total" in metric_names
        assert "message_processing_duration_seconds" in metric_names
        assert "deserialization_errors_total" in metric_names
    
    def test_alert_creation(self):
        """Test alert creation and callback execution."""
        monitor = SchemaEvolutionMonitor()
        
        # Add a callback to capture alerts
        alerts_received = []
        def test_callback(alert):
            alerts_received.append(alert)
        
        monitor.add_alert_callback(test_callback)
        
        # Trigger an alert by recording a slow validation
        monitor.record_validation_attempt(
            subject="test-subject",
            duration=35.0,  # Above critical threshold
            success=True
        )
        
        # Should have created an alert
        assert len(monitor.alerts) > 0
        assert len(alerts_received) > 0
        
        alert = monitor.alerts[0]
        assert alert.severity == AlertSeverity.CRITICAL
        assert "timeout" in alert.title.lower()
        assert alert.subject == "test-subject"
    
    def test_metrics_summary(self):
        """Test metrics summary generation."""
        monitor = SchemaEvolutionMonitor(enable_alerts=False)
        
        # Record some metrics
        monitor.record_validation_attempt("subject1", success=True)
        monitor.record_validation_attempt("subject2", success=False)
        monitor.record_consumer_metrics("topic1", 50, 1.0)
        
        summary = monitor.get_metrics_summary(hours=1)
        
        assert "time_window_hours" in summary
        assert "total_metrics" in summary
        assert "metrics_by_type" in summary
        assert "top_subjects" in summary
        assert "alert_counts" in summary
        
        assert summary["time_window_hours"] == 1
        assert summary["total_metrics"] > 0
    
    def test_recent_alerts_filtering(self):
        """Test filtering recent alerts by severity."""
        monitor = SchemaEvolutionMonitor()
        
        # Manually create alerts with different severities
        monitor._create_alert(
            AlertSeverity.WARNING,
            "Warning Alert",
            "Test warning",
            "test-subject"
        )
        
        monitor._create_alert(
            AlertSeverity.ERROR,
            "Error Alert", 
            "Test error",
            "test-subject"
        )
        
        # Get all recent alerts
        all_alerts = monitor.get_recent_alerts(hours=1)
        assert len(all_alerts) == 2
        
        # Get only warning alerts
        warning_alerts = monitor.get_recent_alerts(hours=1, severity=AlertSeverity.WARNING)
        assert len(warning_alerts) == 1
        assert warning_alerts[0].severity == AlertSeverity.WARNING
        
        # Get only error alerts
        error_alerts = monitor.get_recent_alerts(hours=1, severity=AlertSeverity.ERROR)
        assert len(error_alerts) == 1
        assert error_alerts[0].severity == AlertSeverity.ERROR
    
    def test_export_functionality(self):
        """Test exporting metrics and alerts."""
        monitor = SchemaEvolutionMonitor(enable_alerts=False)
        
        # Record some data
        monitor.record_validation_attempt("test-subject", success=True)
        monitor._create_alert(
            AlertSeverity.INFO,
            "Test Alert",
            "Test message",
            "test-subject"
        )
        
        # Export metrics
        metrics_json = monitor.export_metrics("json")
        assert isinstance(metrics_json, str)
        assert "validation_attempts_total" in metrics_json
        
        # Export alerts
        alerts_json = monitor.export_alerts("json")
        assert isinstance(alerts_json, str)
        assert "Test Alert" in alerts_json
        
        # Test unsupported format
        with pytest.raises(ValueError, match="Unsupported export format"):
            monitor.export_metrics("xml")


def test_enums():
    """Test that all enum values are available."""
    
    # Test AlertSeverity
    expected_severities = ["info", "warning", "error", "critical"]
    actual_severities = [severity.value for severity in AlertSeverity]
    for severity in expected_severities:
        assert severity in actual_severities
    
    # Test MetricType
    expected_types = ["counter", "gauge", "histogram", "timer"]
    actual_types = [mtype.value for mtype in MetricType]
    for mtype in expected_types:
        assert mtype in actual_types


def test_alert_handlers():
    """Test alert handler functions can be created."""
    
    # Test Slack handler creation
    slack_handler = slack_alert_handler("https://hooks.slack.com/test")
    assert callable(slack_handler)
    
    # Test email handler creation
    email_config = {
        'from_email': 'test@example.com',
        'to_email': 'alerts@example.com',
        'smtp_server': 'smtp.example.com',
        'smtp_port': 587
    }
    email_handler = email_alert_handler(email_config)
    assert callable(email_handler)


def test_monitoring_imports():
    """Test that all monitoring components import correctly."""
    from dagster_kafka.monitoring import (
        SchemaEvolutionMonitor,
        AlertSeverity,
        MetricType,
        Alert,
        Metric,
        slack_alert_handler,
        email_alert_handler
    )
    
    assert SchemaEvolutionMonitor is not None
    assert AlertSeverity is not None
    assert MetricType is not None
    assert Alert is not None
    assert Metric is not None
    assert slack_alert_handler is not None
    assert email_alert_handler is not None
    print("✅ All monitoring imports working!")