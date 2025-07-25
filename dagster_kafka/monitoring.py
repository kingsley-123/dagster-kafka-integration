"""
Production monitoring and alerting system for Kafka schema evolution.
Provides metrics collection, alerting, and observability.
"""

from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import time
import json
from datetime import datetime, timedelta
from dagster import get_dagster_logger


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class MetricType(Enum):
    """Types of metrics we track."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class Alert:
    """Schema evolution alert."""
    severity: AlertSeverity
    title: str
    message: str
    timestamp: datetime
    subject: str
    schema_id: Optional[int] = None
    compatibility_level: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary for serialization."""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        data['severity'] = self.severity.value
        return data


@dataclass
class Metric:
    """Schema evolution metric."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: datetime
    tags: Dict[str, str]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metric to dictionary for serialization."""
        return {
            'name': self.name,
            'value': self.value,
            'type': self.metric_type.value,
            'timestamp': self.timestamp.isoformat(),
            'tags': self.tags
        }


class SchemaEvolutionMonitor:
    """
    Production monitoring system for schema evolution operations.
    Tracks metrics, generates alerts, and provides observability.
    """
    
    def __init__(self,
                 alert_thresholds: Optional[Dict[str, Any]] = None,
                 metric_retention_hours: int = 24,
                 enable_alerts: bool = True):
        self.alert_thresholds = alert_thresholds or self._default_alert_thresholds()
        self.metric_retention_hours = metric_retention_hours
        self.enable_alerts = enable_alerts
        self.logger = get_dagster_logger()
        
        # Storage for metrics and alerts
        self.metrics: List[Metric] = []
        self.alerts: List[Alert] = []
        
        # Alert callbacks
        self.alert_callbacks: List[Callable[[Alert], None]] = []
        
        # Metric aggregations
        self.metric_aggregations: Dict[str, List[float]] = {}
        
        self.logger.info(f"Schema evolution monitoring initialized with {len(self.alert_thresholds)} alert rules")
    
    def _default_alert_thresholds(self) -> Dict[str, Any]:
        """Default alert thresholds for schema evolution."""
        return {
            "validation_failure_rate": {
                "warning": 10.0,  # 10% failure rate
                "error": 25.0,    # 25% failure rate
                "critical": 50.0  # 50% failure rate
            },
            "validation_duration": {
                "warning": 5.0,   # 5 seconds
                "error": 10.0,    # 10 seconds
                "critical": 30.0  # 30 seconds
            },
            "breaking_changes_per_hour": {
                "warning": 5,
                "error": 10,
                "critical": 20
            },
            "fallback_usage_rate": {
                "warning": 20.0,  # 20% fallback usage
                "error": 50.0,    # 50% fallback usage
                "critical": 80.0  # 80% fallback usage
            }
        }
    
    def add_alert_callback(self, callback: Callable[[Alert], None]):
        """Add callback function to handle alerts."""
        self.alert_callbacks.append(callback)
        self.logger.info(f"Added alert callback: {callback.__name__}")
    
    def record_validation_attempt(self, 
                                subject: str,
                                schema_id: Optional[int] = None,
                                compatibility_level: Optional[str] = None,
                                duration: Optional[float] = None,
                                success: bool = True,
                                breaking_changes_count: int = 0,
                                fallback_used: bool = False):
        """Record a schema validation attempt."""
        
        timestamp = datetime.now()
        tags = {
            'subject': subject,
            'compatibility_level': compatibility_level or 'unknown',
            'success': str(success).lower()
        }
        
        if schema_id:
            tags['schema_id'] = str(schema_id)
        
        # Record basic metrics
        self._record_metric('validation_attempts_total', 1.0, MetricType.COUNTER, tags)
        
        if success:
            self._record_metric('validation_successes_total', 1.0, MetricType.COUNTER, tags)
        else:
            self._record_metric('validation_failures_total', 1.0, MetricType.COUNTER, tags)
        
        if duration:
            self._record_metric('validation_duration_seconds', duration, MetricType.TIMER, tags)
        
        if breaking_changes_count > 0:
            self._record_metric('breaking_changes_detected', breaking_changes_count, MetricType.COUNTER, tags)
        
        if fallback_used:
            self._record_metric('fallback_usage_total', 1.0, MetricType.COUNTER, tags)
        
        # Check for alerts
        self._check_validation_alerts(subject, success, duration, breaking_changes_count, fallback_used)
    
    def record_schema_registry_operation(self,
                                       operation: str,
                                       subject: str,
                                       duration: float,
                                       success: bool = True,
                                       error_message: Optional[str] = None):
        """Record Schema Registry operation metrics."""
        
        tags = {
            'operation': operation,
            'subject': subject,
            'success': str(success).lower()
        }
        
        self._record_metric('schema_registry_operations_total', 1.0, MetricType.COUNTER, tags)
        self._record_metric('schema_registry_operation_duration_seconds', duration, MetricType.TIMER, tags)
        
        if not success and error_message:
            self._create_alert(
                AlertSeverity.ERROR,
                f"Schema Registry Operation Failed",
                f"Operation '{operation}' failed for subject '{subject}': {error_message}",
                subject
            )
    
    def record_consumer_metrics(self,
                              topic: str,
                              messages_consumed: int,
                              processing_duration: float,
                              deserialization_errors: int = 0):
        """Record Kafka consumer metrics."""
        
        tags = {'topic': topic}
        
        self._record_metric('messages_consumed_total', messages_consumed, MetricType.COUNTER, tags)
        self._record_metric('message_processing_duration_seconds', processing_duration, MetricType.TIMER, tags)
        
        if deserialization_errors > 0:
            self._record_metric('deserialization_errors_total', deserialization_errors, MetricType.COUNTER, tags)
            
            error_rate = (deserialization_errors / messages_consumed) * 100 if messages_consumed > 0 else 0
            if error_rate > 5.0:  # 5% error rate threshold
                self._create_alert(
                    AlertSeverity.WARNING,
                    "High Deserialization Error Rate",
                    f"Topic '{topic}' has {error_rate:.1f}% deserialization error rate ({deserialization_errors}/{messages_consumed})",
                    topic
                )
    
    def _record_metric(self, name: str, value: float, metric_type: MetricType, tags: Dict[str, str]):
        """Record a metric with automatic cleanup."""
        
        metric = Metric(
            name=name,
            value=value,
            metric_type=metric_type,
            timestamp=datetime.now(),
            tags=tags
        )
        
        self.metrics.append(metric)
        
        # Update aggregations for alert checking
        self._update_metric_aggregations(name, value)
        
        # Cleanup old metrics
        self._cleanup_old_metrics()
        
        self.logger.debug(f"Recorded metric: {name}={value} tags={tags}")
    
    def _update_metric_aggregations(self, name: str, value: float):
        """Update metric aggregations for alert checking."""
        if name not in self.metric_aggregations:
            self.metric_aggregations[name] = []
        
        self.metric_aggregations[name].append(value)
        
        # Keep only recent values (last hour)
        cutoff_time = datetime.now() - timedelta(hours=1)
        # In a real implementation, you'd track timestamps with values
        # For simplicity, we're keeping last 100 values
        if len(self.metric_aggregations[name]) > 100:
            self.metric_aggregations[name] = self.metric_aggregations[name][-100:]
    
    def _check_validation_alerts(self,
                               subject: str,
                               success: bool,
                               duration: Optional[float],
                               breaking_changes_count: int,
                               fallback_used: bool):
        """Check if validation metrics trigger any alerts."""
        
        if not self.enable_alerts:
            return
        
        # Check validation duration
        if duration and duration > self.alert_thresholds["validation_duration"]["critical"]:
            self._create_alert(
                AlertSeverity.CRITICAL,
                "Schema Validation Timeout",
                f"Validation for '{subject}' took {duration:.2f}s (threshold: {self.alert_thresholds['validation_duration']['critical']}s)",
                subject
            )
        elif duration and duration > self.alert_thresholds["validation_duration"]["warning"]:
            self._create_alert(
                AlertSeverity.WARNING,
                "Slow Schema Validation",
                f"Validation for '{subject}' took {duration:.2f}s (threshold: {self.alert_thresholds['validation_duration']['warning']}s)",
                subject
            )
        
        # Check breaking changes
        if breaking_changes_count > 0:
            severity = AlertSeverity.WARNING if breaking_changes_count < 5 else AlertSeverity.ERROR
            self._create_alert(
                severity,
                "Breaking Changes Detected",
                f"Found {breaking_changes_count} breaking changes in schema for '{subject}'",
                subject,
                metadata={'breaking_changes_count': breaking_changes_count}
            )
        
        # Check failure rates (simplified - in production you'd calculate over time windows)
        failure_metrics = self.metric_aggregations.get('validation_failures_total', [])
        success_metrics = self.metric_aggregations.get('validation_successes_total', [])
        
        if len(failure_metrics) + len(success_metrics) >= 10:  # Minimum sample size
            total_attempts = len(failure_metrics) + len(success_metrics)
            failure_rate = (len(failure_metrics) / total_attempts) * 100
            
            if failure_rate >= self.alert_thresholds["validation_failure_rate"]["critical"]:
                self._create_alert(
                    AlertSeverity.CRITICAL,
                    "Critical Validation Failure Rate",
                    f"Validation failure rate is {failure_rate:.1f}% for '{subject}'",
                    subject
                )
    
    def _create_alert(self,
                     severity: AlertSeverity,
                     title: str,
                     message: str,
                     subject: str,
                     schema_id: Optional[int] = None,
                     compatibility_level: Optional[str] = None,
                     metadata: Optional[Dict[str, Any]] = None):
        """Create and process an alert."""
        
        alert = Alert(
            severity=severity,
            title=title,
            message=message,
            timestamp=datetime.now(),
            subject=subject,
            schema_id=schema_id,
            compatibility_level=compatibility_level,
            metadata=metadata
        )
        
        self.alerts.append(alert)
        
        # Log the alert
        log_method = {
            AlertSeverity.INFO: self.logger.info,
            AlertSeverity.WARNING: self.logger.warning,
            AlertSeverity.ERROR: self.logger.error,
            AlertSeverity.CRITICAL: self.logger.error
        }[severity]
        
        log_method(f"ALERT [{severity.value.upper()}] {title}: {message}")
        
        # Call alert callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                self.logger.error(f"Alert callback failed: {e}")
    
    def _cleanup_old_metrics(self):
        """Remove metrics older than retention period."""
        cutoff_time = datetime.now() - timedelta(hours=self.metric_retention_hours)
        self.metrics = [m for m in self.metrics if m.timestamp > cutoff_time]
        
        # Also cleanup old alerts
        self.alerts = [a for a in self.alerts if a.timestamp > cutoff_time]
    
    def get_metrics_summary(self, hours: int = 1) -> Dict[str, Any]:
        """Get summary of metrics for the specified time period."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_metrics = [m for m in self.metrics if m.timestamp > cutoff_time]
        
        summary = {
            "time_window_hours": hours,
            "total_metrics": len(recent_metrics),
            "metrics_by_type": {},
            "top_subjects": {},
            "alert_counts": {
                "info": 0,
                "warning": 0,
                "error": 0,
                "critical": 0
            }
        }
        
        # Group metrics by type
        for metric in recent_metrics:
            metric_type = metric.metric_type.value
            if metric_type not in summary["metrics_by_type"]:
                summary["metrics_by_type"][metric_type] = 0
            summary["metrics_by_type"][metric_type] += 1
            
            # Track subjects
            subject = metric.tags.get('subject', 'unknown')
            if subject not in summary["top_subjects"]:
                summary["top_subjects"][subject] = 0
            summary["top_subjects"][subject] += 1
        
        # Count recent alerts
        recent_alerts = [a for a in self.alerts if a.timestamp > cutoff_time]
        for alert in recent_alerts:
            summary["alert_counts"][alert.severity.value] += 1
        
        return summary
    
    def get_recent_alerts(self, hours: int = 1, severity: Optional[AlertSeverity] = None) -> List[Alert]:
        """Get recent alerts, optionally filtered by severity."""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_alerts = [a for a in self.alerts if a.timestamp > cutoff_time]
        
        if severity:
            recent_alerts = [a for a in recent_alerts if a.severity == severity]
        
        return sorted(recent_alerts, key=lambda a: a.timestamp, reverse=True)
    
    def export_metrics(self, format: str = "json") -> str:
        """Export metrics in specified format."""
        if format == "json":
            return json.dumps([m.to_dict() for m in self.metrics], indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def export_alerts(self, format: str = "json") -> str:
        """Export alerts in specified format."""
        if format == "json":
            return json.dumps([a.to_dict() for a in self.alerts], indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")


# Production alert handlers
def slack_alert_handler(webhook_url: str) -> Callable[[Alert], None]:
    """Create a Slack alert handler."""
    
    def handler(alert: Alert):
        try:
            import requests
            
            color_map = {
                AlertSeverity.INFO: "#36a64f",      # Green
                AlertSeverity.WARNING: "#ff9f00",    # Orange  
                AlertSeverity.ERROR: "#ff0000",      # Red
                AlertSeverity.CRITICAL: "#8B0000"    # Dark red
            }
            
            payload = {
                "attachments": [{
                    "color": color_map.get(alert.severity, "#cccccc"),
                    "title": f"[{alert.severity.value.upper()}] {alert.title}",
                    "text": alert.message,
                    "fields": [
                        {"title": "Subject", "value": alert.subject, "short": True},
                        {"title": "Time", "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S"), "short": True}
                    ]
                }]
            }
            
            if alert.schema_id:
                payload["attachments"][0]["fields"].append({
                    "title": "Schema ID", "value": str(alert.schema_id), "short": True
                })
            
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
        except Exception as e:
            logger = get_dagster_logger()
            logger.error(f"Failed to send Slack alert: {e}")
    
    return handler


def email_alert_handler(smtp_config: Dict[str, Any]) -> Callable[[Alert], None]:
    """Create an email alert handler."""
    
    def handler(alert: Alert):
        try:
            import smtplib
            from email.mime.text import MIMEText
            from email.mime.multipart import MIMEMultipart
            
            msg = MIMEMultipart()
            msg['From'] = smtp_config['from_email']
            msg['To'] = smtp_config['to_email']
            msg['Subject'] = f"[{alert.severity.value.upper()}] Schema Evolution Alert: {alert.title}"
            
            body = f"""
            Alert: {alert.title}
            Severity: {alert.severity.value.upper()}
            Subject: {alert.subject}
            Time: {alert.timestamp.strftime("%Y-%m-%d %H:%M:%S")}
            
            Details:
            {alert.message}
            """
            
            if alert.metadata:
                body += f"\nMetadata: {json.dumps(alert.metadata, indent=2)}"
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(smtp_config['smtp_server'], smtp_config['smtp_port'])
            if smtp_config.get('use_tls'):
                server.starttls()
            if smtp_config.get('username'):
                server.login(smtp_config['username'], smtp_config['password'])
            
            server.send_message(msg)
            server.quit()
            
        except Exception as e:
            logger = get_dagster_logger()
            logger.error(f"Failed to send email alert: {e}")
    
    return handler