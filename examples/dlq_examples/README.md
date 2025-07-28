# Dead Letter Queue (DLQ) Examples

This directory contains the complete enterprise-grade Dead Letter Queue tooling suite for the Dagster Kafka Integration.

## Overview

The DLQ tooling suite provides comprehensive management capabilities for Dead Letter Queue topics, including analysis, replay, monitoring, and alerting functionality. These tools are designed for production use with enterprise-grade safety features and monitoring capabilities.

## Tools

### DLQ Inspector

**File**: `dlq_inspector.py`

Analyzes failed messages in DLQ topics with comprehensive error pattern analysis and actionable recommendations.

**Features**:
- Comprehensive error type and retry pattern analysis
- Individual message inspection with full metadata
- Smart recommendations based on error patterns
- Flexible topic inspection (original or specific DLQ topic)
- Efficient message analysis and reporting

**Basic Usage**:

```bash
# Inspect DLQ messages for a specific topic
python dlq_inspector.py --topic user-events

# Inspect a specific DLQ topic with more messages
python dlq_inspector.py --dlq-topic user-events_dlq --max-messages 20

# Use with different Kafka cluster
python dlq_inspector.py --topic orders --kafka-servers "prod-kafka:9092"
```

**Options**:

| Option | Description | Default |
|--------|-------------|---------|
| `--topic` | Original topic name (will inspect `{topic}_dlq`) | - |
| `--dlq-topic` | Specific DLQ topic name to inspect | - |
| `--max-messages` | Maximum number of messages to analyze | 10 |
| `--kafka-servers` | Kafka bootstrap servers | localhost:9092 |

### DLQ Message Replayer

**File**: `dlq_replayer.py`

Replays failed messages back to original topics with filtering capabilities and production safety controls.

**Features**:
- Selective replay by error type, retry count, or time range
- Production safety controls (dry-run, confirmation prompts, rate limiting)
- Real-time progress tracking and detailed reporting
- Configurable message limits and batch processing
- Rate limiting to prevent system overload

**Basic Usage**:

```bash
# Replay messages with filtering and safety confirmation
python dlq_replayer.py --source-topic user-events_dlq --target-topic user-events --error-types "timeout_error" --max-messages 100 --confirm

# Replay with rate limiting for production safety  
python dlq_replayer.py --source-topic orders_dlq --target-topic orders --rate-limit 10 --dry-run

# Replay specific retry patterns
python dlq_replayer.py --source-topic payments_dlq --target-topic payments --min-retry-count 2 --max-messages 50
```

**Options**:

| Option | Description | Default |
|--------|-------------|---------|
| `--source-topic` | Source DLQ topic to read from | Required |
| `--target-topic` | Target topic to replay messages to | Required |
| `--max-messages` | Maximum number of messages to replay | Unlimited |
| `--rate-limit` | Rate limit in messages per second | None |
| `--error-types` | Comma-separated error types to replay | All |
| `--min-retry-count` | Minimum retry count to replay | None |
| `--max-retry-count` | Maximum retry count to replay | None |
| `--dry-run` | Show what would be replayed without doing it | False |
| `--confirm` | Ask for confirmation before replaying | False |

### DLQ Monitor

**File**: `dlq_monitor.py`

Core monitoring and metrics collection for DLQ health analysis with structured output for integration with monitoring systems.

**Features**:
- Structured JSON metrics for monitoring system integration
- Sample-based error pattern and retry distribution analysis
- Multi-topic monitoring capabilities
- Configurable threshold-based health assessments
- Flexible output formats (JSON/summary)

**Basic Usage**:

```bash
# Monitor DLQ health across topics
python dlq_monitor.py --topics user-events_dlq,orders_dlq,payments_dlq --output-format json

# Single topic detailed analysis
python dlq_monitor.py --topic critical-events_dlq --max-sample 200 --output-format summary

# Health check with custom thresholds
python dlq_monitor.py --topic orders_dlq --health-check --output-format summary
```

**Options**:

| Option | Description | Default |
|--------|-------------|---------|
| `--topic` | Single DLQ topic to monitor | - |
| `--topics` | Comma-separated list of DLQ topics | - |
| `--health-check` | Perform health check with thresholds | False |
| `--max-sample` | Maximum messages to sample for analysis | 100 |
| `--output-format` | Output format (json/summary) | json |

### DLQ Alert Generator

**File**: `dlq_alerts.py`

Configurable alerting system with webhook integration for proactive DLQ monitoring and incident response.

**Features**:
- Real-time alerting with configurable thresholds
- Webhook integration (Slack, PagerDuty, custom endpoints)
- Critical error type detection and alerting
- Flexible configuration via JSON files or command-line parameters
- Multi-topic monitoring and alerting

**Basic Usage**:

```bash
# Set up automated alerting with webhook
python dlq_alerts.py --topic critical-events_dlq --max-messages 500 --webhook-url https://hooks.slack.com/services/your/webhook/url

# Alert on critical error types
python dlq_alerts.py --topic user-events_dlq --critical-errors "schema_error,connection_error" --max-messages 100

# Multi-topic alerting
python dlq_alerts.py --topics user-events_dlq,orders_dlq,payments_dlq --max-messages 200 --max-retry-percent 15
```

**Options**:

| Option | Description | Default |
|--------|-------------|---------|
| `--topic` | Single DLQ topic to check | - |
| `--topics` | Comma-separated list of DLQ topics | - |
| `--max-messages` | Message count threshold | 1000 |
| `--max-retry-percent` | High retry percentage threshold | 20 |
| `--critical-errors` | Comma-separated critical error types | None |
| `--webhook-url` | Webhook URL for alert delivery | None |
| `--output-format` | Output format (json/text) | text |

### DLQ Dashboard

**File**: `dlq_dashboard.py`

Operations dashboard providing real-time DLQ health monitoring across multiple topics with clear visual status indicators.

**Features**:
- Clean operations view with status indicators
- Color-coded health status with configurable thresholds
- Multi-topic overview for comprehensive monitoring
- Automation-friendly compact output format
- Real-time status updates with current timestamps

**Basic Usage**:

```bash
# Operations dashboard view
python dlq_dashboard.py --topics user-events_dlq,orders_dlq,payments_dlq --warning-threshold 100 --critical-threshold 1000

# Compact format for automation
python dlq_dashboard.py --topics user-events_dlq,orders_dlq --compact

# Custom thresholds for different environments
python dlq_dashboard.py --topics critical-service_dlq --warning-threshold 10 --critical-threshold 50
```

**Options**:

| Option | Description | Default |
|--------|-------------|---------|
| `--topics` | Comma-separated list of DLQ topics to monitor | Required |
| `--warning-threshold` | Warning threshold for message count | 100 |
| `--critical-threshold` | Critical threshold for message count | 1000 |
| `--compact` | Compact output for monitoring systems | False |

## Supporting Tools

### Test Data Generator

**File**: `create_test_data.py`

Generates realistic test data for DLQ tool validation and testing scenarios.

```bash
# Create test messages with various error types
python create_test_data.py --topic test-events --count 10

# Generate data for specific testing scenarios
python create_test_data.py --topic stress-test --count 100
```

### Stress Test Generator

**File**: `create_stress_test.py`

Creates challenging test scenarios with edge cases for comprehensive tool testing.

```bash
# Generate stress test data with edge cases
python create_stress_test.py --topic stress-test --count 50
```

## Production Workflows

### Complete DLQ Management Workflow

```bash
# 1. Monitor DLQ health across all topics
python dlq_dashboard.py --topics user-events_dlq,orders_dlq,payments_dlq

# 2. Analyze specific issues in detail
python dlq_inspector.py --topic user-events --max-messages 50

# 3. Set up continuous monitoring and alerting
python dlq_alerts.py --topics user-events_dlq,orders_dlq --max-messages 500 --webhook-url https://alerts.company.com

# 4. Replay recovered messages back to processing
python dlq_replayer.py --source-topic user-events_dlq --target-topic user-events --error-types "timeout_error" --confirm --rate-limit 10
```

### Debugging Failed Messages

```bash
# 1. Inspect errors and patterns
python dlq_inspector.py --topic problematic-service --max-messages 20

# 2. Test replay strategy safely
python dlq_replayer.py --source-topic problematic-service_dlq --target-topic problematic-service --dry-run --max-messages 5

# 3. Replay messages after fixes
python dlq_replayer.py --source-topic problematic-service_dlq --target-topic problematic-service --error-types "connection_error" --confirm

# 4. Monitor for new issues
python dlq_monitor.py --topic problematic-service_dlq --health-check
```

## Integration Examples

### Automated Monitoring Setup

```bash
# Daily DLQ health check (cron job)
0 9 * * * cd /path/to/dlq_examples && python dlq_dashboard.py --topics production_dlq_topics --compact >> /var/log/dlq-health.log

# Continuous alerting (every 5 minutes)
*/5 * * * * cd /path/to/dlq_examples && python dlq_alerts.py --topics critical_dlq_topics --max-messages 100 --webhook-url $SLACK_WEBHOOK
```

### Production Recovery Script

```bash
#!/bin/bash
# production_dlq_recovery.sh

# Check DLQ health
echo "Checking DLQ health..."
python dlq_dashboard.py --topics $PRODUCTION_TOPICS

# Analyze issues
echo "Analyzing error patterns..."
python dlq_inspector.py --topic $TARGET_TOPIC --max-messages 100

# Confirm replay
echo "Ready to replay messages. Checking with team..."
python dlq_replayer.py --source-topic ${TARGET_TOPIC}_dlq --target-topic $TARGET_TOPIC --confirm --rate-limit 5
```

## Error Classification

The DLQ tools categorize errors into six types:

1. **DESERIALIZATION_ERROR**: Failed to deserialize message (JSON, Avro, Protobuf parsing issues)
2. **SCHEMA_ERROR**: Schema validation failed (incompatible schema versions)
3. **PROCESSING_ERROR**: Business logic errors in application code
4. **CONNECTION_ERROR**: Kafka connectivity issues
5. **TIMEOUT_ERROR**: Message processing timeouts
6. **UNKNOWN_ERROR**: Unclassified errors

## Sample Output

### DLQ Inspector

```
DLQ Inspector - Dagster Kafka Integration
============================================================
Target DLQ Topic: user-events_dlq
Max Messages: 10
Kafka Servers: localhost:9092

Inspecting DLQ topic: user-events_dlq
Analyzing up to 10 messages...
------------------------------------------------------------

DLQ ANALYSIS REPORT
============================================================
Total Messages Analyzed: 8
Analysis Time: 2025-07-28T23:49:02.030070
DLQ Topic: user-events_dlq

ERROR TYPE DISTRIBUTION:
------------------------------
  timeout_error: 3 (37.5%)
  json_parse_error: 2 (25.0%)
  schema_error: 2 (25.0%)
  connection_error: 1 (12.5%)

RETRY COUNT DISTRIBUTION:
------------------------------
  0 retries: 2 (25.0%)
  1 retries: 3 (37.5%)
  3 retries: 3 (37.5%)

RECOMMENDATIONS:
------------------------------
  High timeout errors detected:
    • Check consumer processing performance
    • Consider increasing timeout values
    • Review consumer resource allocation
```

### DLQ Dashboard

```
DLQ Health Dashboard
==================================================
Updated: 2025-07-28 13:58:56

Status   Topic                     Messages   Details
-------------------------------------------------------
WARNING  user-events_dlq           147        147 messages
OK       orders_dlq                 23         23 messages
HEALTHY  payments_dlq                0         No messages
ERROR    legacy-service_dlq          0         Topic not found

Total Messages: 170
Topics Monitored: 4
OVERALL STATUS: WARNING (1 topics need attention)
```

### DLQ Replayer

```
Replaying from 'user-events_dlq' to 'user-events'
Mode: LIVE, Max: 50
Filter: timeout_error
Rate limit: 5.0/sec
--------------------------------------------------
Replayed: timeout_error
Replayed: timeout_error
Replayed: timeout_error
--------------------------------------------------
Complete: 147 processed, 144 filtered, 3 replayed
Time: 2.1s, Rate: 70.0 messages/sec
```

## DLQ Strategy Integration

This tooling suite integrates with all Dagster-Kafka DLQ strategies:

- **DISABLED**: No DLQ processing
- **IMMEDIATE**: Messages sent to DLQ immediately on failure
- **RETRY_THEN_DLQ**: Messages retried before DLQ (visible in retry patterns)
- **CIRCUIT_BREAKER**: Circuit breaker pattern with DLQ fallback

## Prerequisites

- **Kafka Cluster**: Running Kafka instance accessible from tool execution environment
- **Python Environment**: Python 3.9+ with dagster-kafka-integration installed
- **DLQ Topics**: At least one DLQ topic with messages for meaningful analysis
- **Network Access**: Connectivity to Kafka cluster from tool execution location

## Common Use Cases

### Development and Testing

```bash
# Quick check during development
python dlq_inspector.py --topic my-test-topic --max-messages 5

# Test replay functionality
python dlq_replayer.py --source-topic test_dlq --target-topic test-replay --dry-run
```

### Production Monitoring

```bash
# Daily DLQ health check
python dlq_dashboard.py --topics production_dlq_topics --warning-threshold 500

# Monitor specific error patterns
python dlq_monitor.py --topic critical-service_dlq --max-sample 200
```

### Incident Response

```bash
# Deep dive into recent failures
python dlq_inspector.py --topic user-registrations --max-messages 50

# Replay specific error types after resolution
python dlq_replayer.py --source-topic user-registrations_dlq --target-topic user-registrations --error-types "schema_error" --confirm
```

## Troubleshooting

### No Messages Found

```
No messages found in DLQ topic!
This could mean:
• No failed messages (good!)
• DLQ topic doesn't exist yet
• All messages have been processed
```

This typically indicates healthy pipeline operation.

### Connection Issues

```
Error during inspection: ...
Troubleshooting tips:
• Check if Kafka is running
• Verify the DLQ topic exists
• Ensure correct Kafka server address
```

Common solutions:
- Verify Kafka cluster status
- Check Kafka server addresses and ports
- Confirm network connectivity
- Validate DLQ topic creation

### Permission Issues

Authentication or authorization errors typically indicate:
- Incorrect Kafka security configuration
- Missing read permissions on DLQ topics
- Invalid security credentials

## Best Practices

1. **Regular Monitoring**: Implement daily DLQ health checks in production environments
2. **Pattern Recognition**: Monitor for recurring error types indicating systemic issues
3. **Retry Analysis**: Investigate high retry counts as potential configuration problems
4. **Topic Health**: Track DLQ growth rates across different topics over time
5. **Alert Integration**: Configure alerts when DLQ message counts exceed operational thresholds
6. **Safe Replay**: Always use dry-run mode before executing production replay operations
7. **Rate Limiting**: Apply rate limiting when replaying to prevent system overload
8. **Confirmation**: Use confirmation prompts for all production replay operations

## Related Documentation

- [Main README](../../README.md) - Complete Dagster Kafka Integration documentation
- [DLQ Configuration Guide](../../README.md#dead-letter-queue-dlq-features-v090) - DLQ configuration in Dagster pipelines
- [Security Examples](../security_examples/) - Production security configuration patterns
- [Performance Examples](../performance_examples/) - Performance optimization strategies