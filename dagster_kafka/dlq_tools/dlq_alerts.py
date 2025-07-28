#!/usr/bin/env python3
"""
DLQ Alert Generator - Configurable alerting for DLQ monitoring
Integrates with webhook endpoints, email, or outputs structured alerts.
"""

import argparse
import json
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from .dlq_monitor import DLQMonitor


class DLQAlerter:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.monitor = DLQMonitor(bootstrap_servers)
    
    def check_thresholds(self, topic, config):
        """Check if topic metrics exceed configured thresholds"""
        metrics = self.monitor.monitor_topic(topic, analyze_errors=True)
        
        if 'error' in metrics:
            return {'topic': topic, 'status': 'error', 'message': metrics['error']}
        
        alerts = []
        msg_count = metrics.get('total_messages', 0)
        
        # Message volume alert
        if msg_count > config.get('max_messages', 1000):
            alerts.append({
                'type': 'high_volume',
                'severity': 'warning',
                'message': f'DLQ has {msg_count} messages (threshold: {config["max_messages"]})',
                'value': msg_count,
                'threshold': config['max_messages']
            })
        
        # High retry rate alert
        if 'error_analysis' in metrics:
            analysis = metrics['error_analysis']
            if analysis['sample_size'] > 0:
                high_retry_percent = (analysis['high_retry_messages'] / analysis['sample_size']) * 100
                max_retry_percent = config.get('max_high_retry_percent', 20)
                
                if high_retry_percent > max_retry_percent:
                    alerts.append({
                        'type': 'high_retry_rate',
                        'severity': 'warning',
                        'message': f'High retry rate: {high_retry_percent:.1f}% (threshold: {max_retry_percent}%)',
                        'value': high_retry_percent,
                        'threshold': max_retry_percent
                    })
        
        # Critical error detection (optional)
        if config.get('critical_errors') and 'error_analysis' in metrics:
            error_types = metrics['error_analysis']['error_types']
            critical_errors = [err for err in config['critical_errors'] if err in error_types]
            
            if critical_errors:
                alerts.append({
                    'type': 'critical_errors',
                    'severity': 'critical',
                    'message': f'Critical errors detected: {", ".join(critical_errors)}',
                    'errors': critical_errors
                })
        
        return {
            'topic': topic,
            'status': 'alert' if alerts else 'ok',
            'alerts': alerts,
            'metrics': metrics,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def send_webhook(self, alert_data, webhook_url):
        """Send alert to webhook endpoint"""
        try:
            data = json.dumps(alert_data).encode('utf-8')
            req = urllib.request.Request(
                webhook_url,
                data=data,
                headers={'Content-Type': 'application/json'}
            )
            
            with urllib.request.urlopen(req, timeout=10) as response:
                return response.status == 200
                
        except Exception as e:
            print(f"Failed to send webhook: {e}")
            return False
    
    def format_alert_message(self, result):
        """Format alert for human-readable output"""
        if result['status'] == 'ok':
            return f"✓ {result['topic']}: OK ({result['metrics']['total_messages']} messages)"
        
        lines = [f"⚠ {result['topic']}: ALERT"]
        for alert in result['alerts']:
            severity_icon = '🔴' if alert['severity'] == 'critical' else '🟡'
            lines.append(f"  {severity_icon} {alert['message']}")
        
        return '\n'.join(lines)


def load_config(config_file):
    """Load alert configuration from JSON file"""
    try:
        with open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to load config: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description='DLQ Alert Generator')
    parser.add_argument('--topic', help='Single DLQ topic to check')
    parser.add_argument('--topics', help='Comma-separated list of DLQ topics')
    parser.add_argument('--config', help='JSON config file with thresholds')
    parser.add_argument('--webhook-url', help='Webhook URL for alert delivery')
    parser.add_argument('--max-messages', type=int, default=1000, help='Message count threshold')
    parser.add_argument('--max-retry-percent', type=float, default=20, help='High retry percentage threshold')
    parser.add_argument('--critical-errors', help='Comma-separated critical error types')
    parser.add_argument('--output-format', choices=['json', 'text'], default='text', help='Output format')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka servers')
    
    args = parser.parse_args()
    
    alerter = DLQAlerter(args.bootstrap_servers)
    
    # Build configuration
    config = {
        'max_messages': args.max_messages,
        'max_high_retry_percent': args.max_retry_percent
    }
    
    if args.critical_errors:
        config['critical_errors'] = [e.strip() for e in args.critical_errors.split(',')]
    
    if args.config:
        file_config = load_config(args.config)
        if file_config:
            config.update(file_config)
    
    # Determine topics
    topics = []
    if args.topic:
        topics = [args.topic]
    elif args.topics:
        topics = [t.strip() for t in args.topics.split(',')]
    else:
        print("DLQ Alerter - Specify --topic or --topics to monitor")
        print("Example: python dlq_alerts.py --topic user-events_dlq --max-messages 500")
        return 0
    
    # Check each topic
    results = []
    for topic in topics:
        result = alerter.check_thresholds(topic, config)
        results.append(result)
        
        # Send webhook if configured and there are alerts
        if args.webhook_url and result['status'] == 'alert':
            alerter.send_webhook(result, args.webhook_url)
    
    # Output results
    if args.output_format == 'json':
        if len(results) == 1:
            print(json.dumps(results[0], indent=2))
        else:
            print(json.dumps(results, indent=2))
    else:
        for result in results:
            print(alerter.format_alert_message(result))
    
    # Exit with error code if any alerts
    has_alerts = any(r['status'] == 'alert' for r in results)
    return 1 if has_alerts else 0


if __name__ == '__main__':
    sys.exit(main())
