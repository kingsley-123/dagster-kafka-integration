#!/usr/bin/env python3
"""
DLQ Dashboard - Simple operations view of DLQ health across topics
Quick status overview for production monitoring.
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from .dlq_monitor import DLQMonitor


class DLQDashboard:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.monitor = DLQMonitor(bootstrap_servers)
    
    def _get_status_icon(self, msg_count, thresholds):
        """Get status icon based on message count"""
        if msg_count == 0:
            return "✅"  # No messages - good
        elif msg_count <= thresholds.get('warning', 100):
            return "🟡"  # Some messages but manageable
        elif msg_count <= thresholds.get('critical', 1000):
            return "🟠"  # Getting high
        else:
            return "🔴"  # Critical level
    
    def _format_count(self, count):
        """Format message count for display"""
        if count >= 1000000:
            return f"{count/1000000:.1f}M"
        elif count >= 1000:
            return f"{count/1000:.1f}K"
        else:
            return str(count)
    
    def show_dashboard(self, topics, thresholds=None):
        """Display simple dashboard view"""
        if not thresholds:
            thresholds = {'warning': 100, 'critical': 1000}
        
        print("DLQ Health Dashboard")
        print("=" * 50)
        print(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        results = []
        total_messages = 0
        
        for topic in topics:
            metrics = self.monitor.monitor_topic(topic, analyze_errors=False)
            
            if 'error' in metrics:
                status_icon = "❌"
                msg_count = 0
                status = "ERROR"
                details = metrics['error']
            else:
                msg_count = metrics.get('total_messages', 0)
                status_icon = self._get_status_icon(msg_count, thresholds)
                total_messages += msg_count
                
                if msg_count == 0:
                    status = "HEALTHY"
                    details = "No messages"
                elif msg_count <= thresholds['warning']:
                    status = "OK"
                    details = f"{msg_count} messages"
                elif msg_count <= thresholds['critical']:
                    status = "WARNING"
                    details = f"{msg_count} messages"
                else:
                    status = "CRITICAL"
                    details = f"{msg_count} messages"
            
            results.append({
                'topic': topic,
                'status': status,
                'icon': status_icon,
                'count': msg_count,
                'details': details
            })
        
        # Sort by message count (highest first)
        results.sort(key=lambda x: x['count'], reverse=True)
        
        # Display results
        print(f"{'Status':<8} {'Topic':<25} {'Messages':<10} {'Details'}")
        print("-" * 55)
        
        for result in results:
            print(f"{result['icon']} {result['status']:<6} {result['topic']:<25} {self._format_count(result['count']):<10} {result['details']}")
        
        print()
        print(f"Total Messages: {self._format_count(total_messages)}")
        print(f"Topics Monitored: {len(topics)}")
        
        # Overall status
        critical_count = sum(1 for r in results if r['status'] == 'CRITICAL')
        warning_count = sum(1 for r in results if r['status'] == 'WARNING')
        error_count = sum(1 for r in results if r['status'] == 'ERROR')
        
        if critical_count > 0:
            print(f"🔴 OVERALL STATUS: CRITICAL ({critical_count} topics need immediate attention)")
        elif warning_count > 0:
            print(f"🟠 OVERALL STATUS: WARNING ({warning_count} topics need attention)")
        elif error_count > 0:
            print(f"❌ OVERALL STATUS: ERRORS ({error_count} topics have connection issues)")
        else:
            print("✅ OVERALL STATUS: HEALTHY")
        
        return results
    
    def show_compact(self, topics, thresholds=None):
        """Compact one-line status for scripts/monitoring"""
        if not thresholds:
            thresholds = {'warning': 100, 'critical': 1000}
        
        total_messages = 0
        status_counts = {'healthy': 0, 'warning': 0, 'critical': 0, 'error': 0}
        
        for topic in topics:
            metrics = self.monitor.monitor_topic(topic, analyze_errors=False)
            
            if 'error' in metrics:
                status_counts['error'] += 1
            else:
                msg_count = metrics.get('total_messages', 0)
                total_messages += msg_count
                
                if msg_count == 0:
                    status_counts['healthy'] += 1
                elif msg_count <= thresholds['warning']:
                    status_counts['healthy'] += 1
                elif msg_count <= thresholds['critical']:
                    status_counts['warning'] += 1
                else:
                    status_counts['critical'] += 1
        
        # Determine overall status
        if status_counts['critical'] > 0:
            overall_status = "CRITICAL"
        elif status_counts['warning'] > 0:
            overall_status = "WARNING"
        elif status_counts['error'] > 0:
            overall_status = "ERROR"
        else:
            overall_status = "HEALTHY"
        
        print(f"DLQ_STATUS={overall_status} TOTAL_MESSAGES={total_messages} TOPICS={len(topics)} "
              f"HEALTHY={status_counts['healthy']} WARNING={status_counts['warning']} "
              f"CRITICAL={status_counts['critical']} ERROR={status_counts['error']}")


def main():
    parser = argparse.ArgumentParser(description='DLQ Dashboard - Operations view of DLQ health')
    parser.add_argument('--topics', required=True, help='Comma-separated list of DLQ topics to monitor')
    parser.add_argument('--warning-threshold', type=int, default=100, help='Warning threshold for message count')
    parser.add_argument('--critical-threshold', type=int, default=1000, help='Critical threshold for message count')
    parser.add_argument('--compact', action='store_true', help='Compact output for monitoring systems')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka servers')
    
    args = parser.parse_args()
    
    dashboard = DLQDashboard(args.bootstrap_servers)
    
    topics = [t.strip() for t in args.topics.split(',')]
    thresholds = {
        'warning': args.warning_threshold,
        'critical': args.critical_threshold
    }
    
    if args.compact:
        dashboard.show_compact(topics, thresholds)
    else:
        dashboard.show_dashboard(topics, thresholds)
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
