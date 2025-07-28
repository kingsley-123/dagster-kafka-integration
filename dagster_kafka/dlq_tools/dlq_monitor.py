#!/usr/bin/env python3
"""
DLQ Monitor - Production Monitoring for Dead Letter Queues
Outputs structured metrics for integration with monitoring systems.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from collections import defaultdict, Counter
from kafka import KafkaConsumer
from kafka.structs import TopicPartition


class DLQMonitor:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
    
    def _create_consumer(self, topics=None):
        return KafkaConsumer(
            *topics if topics else [],
            bootstrap_servers=[self.bootstrap_servers],
            value_deserializer=lambda x: x,
            key_deserializer=lambda x: x if x else None,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=2000
        )
    
    def _get_topic_metrics(self, consumer, topic):
        """Get basic metrics for a single topic"""
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            return None
        
        topic_partitions = [TopicPartition(topic, p) for p in partitions]
        
        # Get partition info
        beginning_offsets = consumer.beginning_offsets(topic_partitions)
        end_offsets = consumer.end_offsets(topic_partitions)
        
        total_messages = sum(end_offsets[tp] - beginning_offsets[tp] for tp in topic_partitions)
        
        return {
            'topic': topic,
            'total_messages': total_messages,
            'partitions': len(partitions),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    def _analyze_messages(self, consumer, topic, max_sample=100):
        """Analyze a sample of messages for error patterns"""
        consumer.subscribe([topic])
        
        error_types = Counter()
        retry_counts = Counter()
        sample_count = 0
        
        for message in consumer:
            if sample_count >= max_sample:
                break
                
            headers = dict(message.headers) if message.headers else {}
            
            error_type = headers.get('error_type', b'unknown').decode('utf-8')
            error_types[error_type] += 1
            
            try:
                retry_count = int(headers.get('retry_count', b'0').decode('utf-8'))
                retry_counts[retry_count] += 1
            except (ValueError, AttributeError):
                retry_counts[0] += 1
            
            sample_count += 1
        
        return {
            'sample_size': sample_count,
            'error_types': dict(error_types.most_common(5)),  # Top 5 error types
            'retry_distribution': dict(retry_counts),
            'high_retry_messages': sum(count for retry, count in retry_counts.items() if retry >= 3)
        }
    
    def monitor_topic(self, topic, analyze_errors=True, max_sample=100):
        """Monitor a single DLQ topic"""
        consumer = None
        try:
            consumer = self._create_consumer()
            
            # Get basic metrics
            metrics = self._get_topic_metrics(consumer, topic)
            if not metrics:
                return {'error': f'Topic {topic} not found or empty'}
            
            # Add error analysis if requested and topic has messages
            if analyze_errors and metrics['total_messages'] > 0:
                consumer.close()
                consumer = self._create_consumer()
                error_analysis = self._analyze_messages(consumer, topic, max_sample)
                metrics['error_analysis'] = error_analysis
            
            return metrics
            
        except Exception as e:
            return {'error': f'Failed to monitor topic {topic}: {e}'}
        finally:
            if consumer:
                consumer.close()
    
    def monitor_multiple_topics(self, topics, analyze_errors=True, max_sample=50):
        """Monitor multiple DLQ topics"""
        results = {}
        total_messages = 0
        total_topics = 0
        
        for topic in topics:
            result = self.monitor_topic(topic, analyze_errors, max_sample)
            results[topic] = result
            
            if 'total_messages' in result:
                total_messages += result['total_messages']
                total_topics += 1
        
        # Add summary
        results['_summary'] = {
            'total_topics_monitored': total_topics,
            'total_dlq_messages': total_messages,
            'topics_with_messages': len([r for r in results.values() if isinstance(r, dict) and r.get('total_messages', 0) > 0]),
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        return results
    
    def health_check(self, topic, thresholds=None):
        """Simple health check with configurable thresholds"""
        if not thresholds:
            thresholds = {
                'max_messages': 1000,
                'max_high_retry_percent': 20
            }
        
        metrics = self.monitor_topic(topic, analyze_errors=True)
        
        if 'error' in metrics:
            return {'status': 'error', 'details': metrics['error']}
        
        health = {'status': 'healthy', 'checks': []}
        
        # Check message volume
        msg_count = metrics.get('total_messages', 0)
        if msg_count > thresholds['max_messages']:
            health['status'] = 'warning'
            health['checks'].append(f'High message count: {msg_count} > {thresholds["max_messages"]}')
        
        # Check high retry percentage
        if 'error_analysis' in metrics:
            analysis = metrics['error_analysis']
            if analysis['sample_size'] > 0:
                high_retry_percent = (analysis['high_retry_messages'] / analysis['sample_size']) * 100
                if high_retry_percent > thresholds['max_high_retry_percent']:
                    health['status'] = 'warning'
                    health['checks'].append(f'High retry rate: {high_retry_percent:.1f}% > {thresholds["max_high_retry_percent"]}%')
        
        health['metrics'] = metrics
        return health


def main():
    parser = argparse.ArgumentParser(description='DLQ Monitor - Production monitoring for Dead Letter Queues')
    parser.add_argument('--topic', help='Single DLQ topic to monitor')
    parser.add_argument('--topics', help='Comma-separated list of DLQ topics')
    parser.add_argument('--health-check', action='store_true', help='Perform health check with thresholds')
    parser.add_argument('--max-sample', type=int, default=100, help='Maximum messages to sample for analysis')
    parser.add_argument('--no-analysis', action='store_true', help='Skip error pattern analysis')
    parser.add_argument('--output-format', choices=['json', 'summary'], default='json', help='Output format')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka servers')
    
    args = parser.parse_args()
    
    monitor = DLQMonitor(args.bootstrap_servers)
    
    # Determine topics to monitor
    topics = []
    if args.topic:
        topics = [args.topic]
    elif args.topics:
        topics = [t.strip() for t in args.topics.split(',')]
    else:
        print("DLQ Monitor - Specify --topic or --topics to monitor")
        print("Example: python dlq_monitor.py --topic user-events_dlq")
        return 0
    
    # Perform monitoring
    if args.health_check and len(topics) == 1:
        result = monitor.health_check(topics[0])
    elif len(topics) == 1:
        result = monitor.monitor_topic(topics[0], not args.no_analysis, args.max_sample)
    else:
        result = monitor.monitor_multiple_topics(topics, not args.no_analysis, args.max_sample)
    
    # Output results
    if args.output_format == 'json':
        print(json.dumps(result, indent=2))
    else:
        # Summary format
        if '_summary' in result:
            summary = result['_summary']
            print(f"DLQ Summary: {summary['total_dlq_messages']} messages across {summary['total_topics_monitored']} topics")
        elif 'total_messages' in result:
            print(f"Topic {result['topic']}: {result['total_messages']} messages")
        elif 'status' in result:
            print(f"Health: {result['status']}")
            for check in result.get('checks', []):
                print(f"  - {check}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())