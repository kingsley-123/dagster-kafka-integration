#!/usr/bin/env python3
"""
DLQ Message Inspector Tool - FIXED VERSION

The first DLQ inspection tool for Dagster-Kafka integrations.
Provides detailed analysis of failed messages in Dead Letter Queue topics.

Usage:
    python dlq_inspector_fixed.py --topic user-events --max-messages 10
    python dlq_inspector_fixed.py --dlq-topic user-events_dlq --analyze-errors
"""

import argparse
import json
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional
from collections import defaultdict, Counter

from dagster_kafka import KafkaResource, DLQStrategy, ErrorType
from confluent_kafka import Consumer, KafkaError


class DLQInspector:
    """Inspector tool for analyzing DLQ messages and error patterns."""
    
    def __init__(self, kafka_bootstrap_servers: str = "localhost:9092"):
        """Initialize the DLQ Inspector."""
        self.kafka_resource = KafkaResource(bootstrap_servers=kafka_bootstrap_servers)
        self.consumer_group_id = f"dlq-inspector-{int(datetime.now().timestamp())}"
        
    def inspect_dlq_topic(self, dlq_topic: str, max_messages: int = 10) -> Dict[str, Any]:
        """
        Inspect messages in a DLQ topic and provide detailed analysis.
        
        Args:
            dlq_topic: Name of the DLQ topic to inspect
            max_messages: Maximum number of messages to analyze
            
        Returns:
            Dictionary containing analysis results
        """
        print(f" Inspecting DLQ topic: {dlq_topic}")
        print(f" Analyzing up to {max_messages} messages...")
        print("-" * 60)
        
        consumer = self.kafka_resource.get_consumer(self.consumer_group_id)
        consumer.subscribe([dlq_topic])
        
        messages = []
        error_stats = defaultdict(int)
        retry_stats = defaultdict(int)
        topic_stats = defaultdict(int)
        
        try:
            # Poll messages from DLQ topic
            for i in range(max_messages):
                msg = consumer.poll(timeout=5.0)
                
                if msg is None:
                    print(f" No more messages available (got {len(messages)} messages)")
                    break
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print(f" Reached end of partition (got {len(messages)} messages)")
                        break
                    else:
                        print(f" Kafka error: {msg.error()}")
                        continue
                
                # FIXED: Extract headers and message content properly
                try:
                    # Extract Kafka message headers
                    headers = {}
                    if msg.headers():
                        for key, value in msg.headers():
                            try:
                                # Try to decode header values
                                if value is not None:
                                    headers[key] = value.decode('utf-8') if isinstance(value, bytes) else str(value)
                                else:
                                    headers[key] = None
                            except UnicodeDecodeError:
                                # If decoding fails, keep as raw bytes info
                                headers[key] = f"<binary_data_{len(value)}_bytes>"
                    
                    # Extract message content
                    raw_content = None
                    parsed_content = None
                    parse_error = None
                    
                    if msg.value():
                        try:
                            raw_content = msg.value().decode('utf-8')
                            # Try to parse as JSON
                            parsed_content = json.loads(raw_content)
                        except UnicodeDecodeError:
                            raw_content = f"<binary_content_{len(msg.value())}_bytes>"
                            parse_error = "Binary content - cannot decode as UTF-8"
                        except json.JSONDecodeError as e:
                            parse_error = f"JSON parse error: {str(e)}"
                    else:
                        raw_content = "<empty_message>"
                        parse_error = "Empty message content"
                    
                    # Build message analysis
                    message_data = {
                        'kafka_metadata': {
                            'partition': msg.partition(),
                            'offset': msg.offset(),
                            'timestamp': msg.timestamp()[1] if msg.timestamp()[0] else None,
                            'key': msg.key().decode('utf-8') if msg.key() else None
                        },
                        'headers': headers,
                        'content': {
                            'raw': raw_content,
                            'parsed': parsed_content,
                            'parse_error': parse_error
                        }
                    }
                    
                    messages.append(message_data)
                    
                    # FIXED: Extract statistics from headers first, then fallback to parsed content
                    # Get error type
                    error_type = headers.get('error_type', 'unknown')
                    if error_type == 'unknown' and parsed_content:
                        # Fallback: try to get from parsed JSON
                        error_type = parsed_content.get('error_info', {}).get('type', 'unknown')
                    
                    # Get retry count
                    retry_count = headers.get('retry_count', '0')
                    try:
                        retry_count = int(retry_count)
                    except (ValueError, TypeError):
                        if parsed_content:
                            retry_count = parsed_content.get('error_info', {}).get('retry_count', 0)
                        else:
                            retry_count = 0
                    
                    # Get original topic
                    original_topic = headers.get('original_topic', 'unknown')
                    if original_topic == 'unknown' and parsed_content:
                        # Fallback: try to get from parsed JSON
                        original_topic = parsed_content.get('original_message', {}).get('topic', 'unknown')
                    
                    # Update statistics
                    error_stats[error_type] += 1
                    retry_stats[retry_count] += 1
                    topic_stats[original_topic] += 1
                    
                    print(f"  ✅ Processed message {i+1}: error_type={error_type}, retry_count={retry_count}")
                    
                except Exception as e:
                    print(f"  ❌ Failed to process message {i+1}: {e}")
                    # Still count it as processed
                    error_stats['processing_error'] += 1
                    retry_stats[0] += 1
                    topic_stats['unknown'] += 1
                    continue
                    
        finally:
            consumer.close()
        
        # Generate analysis report
        analysis = {
            'summary': {
                'total_messages': len(messages),
                'dlq_topic': dlq_topic,
                'analysis_timestamp': datetime.now().isoformat()
            },
            'error_distribution': dict(error_stats),
            'retry_distribution': dict(retry_stats),
            'original_topic_distribution': dict(topic_stats),
            'messages': messages
        }
        
        return analysis
    
    def print_analysis_report(self, analysis: Dict[str, Any]):
        """Print a formatted analysis report."""
        
        print(f"\n DLQ ANALYSIS REPORT")
        print("=" * 60)
        
        # Summary
        summary = analysis['summary']
        print(f" Total Messages Analyzed: {summary['total_messages']}")
        print(f" Analysis Time: {summary['analysis_timestamp']}")
        print(f"  DLQ Topic: {summary['dlq_topic']}")
        
        if summary['total_messages'] == 0:
            print("\n No messages found in DLQ topic!")
            print("   This could mean:")
            print("   • No failed messages (good!)")
            print("   • DLQ topic doesn't exist yet")
            print("   • All messages have been processed")
            return
        
        # Error Type Distribution
        print(f"\n ERROR TYPE DISTRIBUTION:")
        print("-" * 30)
        for error_type, count in analysis['error_distribution'].items():
            percentage = (count / summary['total_messages']) * 100
            print(f"   {error_type}: {count} ({percentage:.1f}%)")
        
        # Retry Distribution
        print(f"\n RETRY COUNT DISTRIBUTION:")
        print("-" * 30)
        for retry_count, count in analysis['retry_distribution'].items():
            percentage = (count / summary['total_messages']) * 100
            print(f"   {retry_count} retries: {count} ({percentage:.1f}%)")
        
        # Original Topic Distribution
        print(f"\n ORIGINAL TOPIC DISTRIBUTION:")
        print("-" * 30)
        for topic, count in analysis['original_topic_distribution'].items():
            percentage = (count / summary['total_messages']) * 100
            print(f"   {topic}: {count} ({percentage:.1f}%)")
        
        # Recent Messages Detail
        print(f"\n RECENT MESSAGES DETAIL:")
        print("-" * 30)
        for i, msg in enumerate(analysis['messages'][:5]):  # Show first 5 messages
            headers = msg['headers']
            content = msg['content']
            
            print(f"\n   Message #{i+1}:")
            print(f"   ├─ Original Topic: {headers.get('original_topic', 'unknown')}")
            print(f"   ├─ Error Type: {headers.get('error_type', 'unknown')}")
            print(f"   ├─ Retry Count: {headers.get('retry_count', '0')}")
            
            # Show content info
            if content['parse_error']:
                print(f"   ├─ Content Issue: {content['parse_error']}")
            
            if content['raw'] and len(content['raw']) < 200:
                print(f"   ├─ Content: {content['raw']}")
            elif content['raw']:
                print(f"   ├─ Content: {content['raw'][:100]}... ({len(content['raw'])} chars total)")
            else:
                print(f"   ├─ Content: <empty>")
            
            # Show timestamp
            kafka_meta = msg['kafka_metadata']
            if kafka_meta['timestamp']:
                timestamp = datetime.fromtimestamp(kafka_meta['timestamp'] / 1000.0)
                print(f"   └─ Failure Time: {timestamp.isoformat()}")
            else:
                print(f"   └─ Failure Time: unknown")
        
        if len(analysis['messages']) > 5:
            print(f"\n   ... and {len(analysis['messages']) - 5} more messages")
        
        # IMPROVED: Better recommendations based on actual data
        print(f"\n RECOMMENDATIONS:")
        print("-" * 30)
        
        # Analyze error patterns and provide recommendations
        error_types = analysis['error_distribution']
        total_messages = summary['total_messages']
        
        if 'json_parse_error' in error_types:
            count = error_types['json_parse_error']
            percentage = (count / total_messages) * 100
            print(f"    🔍 JSON parsing errors: {count} messages ({percentage:.1f}%)")
            print("      • Check message serialization format")
            print("      • Verify producers are sending valid JSON")
            print("      • Consider message validation before sending")
        
        if 'timeout_error' in error_types:
            count = error_types['timeout_error']
            percentage = (count / total_messages) * 100
            print(f"    ⏱️  Timeout errors: {count} messages ({percentage:.1f}%)")
            print("      • Check consumer processing performance")
            print("      • Consider increasing timeout values")
            print("      • Review consumer resource allocation")
        
        if 'serialization_error' in error_types:
            count = error_types['serialization_error']
            percentage = (count / total_messages) * 100
            print(f"    📝 Serialization errors: {count} messages ({percentage:.1f}%)")
            print("      • Check message format consistency")
            print("      • Verify schema compatibility")
            print("      • Review producer serialization logic")
        
        # Check retry patterns
        retry_counts = analysis['retry_distribution']
        if retry_counts:
            max_retries = max(retry_counts.keys())
            high_retry_count = sum(count for retry, count in retry_counts.items() if retry >= 3)
            
            if high_retry_count > 0:
                percentage = (high_retry_count / total_messages) * 100
                print(f"    🔄 High retry counts: {high_retry_count} messages ({percentage:.1f}%)")
                print("      • Investigate root cause of persistent failures")
                print("      • Consider adjusting retry strategy")
                print("      • May need circuit breaker pattern")
        
        # Check for content issues
        content_issues = sum(1 for msg in analysis['messages'] if msg['content']['parse_error'])
        if content_issues > 0:
            percentage = (content_issues / total_messages) * 100
            print(f"    ⚠️  Content parsing issues: {content_issues} messages ({percentage:.1f}%)")
            print("      • Review message content format")
            print("      • Check for encoding issues")
            print("      • Validate message structure")
        
        print(f"\n DLQ inspection complete!")
    
    def generate_dlq_topic_name(self, original_topic: str, dlq_suffix: str = "_dlq") -> str:
        """Generate DLQ topic name from original topic."""
        return f"{original_topic}{dlq_suffix}"


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="DLQ Inspector - Analyze failed messages in Dagster-Kafka DLQ topics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Inspect DLQ messages for user-events topic
  python dlq_inspector_fixed.py --topic user-events
  
  # Inspect specific DLQ topic
  python dlq_inspector_fixed.py --dlq-topic user-events_dlq --max-messages 20
  
  # Use different Kafka server
  python dlq_inspector_fixed.py --topic orders --kafka-servers "prod-kafka:9092"
        """
    )
    
    parser.add_argument(
        '--topic',
        help='Original topic name (will inspect {topic}_dlq)',
        type=str
    )
    
    parser.add_argument(
        '--dlq-topic', 
        help='Specific DLQ topic name to inspect',
        type=str
    )
    
    parser.add_argument(
        '--max-messages',
        help='Maximum number of messages to analyze (default: 10)',
        type=int,
        default=10
    )
    
    parser.add_argument(
        '--kafka-servers',
        help='Kafka bootstrap servers (default: localhost:9092)',
        type=str,
        default="localhost:9092"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.topic and not args.dlq_topic:
        print(" Error: Must specify either --topic or --dlq-topic")
        parser.print_help()
        sys.exit(1)
    
    # Determine DLQ topic to inspect
    if args.dlq_topic:
        dlq_topic = args.dlq_topic
    else:
        dlq_topic = f"{args.topic}_dlq"
    
    print(" DLQ Inspector - Dagster Kafka Integration (FIXED)")
    print("=" * 60)
    print(f" Target DLQ Topic: {dlq_topic}")
    print(f" Max Messages: {args.max_messages}")
    print(f" Kafka Servers: {args.kafka_servers}")
    
    try:
        # Initialize inspector
        inspector = DLQInspector(kafka_bootstrap_servers=args.kafka_servers)
        
        # Perform analysis
        analysis = inspector.inspect_dlq_topic(dlq_topic, args.max_messages)
        
        # Print report
        inspector.print_analysis_report(analysis)
        
    except KeyboardInterrupt:
        print("\n\n  Inspection interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n Error during inspection: {e}")
        print("\n Troubleshooting tips:")
        print("   • Check if Kafka is running")
        print("   • Verify the DLQ topic exists")
        print("   • Ensure correct Kafka server address")
        sys.exit(1)


if __name__ == "__main__":
    main()