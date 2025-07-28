#!/usr/bin/env python3
"""
DLQ Message Replayer - Simplified Production Version
Replays failed messages from Dead Letter Queue topics.
"""

import argparse
import sys
import time
from kafka import KafkaConsumer, KafkaProducer


class DLQReplayer:
    def __init__(self, bootstrap_servers='localhost:9092'):
        self.bootstrap_servers = bootstrap_servers
    
    def _create_consumer(self, topic):
        return KafkaConsumer(
            topic,
            bootstrap_servers=[self.bootstrap_servers],
            value_deserializer=lambda x: x,
            key_deserializer=lambda x: x if x else None,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=3000
        )
    
    def _create_producer(self):
        return KafkaProducer(
            bootstrap_servers=[self.bootstrap_servers],
            value_serializer=lambda x: x,
            key_serializer=lambda x: x if x else None
        )
    
    def _matches_filter(self, message, error_types=None, min_retry=None, max_retry=None):
        if not (error_types or min_retry is not None or max_retry is not None):
            return True
            
        headers = dict(message.headers) if message.headers else {}
        
        if error_types:
            error_type = headers.get('error_type', b'').decode('utf-8')
            if error_type not in error_types:
                return False
        
        if min_retry is not None or max_retry is not None:
            try:
                retry_count = int(headers.get('retry_count', b'0').decode('utf-8'))
                if min_retry is not None and retry_count < min_retry:
                    return False
                if max_retry is not None and retry_count > max_retry:
                    return False
            except (ValueError, AttributeError):
                return False
        
        return True
    
    def _rate_limit(self, rate_per_sec, last_sent_time):
        if rate_per_sec and last_sent_time:
            interval = 1.0 / rate_per_sec
            elapsed = time.time() - last_sent_time
            if elapsed < interval:
                time.sleep(interval - elapsed)
        return time.time()
    
    def _confirm_replay(self, source_topic, target_topic, estimated_count):
        if estimated_count == 0:
            print("No messages found to replay.")
            return False
        
        print(f"Will replay ~{estimated_count} messages from '{source_topic}' to '{target_topic}'")
        if estimated_count > 10:
            print("WARNING: Large number of messages will be replayed.")
        
        response = input("Proceed? (y/N): ").strip().lower()
        return response in ['y', 'yes']
    
    def replay(self, source_topic, target_topic, max_messages=None, dry_run=False, 
              rate_limit=None, error_types=None, min_retry=None, max_retry=None, confirm=False):
        
        # Parse error types
        if error_types:
            error_types = set(t.strip() for t in error_types.split(','))
        
        # Quick count for confirmation
        if confirm and not dry_run:
            print("Analyzing messages...")
            consumer = None
            try:
                consumer = self._create_consumer(source_topic)
                count = sum(1 for msg in consumer if self._matches_filter(msg, error_types, min_retry, max_retry))
                if not self._confirm_replay(source_topic, target_topic, min(count, max_messages or count)):
                    return True
            except Exception as e:
                print(f"Error during analysis: {e}")
                return False
            finally:
                if consumer:
                    consumer.close()
        
        # Main replay logic
        consumer = None
        producer = None
        try:
            consumer = self._create_consumer(source_topic)
            producer = self._create_producer()
            
            print(f"Replaying from '{source_topic}' to '{target_topic}'")
            print(f"Mode: {'DRY RUN' if dry_run else 'LIVE'}, Max: {max_messages or 'unlimited'}")
            if rate_limit:
                print(f"Rate limit: {rate_limit}/sec")
            if error_types:
                print(f"Filter: {', '.join(error_types)}")
            print("-" * 50)
            
            processed = replayed = filtered = 0
            last_sent = 0
            start_time = time.time()
            
            for message in consumer:
                processed += 1
                
                # Apply filters
                if not self._matches_filter(message, error_types, min_retry, max_retry):
                    filtered += 1
                    continue
                
                # Check max limit
                if max_messages and replayed >= max_messages:
                    break
                
                # Rate limiting
                if rate_limit and not dry_run:
                    last_sent = self._rate_limit(rate_limit, last_sent)
                
                # Replay or dry run
                if dry_run:
                    replayed += 1
                    if replayed <= 3:  # Show first few
                        headers = dict(message.headers) if message.headers else {}
                        error_type = headers.get('error_type', b'unknown').decode('utf-8')
                        print(f"[DRY RUN] Would replay: {error_type}")
                else:
                    try:
                        producer.send(target_topic, key=message.key, value=message.value, headers=message.headers).get(timeout=10)
                        replayed += 1
                        if replayed <= 3:  # Show first few
                            headers = dict(message.headers) if message.headers else {}
                            error_type = headers.get('error_type', b'unknown').decode('utf-8')
                            print(f"Replayed: {error_type}")
                    except Exception as e:
                        print(f"Failed to replay message: {e}")
                
                # Progress updates
                if processed % 50 == 0:
                    elapsed = time.time() - start_time
                    rate = processed / elapsed if elapsed > 0 else 0
                    print(f"Progress: {processed} processed, {replayed} replayed ({rate:.1f}/sec)")
            
            # Summary
            elapsed = time.time() - start_time
            print("-" * 50)
            print(f"Complete: {processed} processed, {filtered} filtered, {replayed} replayed")
            print(f"Time: {elapsed:.1f}s, Rate: {processed/elapsed:.1f} messages/sec")
            
            return True
            
        except Exception as e:
            print(f"Replay failed: {e}")
            return False
        finally:
            if consumer:
                consumer.close()
            if producer:
                producer.close()
    
    def test_connection(self):
        consumer = None
        try:
            consumer = self._create_consumer('__test_topic__')
            print("Kafka connection successful")
            return True
        except Exception as e:
            print(f"Kafka connection failed: {e}")
            return False
        finally:
            if consumer:
                consumer.close()


def main():
    parser = argparse.ArgumentParser(description='DLQ Message Replayer')
    parser.add_argument('--source-topic', help='Source DLQ topic')
    parser.add_argument('--target-topic', help='Target topic for replay')
    parser.add_argument('--max-messages', type=int, help='Maximum messages to replay')
    parser.add_argument('--rate-limit', type=float, help='Rate limit (messages/second)')
    parser.add_argument('--error-types', help='Comma-separated error types to replay')
    parser.add_argument('--min-retry-count', type=int, help='Minimum retry count')
    parser.add_argument('--max-retry-count', type=int, help='Maximum retry count')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be replayed')
    parser.add_argument('--confirm', action='store_true', help='Ask for confirmation')
    parser.add_argument('--test', action='store_true', help='Test Kafka connection')
    parser.add_argument('--bootstrap-servers', default='localhost:9092', help='Kafka servers')
    
    args = parser.parse_args()
    
    replayer = DLQReplayer(args.bootstrap_servers)
    
    if args.test:
        return 0 if replayer.test_connection() else 1
    
    if not (args.source_topic and args.target_topic):
        if not args.test:
            print("DLQ Replayer - Use --source-topic and --target-topic to replay messages")
            print("Use --test to verify connection, --dry-run to preview, --confirm for safety")
        return 0
    
    success = replayer.replay(
        source_topic=args.source_topic,
        target_topic=args.target_topic,
        max_messages=args.max_messages,
        dry_run=args.dry_run,
        rate_limit=args.rate_limit,
        error_types=args.error_types,
        min_retry=args.min_retry_count,
        max_retry=args.max_retry_count,
        confirm=args.confirm
    )
    
    return 0 if success else 1


if __name__ == '__main__':
    sys.exit(main())