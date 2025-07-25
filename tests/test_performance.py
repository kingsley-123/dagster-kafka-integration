"""Tests for performance optimization components."""

import pytest
import time
import sys
import os
from unittest.mock import Mock, patch

# Add project root to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dagster_kafka.performance import (
    PerformanceOptimizer,
    HighPerformanceCache,
    BatchProcessor,
    ConnectionPool,
    CacheStrategy,
    BatchStrategy,
    PerformanceMetrics
)


class TestPerformanceMetrics:
    """Test PerformanceMetrics dataclass."""
    
    def test_metrics_initialization(self):
        """Test metrics initialize with correct defaults."""
        metrics = PerformanceMetrics()
        
        assert metrics.cache_hits == 0
        assert metrics.cache_misses == 0
        assert metrics.batch_operations == 0
        assert metrics.cache_hit_rate == 0.0
        assert metrics.throughput_messages_per_second == 0.0
    
    def test_cache_hit_rate_calculation(self):
        """Test cache hit rate calculation."""
        metrics = PerformanceMetrics()
        metrics.cache_hits = 80
        metrics.cache_misses = 20
        
        assert metrics.cache_hit_rate == 80.0
    
    def test_throughput_calculation(self):
        """Test throughput calculation."""
        metrics = PerformanceMetrics()
        metrics.average_processing_time_ms = 100.0  # 100ms per message
        
        assert metrics.throughput_messages_per_second == 10.0  # 1000/100


class TestHighPerformanceCache:
    """Test HighPerformanceCache."""
    
    def test_cache_initialization(self):
        """Test cache initializes with correct settings."""
        cache = HighPerformanceCache(
            max_size=500,
            ttl_seconds=600,
            strategy=CacheStrategy.LRU
        )
        
        assert cache.max_size == 500
        assert cache.ttl_seconds == 600
        assert cache.strategy == CacheStrategy.LRU
        assert len(cache._cache) == 0
    
    def test_basic_get_put_operations(self):
        """Test basic cache get/put operations."""
        cache = HighPerformanceCache(max_size=10)
        
        # Put and get
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"
        
        # Cache hit/miss metrics
        assert cache.metrics.cache_hits == 1
        assert cache.metrics.cache_misses == 0
        
        # Get non-existent key
        assert cache.get("nonexistent") is None
        assert cache.metrics.cache_misses == 1
    
    def test_lru_eviction(self):
        """Test LRU eviction policy."""
        cache = HighPerformanceCache(max_size=3, strategy=CacheStrategy.LRU)
        
        # Fill cache to capacity
        cache.put("key1", "value1")
        cache.put("key2", "value2") 
        cache.put("key3", "value3")
        
        # Access key1 to make it recently used
        cache.get("key1")
        
        # Add key4, should evict key2 (least recently used)
        cache.put("key4", "value4")
        
        assert cache.get("key1") == "value1"  # Still there
        assert cache.get("key2") is None      # Evicted
        assert cache.get("key3") == "value3"  # Still there
        assert cache.get("key4") == "value4"  # New item
    
    def test_ttl_expiration(self):
        """Test TTL-based expiration."""
        cache = HighPerformanceCache(
            max_size=10,
            ttl_seconds=1,  # 1 second TTL
            strategy=CacheStrategy.TTL
        )
        
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"
        
        # Wait for TTL expiration
        time.sleep(1.1)
        
        assert cache.get("key1") is None  # Should be expired
        assert cache.metrics.cache_misses == 1
    
    def test_cache_clear(self):
        """Test cache clearing."""
        cache = HighPerformanceCache(max_size=10)
        
        cache.put("key1", "value1")
        cache.put("key2", "value2")
        
        assert len(cache._cache) == 2
        
        cache.clear()
        
        assert len(cache._cache) == 0
        assert cache.get("key1") is None
    
    def test_cache_stats(self):
        """Test cache statistics."""
        cache = HighPerformanceCache(max_size=100)
        
        cache.put("key1", "value1")
        cache.get("key1")  # Hit
        cache.get("key2")  # Miss
        
        stats = cache.get_stats()
        
        assert stats["size"] == 1
        assert stats["max_size"] == 100
        assert stats["cache_hits"] == 1
        assert stats["cache_misses"] == 1
        assert stats["cache_hit_rate"] == 50.0
        assert stats["strategy"] == CacheStrategy.LRU.value


class TestBatchProcessor:
    """Test BatchProcessor."""
    
    def test_batch_processor_initialization(self):
        """Test batch processor initializes correctly."""
        processor = BatchProcessor(
            strategy=BatchStrategy.SIZE_BASED,
            max_batch_size=500,
            max_wait_time_ms=10000
        )
        
        assert processor.strategy == BatchStrategy.SIZE_BASED
        assert processor.max_batch_size == 500
        assert processor.max_wait_time_ms == 10000
        assert len(processor._batches) == 0
    
    def test_size_based_batching(self):
        """Test size-based batching."""
        processor = BatchProcessor(
            strategy=BatchStrategy.SIZE_BASED,
            max_batch_size=3,
            min_batch_size=3
        )
        
        # Add messages one by one
        result1 = processor.add_message("topic1", "msg1")
        result2 = processor.add_message("topic1", "msg2")
        assert result1 is None
        assert result2 is None
        
        # Third message should trigger batch completion
        result3 = processor.add_message("topic1", "msg3")
        assert result3 == ["msg1", "msg2", "msg3"]
    
    def test_time_based_batching(self):
        """Test time-based batching."""
        processor = BatchProcessor(
            strategy=BatchStrategy.TIME_BASED,
            max_wait_time_ms=100  # 100ms timeout
        )
        
        # Add message
        result1 = processor.add_message("topic1", "msg1")
        assert result1 is None
        
        # Wait for timeout and add another message
        time.sleep(0.11)  # Wait longer than timeout
        result2 = processor.add_message("topic1", "msg2")
        
        # Should have completed the first batch (msg1 only)
        # and started new batch with msg2
        assert result2 == ["msg1"]
        
        # Verify new batch has msg2
        pending_batch = processor.flush_batch("topic1")
        assert pending_batch == ["msg2"]
    
    def test_hybrid_batching(self):
        """Test hybrid (size + time) batching."""
        processor = BatchProcessor(
            strategy=BatchStrategy.HYBRID,
            max_batch_size=5,
            min_batch_size=2,
            max_wait_time_ms=100
        )
        
        # Add messages
        processor.add_message("topic1", "msg1")
        
        # Wait for timeout
        time.sleep(0.11)
        result = processor.add_message("topic1", "msg2")
        
        # Should complete due to timeout (msg1 only)
        assert result == ["msg1"]
        
        # Verify new batch has msg2
        pending_batch = processor.flush_batch("topic1")
        assert pending_batch == ["msg2"]
    
    def test_flush_batch(self):
        """Test manual batch flushing."""
        processor = BatchProcessor(strategy=BatchStrategy.SIZE_BASED, max_batch_size=10)
        
        processor.add_message("topic1", "msg1")
        processor.add_message("topic1", "msg2")
        
        # Manually flush
        result = processor.flush_batch("topic1")
        assert result == ["msg1", "msg2"]
        
        # Batch should be empty now
        result2 = processor.flush_batch("topic1")
        assert result2 is None
    
    def test_flush_all_batches(self):
        """Test flushing all pending batches."""
        processor = BatchProcessor(strategy=BatchStrategy.SIZE_BASED, max_batch_size=10)
        
        processor.add_message("topic1", "msg1")
        processor.add_message("topic2", "msg2")
        
        result = processor.flush_all_batches()
        
        assert "topic1" in result
        assert "topic2" in result
        assert result["topic1"] == ["msg1"]
        assert result["topic2"] == ["msg2"]
    
    def test_processing_time_recording(self):
        """Test recording processing times for adaptive sizing."""
        processor = BatchProcessor(strategy=BatchStrategy.ADAPTIVE)
        
        # First complete a batch to initialize metrics
        processor.add_message("topic1", "msg1")
        processor.flush_batch("topic1")  # This updates batch_operations
        
        processor.record_processing_time(100.0)  # 100ms
        processor.record_processing_time(200.0)  # 200ms
        
        # Should update metrics
        assert processor.metrics.average_processing_time_ms > 0
    
    def test_batch_stats(self):
        """Test batch processing statistics."""
        processor = BatchProcessor(strategy=BatchStrategy.SIZE_BASED, max_batch_size=5, min_batch_size=5)
        
        # Add some messages and complete a batch
        processor.add_message("topic1", "msg1")
        processor.add_message("topic1", "msg2")
        processor.add_message("topic1", "msg3")
        processor.add_message("topic1", "msg4")
        result = processor.add_message("topic1", "msg5")  # Complete batch
        
        assert result == ["msg1", "msg2", "msg3", "msg4", "msg5"]
        
        stats = processor.get_stats()
        
        assert stats["strategy"] == BatchStrategy.SIZE_BASED.value
        assert stats["max_batch_size"] == 5
        assert stats["total_batches_processed"] == 1
        assert stats["total_messages_processed"] == 5
        assert stats["average_batch_size"] == 5.0


class TestConnectionPool:
    """Test ConnectionPool."""
    
    def test_connection_pool_initialization(self):
        """Test connection pool initializes correctly."""
        pool = ConnectionPool(
            max_connections=5,
            connection_timeout=10.0,
            idle_timeout=300.0
        )
        
        assert pool.max_connections == 5
        assert pool.connection_timeout == 10.0
        assert pool.idle_timeout == 300.0
        assert pool._connection_count["kafka"] == 0
        assert pool._connection_count["schema_registry"] == 0
    
    @patch('confluent_kafka.Consumer')
    def test_kafka_connection_creation(self, mock_consumer):
        """Test Kafka connection creation and pooling."""
        pool = ConnectionPool(max_connections=2)
        mock_connection = Mock()
        mock_consumer.return_value = mock_connection
        
        # Get connection
        config = {"bootstrap.servers": "localhost:9092"}
        conn1 = pool.get_kafka_connection(config)
        
        assert conn1 == mock_connection
        assert pool._connection_count["kafka"] == 1
        
        # Return connection to pool
        pool.return_kafka_connection(conn1)
        assert len(pool._kafka_pool) == 1
    
    @patch('confluent_kafka.schema_registry.SchemaRegistryClient')
    def test_schema_registry_connection_creation(self, mock_client):
        """Test Schema Registry connection creation."""
        pool = ConnectionPool(max_connections=2)
        mock_connection = Mock()
        mock_client.return_value = mock_connection
        
        # Get connection
        conn1 = pool.get_schema_registry_connection("http://localhost:8081")
        
        assert conn1 == mock_connection
        assert pool._connection_count["schema_registry"] == 1
    
    def test_connection_pool_exhaustion(self):
        """Test behavior when connection pool is exhausted."""
        pool = ConnectionPool(max_connections=1)
        
        with patch('confluent_kafka.Consumer') as mock_consumer:
            mock_consumer.return_value = Mock()
            
            # Get first connection
            config = {"bootstrap.servers": "localhost:9092"}
            conn1 = pool.get_kafka_connection(config)
            
            # Try to get second connection (should fail)
            with pytest.raises(RuntimeError, match="exhausted"):
                pool.get_kafka_connection(config)
    
    def test_connection_pool_stats(self):
        """Test connection pool statistics."""
        pool = ConnectionPool(max_connections=5)
        
        with patch('confluent_kafka.Consumer') as mock_consumer:
            mock_consumer.return_value = Mock()
            
            config = {"bootstrap.servers": "localhost:9092"}
            conn1 = pool.get_kafka_connection(config)
            
            stats = pool.get_stats()
            
            assert stats["max_connections"] == 5
            assert stats["kafka_connections"]["active"] == 1
            assert stats["kafka_connections"]["utilization"] == 20.0  # 1/5 * 100


class TestPerformanceOptimizer:
    """Test PerformanceOptimizer coordinator."""
    
    def test_optimizer_initialization(self):
        """Test performance optimizer initializes all components."""
        optimizer = PerformanceOptimizer()
        
        assert isinstance(optimizer.cache, HighPerformanceCache)
        assert isinstance(optimizer.batch_processor, BatchProcessor)
        assert isinstance(optimizer.connection_pool, ConnectionPool)
    
    def test_custom_configuration(self):
        """Test optimizer with custom component configurations."""
        cache_config = {"max_size": 500, "strategy": CacheStrategy.TTL}
        batch_config = {"strategy": BatchStrategy.TIME_BASED, "max_batch_size": 200}
        pool_config = {"max_connections": 20}
        
        optimizer = PerformanceOptimizer(
            cache_config=cache_config,
            batch_config=batch_config,
            pool_config=pool_config
        )
        
        assert optimizer.cache.max_size == 500
        assert optimizer.cache.strategy == CacheStrategy.TTL
        assert optimizer.batch_processor.strategy == BatchStrategy.TIME_BASED
        assert optimizer.batch_processor.max_batch_size == 200
        assert optimizer.connection_pool.max_connections == 20
    
    def test_comprehensive_stats(self):
        """Test comprehensive statistics gathering."""
        optimizer = PerformanceOptimizer()
        
        # Add some activity
        optimizer.cache.put("test", "value")
        optimizer.cache.get("test")
        optimizer.batch_processor.add_message("topic1", "msg1")
        
        stats = optimizer.get_comprehensive_stats()
        
        assert "cache" in stats
        assert "batching" in stats
        assert "connections" in stats
        assert "timestamp" in stats
        
        # Verify cache stats
        assert stats["cache"]["cache_hits"] == 1
        assert stats["cache"]["size"] == 1
    
    def test_throughput_optimization(self):
        """Test throughput optimization recommendations."""
        optimizer = PerformanceOptimizer()
        
        recommendations = optimizer.optimize_for_throughput()
        
        assert recommendations["optimization_focus"] == "throughput"
        assert "recommendations" in recommendations
        assert "current_stats" in recommendations
        assert isinstance(recommendations["recommendations"], list)
    
    def test_latency_optimization(self):
        """Test latency optimization recommendations."""
        optimizer = PerformanceOptimizer()
        
        recommendations = optimizer.optimize_for_latency()
        
        assert recommendations["optimization_focus"] == "latency"
        assert "recommendations" in recommendations
        assert "current_stats" in recommendations
        assert isinstance(recommendations["recommendations"], list)


def test_enum_values():
    """Test that all enum values are available."""
    
    # Test CacheStrategy
    expected_cache_strategies = ["lru", "ttl", "write_through", "write_back"]
    actual_cache_strategies = [strategy.value for strategy in CacheStrategy]
    for strategy in expected_cache_strategies:
        assert strategy in actual_cache_strategies
    
    # Test BatchStrategy
    expected_batch_strategies = ["size_based", "time_based", "hybrid", "adaptive"]
    actual_batch_strategies = [strategy.value for strategy in BatchStrategy]
    for strategy in expected_batch_strategies:
        assert strategy in actual_batch_strategies


def test_performance_imports():
    """Test that all performance components import correctly."""
    from dagster_kafka.performance import (
        PerformanceOptimizer,
        HighPerformanceCache,
        BatchProcessor,
        ConnectionPool,
        CacheStrategy,
        BatchStrategy,
        PerformanceMetrics
    )
    
    assert PerformanceOptimizer is not None
    assert HighPerformanceCache is not None
    assert BatchProcessor is not None
    assert ConnectionPool is not None
    assert CacheStrategy is not None
    assert BatchStrategy is not None
    assert PerformanceMetrics is not None
    print("✅ All performance imports working!")