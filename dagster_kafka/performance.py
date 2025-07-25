"""
Performance optimization module for high-throughput Kafka schema evolution.
Includes caching, batching, connection pooling, and async processing.
"""

from typing import Dict, Any, List, Optional, Tuple, Callable, AsyncGenerator
from dataclasses import dataclass, field
from enum import Enum
import time
import asyncio
import threading
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import hashlib
from datetime import datetime, timedelta
from dagster import get_dagster_logger


class CacheStrategy(Enum):
    """Caching strategies for schema operations."""
    LRU = "lru"              # Least Recently Used
    TTL = "ttl"              # Time To Live
    WRITE_THROUGH = "write_through"  # Write to cache and storage
    WRITE_BACK = "write_back"        # Write to cache first, storage later


class BatchStrategy(Enum):
    """Batching strategies for message processing."""
    SIZE_BASED = "size_based"        # Batch by number of messages
    TIME_BASED = "time_based"        # Batch by time window  
    HYBRID = "hybrid"                # Both size and time based
    ADAPTIVE = "adaptive"            # Dynamically adjust batch size


@dataclass
class PerformanceMetrics:
    """Performance metrics for optimization monitoring."""
    cache_hits: int = 0
    cache_misses: int = 0
    cache_evictions: int = 0
    batch_operations: int = 0
    total_messages_processed: int = 0
    average_batch_size: float = 0.0
    average_processing_time_ms: float = 0.0
    connection_pool_usage: Dict[str, int] = field(default_factory=dict)
    memory_usage_mb: float = 0.0
    
    @property
    def cache_hit_rate(self) -> float:
        """Calculate cache hit rate percentage."""
        total = self.cache_hits + self.cache_misses
        return (self.cache_hits / total * 100) if total > 0 else 0.0
    
    @property
    def throughput_messages_per_second(self) -> float:
        """Calculate throughput in messages per second."""
        if self.average_processing_time_ms > 0:
            return 1000.0 / self.average_processing_time_ms
        return 0.0


class HighPerformanceCache:
    """
    High-performance caching system for schema and validation results.
    Supports multiple caching strategies and automatic eviction.
    """
    
    def __init__(self, 
                 max_size: int = 1000,
                 ttl_seconds: int = 300,
                 strategy: CacheStrategy = CacheStrategy.LRU):
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.strategy = strategy
        self.logger = get_dagster_logger()
        
        # Cache storage
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._access_times: Dict[str, float] = {}
        self._insert_times: Dict[str, float] = {}
        self._access_order: deque = deque()  # For LRU
        
        # Metrics
        self.metrics = PerformanceMetrics()
        
        # Thread safety
        self._lock = threading.RLock()
        
        self.logger.info(f"Initialized cache with strategy={strategy.value}, max_size={max_size}, ttl={ttl_seconds}s")
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache with strategy-specific logic."""
        with self._lock:
            current_time = time.time()
            
            if key not in self._cache:
                self.metrics.cache_misses += 1
                return None
            
            # Check TTL expiration
            if self.strategy in [CacheStrategy.TTL, CacheStrategy.WRITE_THROUGH]:
                if current_time - self._insert_times[key] > self.ttl_seconds:
                    self._evict_key(key)
                    self.metrics.cache_misses += 1
                    return None
            
            # Update access tracking for LRU
            if self.strategy in [CacheStrategy.LRU, CacheStrategy.WRITE_BACK]:
                self._access_times[key] = current_time
                # Move to end of access order
                if key in self._access_order:
                    self._access_order.remove(key)
                self._access_order.append(key)
            
            self.metrics.cache_hits += 1
            return self._cache[key]["value"]
    
    def put(self, key: str, value: Any, ttl_override: Optional[int] = None) -> None:
        """Put value in cache with eviction if necessary."""
        with self._lock:
            current_time = time.time()
            
            # Check if we need to evict
            if key not in self._cache and len(self._cache) >= self.max_size:
                self._evict_one()
            
            # Store the value
            self._cache[key] = {
                "value": value,
                "size": self._estimate_size(value),
                "metadata": {"created": current_time}
            }
            
            self._access_times[key] = current_time
            self._insert_times[key] = current_time
            
            # Update access order for LRU
            if self.strategy in [CacheStrategy.LRU, CacheStrategy.WRITE_BACK]:
                if key in self._access_order:
                    self._access_order.remove(key)
                self._access_order.append(key)
    
    def _evict_one(self) -> None:
        """Evict one item based on strategy."""
        if not self._cache:
            return
        
        key_to_evict = None
        
        if self.strategy == CacheStrategy.LRU:
            # Evict least recently used
            key_to_evict = self._access_order.popleft() if self._access_order else next(iter(self._cache))
        
        elif self.strategy == CacheStrategy.TTL:
            # Evict oldest by insertion time
            key_to_evict = min(self._insert_times.keys(), key=self._insert_times.get)
        
        else:
            # Default: evict first item
            key_to_evict = next(iter(self._cache))
        
        if key_to_evict:
            self._evict_key(key_to_evict)
    
    def _evict_key(self, key: str) -> None:
        """Remove key from all tracking structures."""
        if key in self._cache:
            del self._cache[key]
        if key in self._access_times:
            del self._access_times[key]
        if key in self._insert_times:
            del self._insert_times[key]
        if key in self._access_order:
            self._access_order.remove(key)
        
        self.metrics.cache_evictions += 1
    
    def _estimate_size(self, value: Any) -> int:
        """Estimate memory size of cached value."""
        try:
            if isinstance(value, str):
                return len(value.encode('utf-8'))
            elif isinstance(value, (dict, list)):
                return len(json.dumps(value).encode('utf-8'))
            else:
                return len(str(value).encode('utf-8'))
        except:
            return 1024  # Default 1KB estimate
    
    def clear(self) -> None:
        """Clear all cached items."""
        with self._lock:
            count = len(self._cache)
            self._cache.clear()
            self._access_times.clear()
            self._insert_times.clear()
            self._access_order.clear()
            self.logger.info(f"Cleared {count} items from cache")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            total_size = sum(item["size"] for item in self._cache.values())
            return {
                "size": len(self._cache),
                "max_size": self.max_size,
                "utilization_percent": (len(self._cache) / self.max_size) * 100,
                "total_size_bytes": total_size,
                "cache_hits": self.metrics.cache_hits,
                "cache_misses": self.metrics.cache_misses,
                "cache_hit_rate": self.metrics.cache_hit_rate,
                "evictions": self.metrics.cache_evictions,
                "strategy": self.strategy.value
            }


class BatchProcessor:
    """
    High-performance batch processor for Kafka messages.
    Supports multiple batching strategies and adaptive sizing.
    """
    
    def __init__(self,
                 strategy: BatchStrategy = BatchStrategy.HYBRID,
                 max_batch_size: int = 1000,
                 max_wait_time_ms: int = 5000,
                 min_batch_size: int = 10,
                 adaptive_target_latency_ms: float = 100.0):
        self.strategy = strategy
        self.max_batch_size = max_batch_size
        self.max_wait_time_ms = max_wait_time_ms
        self.min_batch_size = min_batch_size
        self.adaptive_target_latency_ms = adaptive_target_latency_ms
        self.logger = get_dagster_logger()
        
        # Batch storage
        self._batches: Dict[str, List[Any]] = defaultdict(list)
        self._batch_start_times: Dict[str, float] = {}
        
        # Adaptive sizing state
        self._processing_times: deque = deque(maxlen=100)  # Last 100 batch times
        self._current_batch_size = min_batch_size
        
        # Metrics
        self.metrics = PerformanceMetrics()
        
        # Thread safety
        self._lock = threading.RLock()
        
        self.logger.info(f"Initialized batch processor: strategy={strategy.value}, max_size={max_batch_size}")
    
    def add_message(self, topic: str, message: Any) -> Optional[List[Any]]:
        """
        Add message to batch. Returns completed batch if ready for processing.
        """
        with self._lock:
            current_time = time.time()
            
            # Check if existing batch is ready before adding new message
            if topic in self._batches and self._batches[topic] and self._is_batch_ready(topic, current_time):
                # Finalize the existing batch first
                completed_batch = self._finalize_batch(topic)
                # Start new batch with current message
                self._batches[topic].append(message)
                self._batch_start_times[topic] = current_time
                return completed_batch
            
            # Initialize batch if needed
            if topic not in self._batches or not self._batches[topic]:
                self._batch_start_times[topic] = current_time
            
            # Add message to batch
            self._batches[topic].append(message)
            
            # Check if batch is ready after adding message
            if self._is_batch_ready(topic, current_time):
                return self._finalize_batch(topic)
            
            return None
    
    def _is_batch_ready(self, topic: str, current_time: float) -> bool:
        """Check if batch is ready for processing based on strategy."""
        batch = self._batches[topic]
        if not batch:
            return False
            
        batch_age_ms = (current_time - self._batch_start_times[topic]) * 1000
        
        if self.strategy == BatchStrategy.SIZE_BASED:
            return len(batch) >= self._get_target_batch_size()
        
        elif self.strategy == BatchStrategy.TIME_BASED:
            return batch_age_ms >= self.max_wait_time_ms
        
        elif self.strategy == BatchStrategy.HYBRID:
            return (len(batch) >= self._get_target_batch_size() or 
                   batch_age_ms >= self.max_wait_time_ms)
        
        elif self.strategy == BatchStrategy.ADAPTIVE:
            target_size = self._get_adaptive_batch_size()
            return (len(batch) >= target_size or 
                   batch_age_ms >= self.max_wait_time_ms or
                   len(batch) >= self.max_batch_size)
        
        return False
    
    def _get_target_batch_size(self) -> int:
        """Get target batch size based on strategy."""
        if self.strategy == BatchStrategy.ADAPTIVE:
            return self._get_adaptive_batch_size()
        return self._current_batch_size
    
    def _get_adaptive_batch_size(self) -> int:
        """Calculate adaptive batch size based on processing performance."""
        if not self._processing_times:
            return self.min_batch_size
        
        # Calculate average processing time
        avg_time_ms = sum(self._processing_times) / len(self._processing_times)
        
        # Adjust batch size based on performance
        if avg_time_ms < self.adaptive_target_latency_ms * 0.8:
            # Processing is fast, increase batch size
            self._current_batch_size = min(
                self._current_batch_size + 10,
                self.max_batch_size
            )
        elif avg_time_ms > self.adaptive_target_latency_ms * 1.2:
            # Processing is slow, decrease batch size
            self._current_batch_size = max(
                self._current_batch_size - 5,
                self.min_batch_size
            )
        
        return self._current_batch_size
    
    def _finalize_batch(self, topic: str) -> List[Any]:
        """Finalize and return batch for processing."""
        if topic not in self._batches or not self._batches[topic]:
            return []
            
        batch = self._batches[topic].copy()
        self._batches[topic].clear()
        self._batch_start_times[topic] = time.time()
        
        # Update metrics
        self.metrics.batch_operations += 1
        self.metrics.total_messages_processed += len(batch)
        
        # Update average batch size
        total_batches = self.metrics.batch_operations
        if total_batches == 1:
            self.metrics.average_batch_size = float(len(batch))
        else:
            current_total = self.metrics.average_batch_size * (total_batches - 1)
            self.metrics.average_batch_size = (current_total + len(batch)) / total_batches
        
        return batch
    
    def record_processing_time(self, processing_time_ms: float) -> None:
        """Record processing time for adaptive sizing."""
        with self._lock:
            self._processing_times.append(processing_time_ms)
            
            # Update average processing time metrics
            if self.metrics.batch_operations == 0:
                self.metrics.average_processing_time_ms = processing_time_ms
            else:
                # Running average calculation
                total_batches = self.metrics.batch_operations
                current_total = self.metrics.average_processing_time_ms * total_batches
                self.metrics.average_processing_time_ms = (current_total + processing_time_ms) / (total_batches + 1)
    
    def flush_batch(self, topic: str) -> Optional[List[Any]]:
        """Force flush batch regardless of size/time."""
        with self._lock:
            if topic in self._batches and self._batches[topic]:
                return self._finalize_batch(topic)
            return None
    
    def flush_all_batches(self) -> Dict[str, List[Any]]:
        """Force flush all pending batches."""
        with self._lock:
            result = {}
            for topic in list(self._batches.keys()):
                batch = self.flush_batch(topic)
                if batch:
                    result[topic] = batch
            return result
    
    def get_stats(self) -> Dict[str, Any]:
        """Get batch processing statistics."""
        with self._lock:
            pending_messages = sum(len(batch) for batch in self._batches.values())
            return {
                "strategy": self.strategy.value,
                "current_batch_size": self._current_batch_size,
                "max_batch_size": self.max_batch_size,
                "pending_batches": len(self._batches),
                "pending_messages": pending_messages,
                "total_batches_processed": self.metrics.batch_operations,
                "total_messages_processed": self.metrics.total_messages_processed,
                "average_batch_size": self.metrics.average_batch_size,
                "average_processing_time_ms": self.metrics.average_processing_time_ms,
                "throughput_msg_per_sec": self.metrics.throughput_messages_per_second
            }


class ConnectionPool:
    """
    Connection pool for Kafka and Schema Registry connections.
    Manages connection lifecycle and provides high availability.
    """
    
    def __init__(self,
                 max_connections: int = 10,
                 connection_timeout: float = 30.0,
                 idle_timeout: float = 300.0):
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.idle_timeout = idle_timeout
        self.logger = get_dagster_logger()
        
        # Connection pools by type
        self._kafka_pool: deque = deque()
        self._schema_registry_pool: deque = deque()
        
        # Connection tracking
        self._connection_count = {"kafka": 0, "schema_registry": 0}
        self._connection_times = {"kafka": {}, "schema_registry": {}}
        
        # Thread safety
        self._lock = threading.RLock()
        
        self.logger.info(f"Initialized connection pool: max={max_connections}, timeout={connection_timeout}s")
    
    def get_kafka_connection(self, config: Dict[str, Any]) -> Any:
        """Get Kafka connection from pool or create new one."""
        with self._lock:
            # Try to get from pool
            if self._kafka_pool:
                connection = self._kafka_pool.popleft()
                if self._is_connection_valid(connection, "kafka"):
                    return connection
                else:
                    self._connection_count["kafka"] -= 1
            
            # Create new connection if under limit
            if self._connection_count["kafka"] < self.max_connections:
                connection = self._create_kafka_connection(config)
                self._connection_count["kafka"] += 1
                self._connection_times["kafka"][id(connection)] = time.time()
                return connection
            
            # Pool exhausted
            raise RuntimeError("Kafka connection pool exhausted")
    
    def return_kafka_connection(self, connection: Any) -> None:
        """Return Kafka connection to pool."""
        with self._lock:
            if len(self._kafka_pool) < self.max_connections:
                self._kafka_pool.append(connection)
            else:
                # Pool full, close connection
                self._close_kafka_connection(connection)
                self._connection_count["kafka"] -= 1
    
    def get_schema_registry_connection(self, url: str) -> Any:
        """Get Schema Registry connection from pool."""
        with self._lock:
            # Similar logic to Kafka connections
            if self._schema_registry_pool:
                connection = self._schema_registry_pool.popleft()
                if self._is_connection_valid(connection, "schema_registry"):
                    return connection
                else:
                    self._connection_count["schema_registry"] -= 1
            
            if self._connection_count["schema_registry"] < self.max_connections:
                connection = self._create_schema_registry_connection(url)
                self._connection_count["schema_registry"] += 1
                self._connection_times["schema_registry"][id(connection)] = time.time()
                return connection
            
            raise RuntimeError("Schema Registry connection pool exhausted")
    
    def return_schema_registry_connection(self, connection: Any) -> None:
        """Return Schema Registry connection to pool."""
        with self._lock:
            if len(self._schema_registry_pool) < self.max_connections:
                self._schema_registry_pool.append(connection)
            else:
                self._close_schema_registry_connection(connection)
                self._connection_count["schema_registry"] -= 1
    
    def _create_kafka_connection(self, config: Dict[str, Any]) -> Any:
        """Create new Kafka connection."""
        try:
            from confluent_kafka import Consumer
            return Consumer(config)
        except Exception as e:
            self.logger.error(f"Failed to create Kafka connection: {e}")
            raise
    
    def _create_schema_registry_connection(self, url: str) -> Any:
        """Create new Schema Registry connection."""
        try:
            from confluent_kafka.schema_registry import SchemaRegistryClient
            return SchemaRegistryClient({'url': url})
        except Exception as e:
            self.logger.error(f"Failed to create Schema Registry connection: {e}")
            raise
    
    def _is_connection_valid(self, connection: Any, connection_type: str) -> bool:
        """Check if connection is still valid."""
        try:
            conn_id = id(connection)
            if conn_id in self._connection_times[connection_type]:
                age = time.time() - self._connection_times[connection_type][conn_id]
                if age > self.idle_timeout:
                    return False
            
            # Additional health checks could go here
            return True
        except:
            return False
    
    def _close_kafka_connection(self, connection: Any) -> None:
        """Close Kafka connection."""
        try:
            connection.close()
        except:
            pass
    
    def _close_schema_registry_connection(self, connection: Any) -> None:
        """Close Schema Registry connection."""
        # Schema Registry client doesn't need explicit closing
        pass
    
    def cleanup_idle_connections(self) -> None:
        """Clean up idle connections beyond timeout."""
        with self._lock:
            current_time = time.time()
            
            # Clean Kafka connections
            active_kafka = deque()
            while self._kafka_pool:
                conn = self._kafka_pool.popleft()
                if self._is_connection_valid(conn, "kafka"):
                    active_kafka.append(conn)
                else:
                    self._close_kafka_connection(conn)
                    self._connection_count["kafka"] -= 1
            self._kafka_pool = active_kafka
            
            # Clean Schema Registry connections  
            active_sr = deque()
            while self._schema_registry_pool:
                conn = self._schema_registry_pool.popleft()
                if self._is_connection_valid(conn, "schema_registry"):
                    active_sr.append(conn)
                else:
                    self._close_schema_registry_connection(conn)
                    self._connection_count["schema_registry"] -= 1
            self._schema_registry_pool = active_sr
    
    def get_stats(self) -> Dict[str, Any]:
        """Get connection pool statistics."""
        with self._lock:
            return {
                "max_connections": self.max_connections,
                "kafka_connections": {
                    "active": self._connection_count["kafka"],
                    "pooled": len(self._kafka_pool),
                    "utilization": (self._connection_count["kafka"] / self.max_connections) * 100
                },
                "schema_registry_connections": {
                    "active": self._connection_count["schema_registry"], 
                    "pooled": len(self._schema_registry_pool),
                    "utilization": (self._connection_count["schema_registry"] / self.max_connections) * 100
                }
            }


class PerformanceOptimizer:
    """
    Main performance optimization coordinator.
    Combines caching, batching, and connection pooling.
    """
    
    def __init__(self,
                 cache_config: Optional[Dict[str, Any]] = None,
                 batch_config: Optional[Dict[str, Any]] = None,
                 pool_config: Optional[Dict[str, Any]] = None):
        
        # Initialize components with configs
        cache_config = cache_config or {}
        batch_config = batch_config or {}
        pool_config = pool_config or {}
        
        self.cache = HighPerformanceCache(**cache_config)
        self.batch_processor = BatchProcessor(**batch_config)
        self.connection_pool = ConnectionPool(**pool_config)
        
        self.logger = get_dagster_logger()
        self.logger.info("Performance optimizer initialized with all components")
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics."""
        return {
            "cache": self.cache.get_stats(),
            "batching": self.batch_processor.get_stats(), 
            "connections": self.connection_pool.get_stats(),
            "timestamp": datetime.now().isoformat()
        }
    
    def optimize_for_throughput(self) -> Dict[str, Any]:
        """Apply optimizations focused on maximizing throughput."""
        recommendations = []
        
        cache_stats = self.cache.get_stats()
        if cache_stats["cache_hit_rate"] < 70:
            recommendations.append("Consider increasing cache size or TTL")
        
        batch_stats = self.batch_processor.get_stats()
        if batch_stats["average_batch_size"] < 100:
            recommendations.append("Consider increasing batch size for better throughput")
        
        conn_stats = self.connection_pool.get_stats()
        kafka_util = conn_stats["kafka_connections"]["utilization"]
        if kafka_util > 80:
            recommendations.append("Consider increasing Kafka connection pool size")
        
        return {
            "optimization_focus": "throughput",
            "recommendations": recommendations,
            "current_stats": self.get_comprehensive_stats()
        }
    
    def optimize_for_latency(self) -> Dict[str, Any]:
        """Apply optimizations focused on minimizing latency."""
        recommendations = []
        
        batch_stats = self.batch_processor.get_stats()
        if batch_stats["average_processing_time_ms"] > 1000:
            recommendations.append("Consider reducing batch size for lower latency")
        
        cache_stats = self.cache.get_stats()
        if cache_stats["size"] > cache_stats["max_size"] * 0.9:
            recommendations.append("Cache near capacity - consider cleanup or size increase")
        
        return {
            "optimization_focus": "latency",
            "recommendations": recommendations,
            "current_stats": self.get_comprehensive_stats()
        }