"""
Production utilities for robust schema evolution and error handling.
"""

from typing import Optional, Dict, Any, List, Callable
from enum import Enum
import time
import json
from dataclasses import dataclass
from dagster import get_dagster_logger
from .schema_evolution import SchemaEvolutionValidator, CompatibilityLevel


class RecoveryStrategy(Enum):
    """Error recovery strategies for schema evolution failures."""
    FAIL_FAST = "fail_fast"
    FALLBACK_SCHEMA = "fallback_schema"
    SKIP_VALIDATION = "skip_validation"
    RETRY_WITH_BACKOFF = "retry_with_backoff"
    GRACEFUL_DEGRADATION = "graceful_degradation"


@dataclass
class SchemaEvolutionMetrics:
    """Metrics for schema evolution operations."""
    validation_attempts: int = 0
    validation_successes: int = 0
    validation_failures: int = 0
    fallback_used: int = 0
    total_processing_time: float = 0.0
    breaking_changes_detected: int = 0
    compatibility_checks: int = 0
    
    @property
    def success_rate(self) -> float:
        if self.validation_attempts == 0:
            return 0.0
        return (self.validation_successes / self.validation_attempts) * 100
    
    @property
    def average_processing_time(self) -> float:
        if self.validation_attempts == 0:
            return 0.0
        return self.total_processing_time / self.validation_attempts


class ProductionSchemaEvolutionManager:
    """
    Production-grade schema evolution manager with error handling,
    monitoring, caching, and recovery strategies.
    """
    
    def __init__(self,
                 validator: SchemaEvolutionValidator,
                 recovery_strategy: RecoveryStrategy = RecoveryStrategy.FALLBACK_SCHEMA,
                 enable_caching: bool = True,
                 cache_ttl: int = 300,  # 5 minutes
                 max_retries: int = 3,
                 retry_backoff: float = 1.0):
        self.validator = validator
        self.recovery_strategy = recovery_strategy
        self.enable_caching = enable_caching
        self.cache_ttl = cache_ttl
        self.max_retries = max_retries
        self.retry_backoff = retry_backoff
        self.logger = get_dagster_logger()
        
        # Caching
        self._schema_cache: Dict[str, Dict[str, Any]] = {}
        self._compatibility_cache: Dict[str, Dict[str, Any]] = {}
        
        # Metrics
        self.metrics = SchemaEvolutionMetrics()
        
        # Fallback schemas registry
        self.fallback_schemas: Dict[str, List[str]] = {}
    
    def register_fallback_schemas(self, subject: str, schema_versions: List[str]):
        """Register fallback schemas for a subject in priority order."""
        self.fallback_schemas[subject] = schema_versions
        self.logger.info(f"Registered {len(schema_versions)} fallback schemas for {subject}")
    
    def validate_with_recovery(self,
                              subject: str,
                              new_schema: str,
                              compatibility_level: CompatibilityLevel,
                              context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Validate schema with production-grade error handling and recovery.
        """
        start_time = time.time()
        self.metrics.validation_attempts += 1
        
        try:
            # Check cache first
            if self.enable_caching:
                cached_result = self._get_cached_compatibility(subject, new_schema, compatibility_level)
                if cached_result:
                    self.logger.debug(f"Using cached compatibility result for {subject}")
                    return cached_result
            
            # Attempt validation with retries
            result = self._validate_with_retries(subject, new_schema, compatibility_level)
            
            # Cache successful result
            if self.enable_caching and result["compatible"]:
                self._cache_compatibility_result(subject, new_schema, compatibility_level, result)
            
            if result["compatible"]:
                self.metrics.validation_successes += 1
                return result
            else:
                # Validation failed - apply recovery strategy
                return self._apply_recovery_strategy(subject, new_schema, compatibility_level, result, context)
                
        except Exception as e:
            self.logger.error(f"Schema validation error for {subject}: {e}")
            self.metrics.validation_failures += 1
            return self._handle_validation_exception(subject, new_schema, e, context)
        
        finally:
            processing_time = time.time() - start_time
            self.metrics.total_processing_time += processing_time
            self.logger.debug(f"Schema validation took {processing_time:.3f}s")
    
    def _validate_with_retries(self, subject: str, new_schema: str, 
                              compatibility_level: CompatibilityLevel) -> Dict[str, Any]:
        """Validate with exponential backoff retries."""
        last_exception = None
        
        for attempt in range(self.max_retries):
            try:
                self.metrics.compatibility_checks += 1
                result = self.validator.validate_schema_compatibility(
                    subject, new_schema, compatibility_level
                )
                return result
                
            except Exception as e:
                last_exception = e
                if attempt < self.max_retries - 1:
                    backoff_time = self.retry_backoff * (2 ** attempt)
                    self.logger.warning(f"Validation attempt {attempt + 1} failed, retrying in {backoff_time}s: {e}")
                    time.sleep(backoff_time)
                else:
                    self.logger.error(f"All {self.max_retries} validation attempts failed")
        
        raise last_exception
    
    def _apply_recovery_strategy(self, 
                               subject: str, 
                               new_schema: str,
                               compatibility_level: CompatibilityLevel,
                               failed_result: Dict[str, Any],
                               context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply the configured recovery strategy."""
        
        self.logger.warning(f"Schema validation failed for {subject}: {failed_result['reason']}")
        
        if self.recovery_strategy == RecoveryStrategy.FAIL_FAST:
            raise ValueError(f"Schema validation failed: {failed_result['reason']}")
        
        elif self.recovery_strategy == RecoveryStrategy.FALLBACK_SCHEMA:
            return self._try_fallback_schemas(subject, compatibility_level, failed_result)
        
        elif self.recovery_strategy == RecoveryStrategy.SKIP_VALIDATION:
            self.logger.warning(f"Skipping validation for {subject} due to recovery strategy")
            return {
                "compatible": True,
                "reason": "Validation skipped due to recovery strategy",
                "recovery_applied": "skip_validation",
                "original_failure": failed_result
            }
        
        elif self.recovery_strategy == RecoveryStrategy.GRACEFUL_DEGRADATION:
            return self._graceful_degradation(subject, new_schema, failed_result, context)
        
        else:
            raise ValueError(f"Unknown recovery strategy: {self.recovery_strategy}")
    
    def _try_fallback_schemas(self, 
                            subject: str, 
                            compatibility_level: CompatibilityLevel,
                            failed_result: Dict[str, Any]) -> Dict[str, Any]:
        """Try fallback schemas in priority order."""
        
        fallback_schemas = self.fallback_schemas.get(subject, [])
        if not fallback_schemas:
            self.logger.error(f"No fallback schemas registered for {subject}")
            raise ValueError(f"Schema validation failed and no fallback available: {failed_result['reason']}")
        
        self.logger.info(f"Trying {len(fallback_schemas)} fallback schemas for {subject}")
        
        for i, fallback_schema in enumerate(fallback_schemas):
            try:
                result = self.validator.validate_schema_compatibility(
                    subject, fallback_schema, compatibility_level
                )
                if result["compatible"]:
                    self.metrics.fallback_used += 1
                    self.logger.info(f"Successfully using fallback schema {i+1} for {subject}")
                    result["recovery_applied"] = "fallback_schema"
                    result["fallback_index"] = i
                    result["original_failure"] = failed_result
                    return result
            except Exception as e:
                self.logger.warning(f"Fallback schema {i+1} also failed: {e}")
                continue
        
        # All fallbacks failed
        raise ValueError(f"All fallback schemas failed for {subject}")
    
    def _graceful_degradation(self, 
                            subject: str, 
                            new_schema: str,
                            failed_result: Dict[str, Any],
                            context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply graceful degradation strategy."""
        
        # Analyze the breaking changes to determine if we can proceed
        if failed_result.get("version"):
            try:
                old_schema = self.validator.client.get_version(subject, failed_result["version"]).schema.schema_str
                breaking_changes = self.validator.validate_breaking_changes(old_schema, new_schema)
                
                # Determine if breaking changes are acceptable
                acceptable_breaking_changes = [
                    "Added field with default",
                    "Removed optional field"
                ]
                
                critical_breaking_changes = [
                    change for change in breaking_changes["breaking_changes"]
                    if not any(acceptable in change for acceptable in acceptable_breaking_changes)
                ]
                
                if not critical_breaking_changes:
                    self.logger.info(f"Proceeding with graceful degradation - only acceptable breaking changes detected")
                    return {
                        "compatible": True,
                        "reason": "Graceful degradation applied - breaking changes are acceptable",
                        "recovery_applied": "graceful_degradation",
                        "breaking_changes": breaking_changes,
                        "original_failure": failed_result
                    }
                
            except Exception as e:
                self.logger.error(f"Failed to analyze breaking changes: {e}")
        
        # Cannot proceed with graceful degradation
        raise ValueError(f"Graceful degradation not possible: {failed_result['reason']}")
    
    def _handle_validation_exception(self, 
                                   subject: str, 
                                   new_schema: str,
                                   exception: Exception,
                                   context: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Handle validation exceptions based on recovery strategy."""
        
        if self.recovery_strategy == RecoveryStrategy.FAIL_FAST:
            raise exception
        
        elif self.recovery_strategy in [RecoveryStrategy.SKIP_VALIDATION, RecoveryStrategy.GRACEFUL_DEGRADATION]:
            self.logger.error(f"Validation exception handled by recovery strategy: {exception}")
            return {
                "compatible": True,
                "reason": f"Exception handled by {self.recovery_strategy.value}",
                "recovery_applied": self.recovery_strategy.value,
                "original_exception": str(exception)
            }
        
        else:
            raise exception
    
    def _get_cached_compatibility(self, 
                                subject: str, 
                                schema: str, 
                                compatibility_level: CompatibilityLevel) -> Optional[Dict[str, Any]]:
        """Get cached compatibility result if available and not expired."""
        
        cache_key = f"{subject}:{hash(schema)}:{compatibility_level.value}"
        cached = self._compatibility_cache.get(cache_key)
        
        if cached and time.time() - cached["timestamp"] < self.cache_ttl:
            return cached["result"]
        
        return None
    
    def _cache_compatibility_result(self, 
                                  subject: str, 
                                  schema: str,
                                  compatibility_level: CompatibilityLevel,
                                  result: Dict[str, Any]):
        """Cache compatibility result."""
        
        cache_key = f"{subject}:{hash(schema)}:{compatibility_level.value}"
        self._compatibility_cache[cache_key] = {
            "result": result,
            "timestamp": time.time()
        }
    
    def get_metrics_report(self) -> Dict[str, Any]:
        """Get comprehensive metrics report."""
        return {
            "validation_attempts": self.metrics.validation_attempts,
            "validation_successes": self.metrics.validation_successes,
            "validation_failures": self.metrics.validation_failures,
            "success_rate_percent": self.metrics.success_rate,
            "fallback_used": self.metrics.fallback_used,
            "average_processing_time_ms": self.metrics.average_processing_time * 1000,
            "total_processing_time_ms": self.metrics.total_processing_time * 1000,
            "breaking_changes_detected": self.metrics.breaking_changes_detected,
            "compatibility_checks": self.metrics.compatibility_checks,
            "cache_size": len(self._compatibility_cache),
            "registered_fallback_subjects": len(self.fallback_schemas)
        }
    
    def clear_cache(self):
        """Clear all cached results."""
        self._schema_cache.clear()
        self._compatibility_cache.clear()
        self.logger.info("Schema evolution cache cleared")


# Production-ready schema evolution decorator
def with_schema_evolution_monitoring(metrics_callback: Optional[Callable] = None):
    """Decorator to add schema evolution monitoring to assets."""
    
    def decorator(asset_func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            logger = get_dagster_logger()
            
            try:
                result = asset_func(*args, **kwargs)
                
                # Record success metrics
                processing_time = time.time() - start_time
                if metrics_callback:
                    metrics_callback({
                        "status": "success",
                        "processing_time": processing_time,
                        "asset_name": asset_func.__name__
                    })
                
                logger.info(f"Asset {asset_func.__name__} completed successfully in {processing_time:.3f}s")
                return result
                
            except Exception as e:
                # Record failure metrics
                processing_time = time.time() - start_time
                if metrics_callback:
                    metrics_callback({
                        "status": "failure", 
                        "processing_time": processing_time,
                        "asset_name": asset_func.__name__,
                        "error": str(e)
                    })
                
                logger.error(f"Asset {asset_func.__name__} failed after {processing_time:.3f}s: {e}")
                raise
        
        return wrapper
    return decorator