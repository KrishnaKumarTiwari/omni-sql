"""
Freshness Cache Implementation

Provides TTL-based caching for connector data to support
the max_staleness_ms parameter and reduce SaaS API calls.
"""

import time
import hashlib
import threading
from typing import Any, Dict, List, Optional


class CacheEntry:
    """Represents a single cache entry with TTL"""
    
    def __init__(self, data: List[Dict[str, Any]], timestamp: float):
        self.data = data
        self.timestamp = timestamp
    
    def age_ms(self) -> int:
        """Returns age of cache entry in milliseconds"""
        return int((time.time() - self.timestamp) * 1000)
    
    def is_fresh(self, max_staleness_ms: int) -> bool:
        """Check if cache entry is still fresh"""
        return self.age_ms() <= max_staleness_ms


class FreshnessCache:
    """
    TTL-based cache for connector data.
    
    Features:
    - Thread-safe operations
    - Automatic expiration based on max_staleness_ms
    - Cache key generation from connector + filters
    - Hit/miss tracking for metrics
    """
    
    def __init__(self, default_ttl_ms: int = 60000):
        """
        Initialize cache.
        
        Args:
            default_ttl_ms: Default TTL in milliseconds (default: 60s)
        """
        self.default_ttl_ms = default_ttl_ms
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0
    
    def _generate_key(self, connector_id: str, filters: Optional[Dict[str, Any]] = None) -> str:
        """
        Generate cache key from connector ID and filters.
        
        Args:
            connector_id: Connector identifier (e.g., "github", "jira")
            filters: Optional filter parameters
        
        Returns:
            Cache key string
        """
        key_parts = [connector_id]
        if filters:
            # Sort filters for consistent key generation
            filter_str = str(sorted(filters.items()))
            key_parts.append(filter_str)
        
        key = ":".join(key_parts)
        # Hash for consistent length
        return hashlib.md5(key.encode()).hexdigest()
    
    def get(
        self,
        connector_id: str,
        max_staleness_ms: int,
        filters: Optional[Dict[str, Any]] = None
    ) -> Optional[tuple[List[Dict[str, Any]], int]]:
        """
        Retrieve data from cache if fresh enough.
        
        Args:
            connector_id: Connector identifier
            max_staleness_ms: Maximum acceptable staleness in milliseconds
            filters: Optional filter parameters
        
        Returns:
            Tuple of (data, age_ms) if cache hit, None if miss
        """
        with self._lock:
            key = self._generate_key(connector_id, filters)
            
            if key not in self._cache:
                self._misses += 1
                return None
            
            entry = self._cache[key]
            
            # Check if entry is still fresh
            if not entry.is_fresh(max_staleness_ms):
                # Stale entry, remove it
                del self._cache[key]
                self._misses += 1
                return None
            
            self._hits += 1
            return (entry.data, entry.age_ms())
    
    def put(
        self,
        connector_id: str,
        data: List[Dict[str, Any]],
        filters: Optional[Dict[str, Any]] = None
    ):
        """
        Store data in cache.
        
        Args:
            connector_id: Connector identifier
            data: Data to cache
            filters: Optional filter parameters
        """
        with self._lock:
            key = self._generate_key(connector_id, filters)
            self._cache[key] = CacheEntry(data, time.time())
    
    def invalidate(self, connector_id: str, filters: Optional[Dict[str, Any]] = None):
        """
        Invalidate specific cache entry.
        
        Args:
            connector_id: Connector identifier
            filters: Optional filter parameters
        """
        with self._lock:
            key = self._generate_key(connector_id, filters)
            if key in self._cache:
                del self._cache[key]
    
    def clear(self):
        """Clear all cache entries"""
        with self._lock:
            self._cache.clear()
            self._hits = 0
            self._misses = 0
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        with self._lock:
            total = self._hits + self._misses
            hit_rate = (self._hits / total * 100) if total > 0 else 0
            
            return {
                "hits": self._hits,
                "misses": self._misses,
                "total_requests": total,
                "hit_rate_percent": round(hit_rate, 2),
                "entries": len(self._cache)
            }
    
    def cleanup_expired(self, max_age_ms: int):
        """
        Remove expired entries older than max_age_ms.
        
        Args:
            max_age_ms: Maximum age in milliseconds
        """
        with self._lock:
            expired_keys = [
                key for key, entry in self._cache.items()
                if not entry.is_fresh(max_age_ms)
            ]
            for key in expired_keys:
                del self._cache[key]
            
            return len(expired_keys)
