"""
Freshness Cache Tests

Tests verify TTL-based caching behavior, cache hits/misses,
and freshness calculations.
"""

import pytest
import time
from prototype.cache.freshness import FreshnessCache, CacheEntry


class TestCacheEntry:
    """Test cache entry age and freshness calculations"""
    
    def test_cache_entry_age(self):
        """Cache entry should track age correctly"""
        entry = CacheEntry(data=[{"id": 1}], timestamp=time.time())
        time.sleep(0.1)
        assert entry.age_ms() >= 100
    
    def test_cache_entry_freshness(self):
        """Cache entry should determine freshness correctly"""
        entry = CacheEntry(data=[{"id": 1}], timestamp=time.time())
        assert entry.is_fresh(max_staleness_ms=1000) is True
        
        time.sleep(0.5)
        assert entry.is_fresh(max_staleness_ms=100) is False
        assert entry.is_fresh(max_staleness_ms=1000) is True


class TestFreshnessCache:
    """Test freshness cache operations"""
    
    def test_cache_miss_on_empty(self):
        """Cache miss on first request"""
        cache = FreshnessCache()
        result = cache.get("github", max_staleness_ms=5000)
        assert result is None
        
        stats = cache.get_stats()
        assert stats["misses"] == 1
        assert stats["hits"] == 0
    
    def test_cache_hit_after_put(self):
        """Cache hit after storing data"""
        cache = FreshnessCache()
        test_data = [{"pr_id": 1}, {"pr_id": 2}]
        
        cache.put("github", test_data)
        result = cache.get("github", max_staleness_ms=5000)
        
        assert result is not None
        data, age_ms = result
        assert data == test_data
        assert age_ms < 100  # Should be very fresh
        
        stats = cache.get_stats()
        assert stats["hits"] == 1
    
    def test_cache_expiration(self):
        """Expired cache entries should be treated as misses"""
        cache = FreshnessCache()
        test_data = [{"pr_id": 1}]
        
        cache.put("github", test_data)
        time.sleep(0.2)  # Wait 200ms
        
        # Request with 100ms staleness tolerance (expired)
        result = cache.get("github", max_staleness_ms=100)
        assert result is None
        
        stats = cache.get_stats()
        assert stats["misses"] == 1
    
    def test_cache_key_with_filters(self):
        """Different filters should create different cache keys"""
        cache = FreshnessCache()
        
        cache.put("github", [{"pr_id": 1}], filters={"status": "open"})
        cache.put("github", [{"pr_id": 2}], filters={"status": "merged"})
        
        result1 = cache.get("github", max_staleness_ms=5000, filters={"status": "open"})
        result2 = cache.get("github", max_staleness_ms=5000, filters={"status": "merged"})
        
        assert result1 is not None
        assert result2 is not None
        assert result1[0] != result2[0]  # Different data
    
    def test_cache_invalidation(self):
        """Invalidate should remove specific entry"""
        cache = FreshnessCache()
        cache.put("github", [{"pr_id": 1}])
        
        cache.invalidate("github")
        result = cache.get("github", max_staleness_ms=5000)
        
        assert result is None
    
    def test_cache_clear(self):
        """Clear should remove all entries"""
        cache = FreshnessCache()
        cache.put("github", [{"pr_id": 1}])
        cache.put("jira", [{"issue_key": "MOB-1"}])
        
        cache.clear()
        
        assert cache.get("github", max_staleness_ms=5000) is None
        assert cache.get("jira", max_staleness_ms=5000) is None
        
        stats = cache.get_stats()
        assert stats["entries"] == 0
    
    def test_cache_stats_hit_rate(self):
        """Stats should calculate hit rate correctly"""
        cache = FreshnessCache()
        test_data = [{"id": 1}]
        
        cache.put("github", test_data)
        
        # 3 hits
        for _ in range(3):
            cache.get("github", max_staleness_ms=5000)
        
        # 1 miss
        cache.get("jira", max_staleness_ms=5000)
        
        stats = cache.get_stats()
        assert stats["hits"] == 3
        assert stats["misses"] == 1
        assert stats["total_requests"] == 4
        assert stats["hit_rate_percent"] == 75.0
    
    def test_cleanup_expired(self):
        """Cleanup should remove old entries"""
        cache = FreshnessCache()
        
        cache.put("github", [{"pr_id": 1}])
        time.sleep(0.2)
        cache.put("jira", [{"issue_key": "MOB-1"}])
        
        # Cleanup entries older than 150ms
        removed = cache.cleanup_expired(max_age_ms=150)
        
        assert removed == 1  # Only github entry should be removed
        assert cache.get("github", max_staleness_ms=5000) is None
        assert cache.get("jira", max_staleness_ms=5000) is not None
    
    def test_thread_safety(self):
        """Cache should handle concurrent access safely"""
        import threading
        
        cache = FreshnessCache()
        test_data = [{"id": 1}]
        
        def put_data():
            for i in range(10):
                cache.put(f"connector_{i}", test_data)
        
        def get_data():
            for i in range(10):
                cache.get(f"connector_{i}", max_staleness_ms=5000)
        
        threads = []
        for _ in range(5):
            threads.append(threading.Thread(target=put_data))
            threads.append(threading.Thread(target=get_data))
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Should complete without errors
        stats = cache.get_stats()
        assert stats["total_requests"] >= 50
