from abc import ABC, abstractmethod
import time
from typing import Dict, Any, List, Optional
from prototype.governance.rate_limit import TokenBucket

class BaseConnector(ABC):
    def __init__(self, name: str, rate_limit_capacity: int, refill_rate: float, cache_ttl: int = 60):
        self.name = name
        self.rate_limiter = TokenBucket(rate_limit_capacity, refill_rate)
        self.cache: Dict[str, Any] = {}
        self.cache_ttl = cache_ttl

    @abstractmethod
    def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        pass

    def get_data(self, query_key: str, query_context: Dict[str, Any], max_staleness_ms: int = 0) -> Dict[str, Any]:
        """
        Retrieves data with caching and rate limit logic.
        """
        now = time.time()
        cached_entry = self.cache.get(query_key)

        # Check cache only if max_staleness_ms > 0
        if max_staleness_ms > 0 and cached_entry:
            age_ms = (now - cached_entry["timestamp"]) * 1000
            if age_ms <= max_staleness_ms:
                return {
                    "data": cached_entry["data"],
                    "freshness_ms": int(age_ms),
                    "from_cache": True,
                    "rate_limit_status": self.rate_limiter.get_status()
                }

        # Rate limit check before live fetch
        if not self.rate_limiter.consume():
            # Fallback to cache if available, even if stale, when rate limited? 
            # No, for this prototype, let's strictly return 429 to test the governor.
            return {
                "error": "Rate limit exceeded",
                "status_code": 429,
                "retry_after": 5, # Simulated retry seconds
                "rate_limit_status": self.rate_limiter.get_status()
            }

        # Live fetch
        start_time = time.time()
        data = self.fetch_data(query_context)
        fetch_time_ms = (time.time() - start_time) * 1000

        # Update cache
        self.cache[query_key] = {
            "data": data,
            "timestamp": now
        }

        return {
            "data": data,
            "freshness_ms": 0,
            "from_cache": False,
            "fetch_time_ms": int(fetch_time_ms),
            "rate_limit_status": self.rate_limiter.get_status()
        }
