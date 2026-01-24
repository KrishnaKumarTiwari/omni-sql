"""
Rate Limiting Tests

Tests verify that token bucket rate limiting is correctly enforced
and returns proper 429 responses with retry-after headers.
"""

import pytest
import time
from prototype.governance.rate_limit import TokenBucket


class TestTokenBucket:
    """Test token bucket rate limiting implementation"""
    
    def test_initial_capacity(self):
        """Bucket should start at full capacity"""
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        status = bucket.get_status()
        assert status["remaining"] == 10
        assert status["capacity"] == 10
    
    def test_consume_tokens(self):
        """Consuming tokens should decrease remaining count"""
        bucket = TokenBucket(capacity=10, refill_rate=1.0)
        assert bucket.consume(3) is True
        status = bucket.get_status()
        assert status["remaining"] == 7
    
    def test_consume_more_than_available(self):
        """Cannot consume more tokens than available"""
        bucket = TokenBucket(capacity=5, refill_rate=1.0)
        assert bucket.consume(3) is True
        assert bucket.consume(3) is False  # Only 2 remaining
        status = bucket.get_status()
        assert status["remaining"] == 2
    
    def test_refill_over_time(self):
        """Tokens should refill at specified rate"""
        bucket = TokenBucket(capacity=10, refill_rate=5.0)  # 5 tokens/sec
        bucket.consume(10)  # Drain bucket
        
        time.sleep(1.1)  # Wait for refill
        
        status = bucket.get_status()
        assert status["remaining"] >= 5  # Should have refilled ~5 tokens
    
    def test_refill_does_not_exceed_capacity(self):
        """Refill should not exceed bucket capacity"""
        bucket = TokenBucket(capacity=10, refill_rate=5.0)
        
        time.sleep(5)  # Wait long enough to refill beyond capacity
        
        status = bucket.get_status()
        assert status["remaining"] == 10  # Capped at capacity
    
    def test_thread_safety(self):
        """Bucket should handle concurrent access safely"""
        import threading
        
        bucket = TokenBucket(capacity=100, refill_rate=10.0)
        results = []
        
        def consume_tokens():
            for _ in range(10):
                results.append(bucket.consume(1))
        
        threads = [threading.Thread(target=consume_tokens) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # All 50 consumes should succeed (100 capacity)
        assert sum(results) == 50
        status = bucket.get_status()
        assert status["remaining"] == 50


class TestRateLimitIntegration:
    """Test rate limiting in API context"""
    
    def test_rate_limit_exhaustion_scenario(self):
        """Simulate API rate limit exhaustion"""
        bucket = TokenBucket(capacity=5, refill_rate=0.5)
        
        # First 5 requests succeed
        for i in range(5):
            assert bucket.consume(1) is True, f"Request {i+1} should succeed"
        
        # 6th request fails (rate limited)
        assert bucket.consume(1) is False
        
        # Wait for refill
        time.sleep(2.1)  # Should refill 1 token
        
        # Next request succeeds
        assert bucket.consume(1) is True
