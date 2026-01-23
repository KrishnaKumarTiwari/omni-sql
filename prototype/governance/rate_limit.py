import time
import threading

class TokenBucket:
    def __init__(self, capacity: int, refill_rate: float):
        """
        :param capacity: Maximum number of tokens in the bucket.
        :param refill_rate: Tokens added per second.
        """
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self.lock = threading.Lock()

    def consume(self, amount: int = 1) -> bool:
        with self.lock:
            self._refill()
            if self.tokens >= amount:
                self.tokens -= amount
                return True
            return False

    def _refill(self):
        now = time.time()
        delta = now - self.last_refill
        new_tokens = delta * self.refill_rate
        self.tokens = min(self.capacity, self.tokens + new_tokens)
        self.last_refill = now

    def get_status(self):
        with self.lock:
            self._refill()
            return {
                "remaining": int(self.tokens),
                "capacity": self.capacity
            }
