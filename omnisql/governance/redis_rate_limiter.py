from __future__ import annotations
import logging
import time
from typing import Any, Dict

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


# Lua script: atomic token-bucket consume + refill.
# Runs as a single Redis command — no race conditions across pods.
#
# KEYS[1] = omnisql:ratelimit:{tenant_id}:{connector_id}
# ARGV[1] = capacity         (int)
# ARGV[2] = refill_rate      (float, tokens/second)
# ARGV[3] = amount           (int, tokens to consume, usually 1)
# ARGV[4] = now              (float, Unix timestamp)
#
# Returns: [allowed (0|1), remaining_tokens (int)]
_RATE_LIMIT_LUA = """
local key          = KEYS[1]
local capacity     = tonumber(ARGV[1])
local refill_rate  = tonumber(ARGV[2])
local requested    = tonumber(ARGV[3])
local now          = tonumber(ARGV[4])

local data        = redis.call('HMGET', key, 'tokens', 'last_refill')
local tokens      = tonumber(data[1]) or capacity
local last_refill = tonumber(data[2]) or now

local delta    = math.max(0, now - last_refill)
local new_tok  = math.min(capacity, tokens + delta * refill_rate)

local allowed = 0
if new_tok >= requested then
    new_tok = new_tok - requested
    allowed = 1
end

local ttl = math.ceil((capacity / refill_rate) * 2)
redis.call('HSET', key, 'tokens', tostring(new_tok), 'last_refill', tostring(now))
redis.call('EXPIRE', key, ttl)

return {allowed, math.floor(new_tok)}
"""


class RedisRateLimiter:
    """
    Distributed token-bucket rate limiter backed by Redis.

    Replaces the per-process TokenBucket. All pods share the same Redis key,
    so the budget is enforced globally across the fleet.

    Key schema:
        omnisql:ratelimit:{tenant_id}:{connector_id}

    Value: Redis hash with fields 'tokens' (float) and 'last_refill' (Unix ts).

    The Lua script handles refill + consume in a single atomic operation —
    no WATCH/MULTI/EXEC round-trips needed.
    """

    KEY_PREFIX = "omnisql:ratelimit"

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client
        self._script = self._redis.register_script(_RATE_LIMIT_LUA)

    def _build_key(self, tenant_id: str, connector_id: str) -> str:
        return f"{self.KEY_PREFIX}:{tenant_id}:{connector_id}"

    async def consume(
        self,
        tenant_id: str,
        connector_id: str,
        capacity: int,
        refill_rate: float,
        amount: int = 1,
    ) -> bool:
        """
        Attempt to consume `amount` tokens. Executes Lua atomically.

        Args:
            capacity:    From tenant_cfg.connector_configs[connector_id].rate_limit_capacity
            refill_rate: From tenant_cfg.connector_configs[connector_id].rate_limit_refill_rate
            amount:      Tokens to consume (default 1 per API call).

        Returns:
            True if tokens were available and consumed, False if rate limited.
        """
        key = self._build_key(tenant_id, connector_id)
        now = time.time()
        result = await self._script(
            keys=[key],
            args=[capacity, refill_rate, amount, now],
        )
        allowed = bool(result[0])
        remaining = int(result[1])
        if not allowed:
            logger.warning(
                "Rate limit hit: tenant=%s connector=%s remaining=%d",
                tenant_id, connector_id, remaining,
            )
        return allowed

    async def get_status(
        self, tenant_id: str, connector_id: str, capacity: int
    ) -> Dict[str, Any]:
        """
        Return current bucket state without consuming tokens.
        Used for response metadata.
        """
        key = self._build_key(tenant_id, connector_id)
        tokens_raw = await self._redis.hget(key, "tokens")
        remaining = int(float(tokens_raw)) if tokens_raw else capacity
        return {
            "connector_id": connector_id,
            "remaining": remaining,
            "capacity": capacity,
        }
