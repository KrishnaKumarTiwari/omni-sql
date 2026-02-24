from __future__ import annotations
import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import msgpack
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


class RedisCache:
    """
    Distributed TTL cache for connector data.

    Replaces the in-memory FreshnessCache. Each entry is scoped by tenant_id
    so no cross-tenant data leakage is possible at the key level.

    Key schema:
        omnisql:cache:{tenant_id}:{connector_id}:{md5(sorted_filters)}

    Value: MessagePack-serialized dict:
        {"data": List[Dict], "fetched_at": float, "etag": str | None}

    TTL is set via native Redis EXPIRE (per connector freshness_ttl_ms).
    """

    KEY_PREFIX = "omnisql:cache"

    def __init__(self, redis_client: aioredis.Redis) -> None:
        self._redis = redis_client

    # ------------------------------------------------------------------
    # Key construction
    # ------------------------------------------------------------------

    def _build_key(
        self,
        tenant_id: str,
        connector_id: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> str:
        filter_str = json.dumps(sorted((filters or {}).items()), sort_keys=True)
        filter_hash = hashlib.md5(filter_str.encode()).hexdigest()[:12]
        return f"{self.KEY_PREFIX}:{tenant_id}:{connector_id}:{filter_hash}"

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    async def get(
        self,
        tenant_id: str,
        connector_id: str,
        max_staleness_ms: int,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[List[Dict], int]]:
        """
        Retrieve cached data if within the staleness budget.

        Redis TTL handles hard expiry (entry is gone after freshness_ttl_ms).
        This method performs an additional soft-freshness check via the stored
        fetched_at timestamp so callers can honor max_staleness_ms values
        shorter than the connector's configured TTL.

        Returns:
            (data, age_ms) on cache hit within budget, None otherwise.
        """
        key = self._build_key(tenant_id, connector_id, filters)
        raw = await self._redis.get(key)
        if raw is None:
            return None

        try:
            entry = msgpack.unpackb(raw, raw=False)
        except Exception as exc:
            logger.warning("Cache deserialization failed for %s: %s", key, exc)
            return None

        age_ms = int((time.time() - entry["fetched_at"]) * 1000)

        # max_staleness_ms == 0 means "live only" — bypass cache
        if max_staleness_ms == 0:
            return None
        if age_ms > max_staleness_ms:
            return None

        logger.debug("Cache HIT %s (age=%dms)", key, age_ms)
        return entry["data"], age_ms

    async def put(
        self,
        tenant_id: str,
        connector_id: str,
        data: List[Dict],
        ttl_ms: int,
        filters: Optional[Dict[str, Any]] = None,
        etag: Optional[str] = None,
    ) -> None:
        """Store data in Redis with the connector's configured TTL."""
        key = self._build_key(tenant_id, connector_id, filters)
        payload = {
            "data": data,
            "fetched_at": time.time(),
            "etag": etag,
        }
        ttl_seconds = max(1, ttl_ms // 1000)
        packed = msgpack.packb(payload, use_bin_type=True)
        await self._redis.set(key, packed, ex=ttl_seconds)
        logger.debug("Cache PUT %s (ttl=%ds, rows=%d)", key, ttl_seconds, len(data))

    async def invalidate(
        self,
        tenant_id: str,
        connector_id: str,
        filters: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Delete a specific cache entry."""
        key = self._build_key(tenant_id, connector_id, filters)
        await self._redis.delete(key)

    async def get_stats(self, tenant_id: str) -> Dict[str, Any]:
        """
        Return approximate cache statistics for a tenant.
        Uses SCAN (never KEYS) to avoid blocking Redis.
        """
        pattern = f"{self.KEY_PREFIX}:{tenant_id}:*"
        count = 0
        async for _ in self._redis.scan_iter(match=pattern, count=100):
            count += 1
        return {"tenant_id": tenant_id, "cached_entries": count}

    async def ping(self) -> bool:
        """Health check — returns True if Redis is reachable."""
        try:
            return await self._redis.ping()
        except Exception:
            return False
