from __future__ import annotations
import asyncio
import logging
import random
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import aiohttp
from opentelemetry import trace

from omnisql.cache.redis_cache import RedisCache
from omnisql.governance.redis_rate_limiter import RedisRateLimiter
from omnisql.tenant.models import ConnectorConfig

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("omnisql.connector")


class AsyncBaseConnector(ABC):
    """
    Abstract base for all async SaaS connectors.

    Responsibilities (all handled here — subclasses override only fetch_data):
    - Redis cache check before every fetch
    - Distributed rate limit check (Lua-atomic via RedisRateLimiter)
    - Exponential-backoff retry: 3 attempts, 2x delay, ±10% jitter
    - Shared aiohttp.ClientSession (connection pooling)
    - Both REST (GET) and GraphQL (POST /graphql) transports
    - Pagination: cursor-based (GraphQL) and Link-header (REST)
    - OpenTelemetry tracing spans for observability
    """

    MAX_RETRIES = 3
    RETRY_BASE_DELAY_S = 0.5
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(
        self,
        config: ConnectorConfig,
        rate_limiter: RedisRateLimiter,
        cache: RedisCache,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self.config = config
        self._rate_limiter = rate_limiter
        self._cache = cache
        self._session = session
        self._own_session = session is None
        self._logger = logging.getLogger(
            f"omnisql.connector.{config.connector_id}"
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session

    async def close(self) -> None:
        if self._own_session and self._session and not self._session.closed:
            await self._session.close()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def get_data(
        self,
        tenant_id: str,
        fetch_key: str,
        query_context: Dict[str, Any],
        max_staleness_ms: int = 0,
        filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Orchestrates: cache check → rate limit → fetch+retry → cache write-back.

        Returns:
            {data, freshness_ms, from_cache, rate_limit_status}
            May include "stale": True if returning stale data due to rate limit.

        Raises:
            RuntimeError("RATE_LIMIT_EXHAUSTED") — budget exhausted and no stale data.
            RuntimeError("SOURCE_TIMEOUT")        — all retries failed.
        """
        with tracer.start_as_current_span(
            f"connector.{self.config.connector_id}.get_data",
            attributes={
                "connector.id": self.config.connector_id,
                "connector.fetch_key": fetch_key,
                "connector.max_staleness_ms": max_staleness_ms,
            },
        ) as span:
            # 1. Cache check
            cache_start = time.time()
            cached = await self._cache.get(
                tenant_id, self.config.connector_id, max_staleness_ms, filters
            )
            cache_check_ms = int((time.time() - cache_start) * 1000)
            span.set_attribute("connector.cache_check_ms", cache_check_ms)

            if cached:
                data, age_ms = cached
                rate_status = await self._rate_limiter.get_status(
                    tenant_id, self.config.connector_id, self.config.rate_limit_capacity
                )
                span.set_attribute("connector.from_cache", True)
                span.set_attribute("connector.freshness_ms", age_ms)
                return {
                    "data": data,
                    "freshness_ms": age_ms,
                    "from_cache": True,
                    "rate_limit_status": rate_status,
                }

            # 2. Rate limit check
            allowed = await self._rate_limiter.consume(
                tenant_id,
                self.config.connector_id,
                self.config.rate_limit_capacity,
                self.config.rate_limit_refill_rate,
            )
            if not allowed:
                # STALE_DATA fallback: try to return any cached data regardless
                # of staleness rather than hard-failing with RATE_LIMIT_EXHAUSTED.
                stale = await self._cache.get(
                    tenant_id, self.config.connector_id,
                    max_staleness_ms=999_999_999,  # accept any age
                    filters=filters,
                )
                if stale:
                    stale_data, stale_age_ms = stale
                    rate_status = await self._rate_limiter.get_status(
                        tenant_id, self.config.connector_id,
                        self.config.rate_limit_capacity,
                    )
                    span.set_attribute("connector.stale_fallback", True)
                    span.set_attribute("connector.freshness_ms", stale_age_ms)
                    self._logger.warning(
                        "Rate limit exhausted for %s — returning stale data (age=%dms)",
                        self.config.connector_id, stale_age_ms,
                    )
                    return {
                        "data": stale_data,
                        "freshness_ms": stale_age_ms,
                        "from_cache": True,
                        "stale": True,
                        "rate_limit_status": rate_status,
                    }

                rate_status = await self._rate_limiter.get_status(
                    tenant_id, self.config.connector_id, self.config.rate_limit_capacity
                )
                span.set_attribute("connector.rate_limited", True)
                raise RuntimeError(
                    f"RATE_LIMIT_EXHAUSTED:{self.config.connector_id}:"
                    + str(rate_status.get("remaining", 0))
                )

            # 3. Fetch with retry
            fetch_start = time.time()
            data = await self._fetch_with_retry(query_context)
            fetch_ms = int((time.time() - fetch_start) * 1000)
            span.set_attribute("connector.fetch_ms", fetch_ms)
            span.set_attribute("connector.from_cache", False)
            span.set_attribute("connector.rows_fetched", len(data))

            # 4. Write-back to cache
            await self._cache.put(
                tenant_id,
                self.config.connector_id,
                data,
                self.config.freshness_ttl_ms,
                filters,
            )

            rate_status = await self._rate_limiter.get_status(
                tenant_id, self.config.connector_id, self.config.rate_limit_capacity
            )
            return {
                "data": data,
                "freshness_ms": fetch_ms,
                "from_cache": False,
                "rate_limit_status": rate_status,
            }

    # ------------------------------------------------------------------
    # Abstract: subclasses implement data fetching
    # ------------------------------------------------------------------

    @abstractmethod
    async def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Perform the actual data fetch.
        Raise aiohttp.ClientResponseError for HTTP errors so _fetch_with_retry
        can apply the retry policy.
        """
        ...

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------

    async def _fetch_with_retry(
        self, query_context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Wrap fetch_data() with exponential-backoff retry."""
        last_exc: Optional[Exception] = None
        for attempt in range(self.MAX_RETRIES):
            try:
                with tracer.start_as_current_span(
                    f"connector.{self.config.connector_id}.fetch_attempt",
                    attributes={"attempt": attempt + 1},
                ):
                    return await self.fetch_data(query_context)
            except aiohttp.ClientResponseError as exc:
                if exc.status not in self.RETRYABLE_STATUS_CODES:
                    raise  # 400/401/403/404 — non-retryable
                last_exc = exc
                if attempt < self.MAX_RETRIES - 1:
                    delay = self.RETRY_BASE_DELAY_S * (2 ** attempt)
                    jitter = random.uniform(0, delay * 0.1)
                    self._logger.warning(
                        "Retryable error (attempt %d/%d): %s — sleeping %.2fs",
                        attempt + 1, self.MAX_RETRIES, exc.status, delay + jitter,
                    )
                    await asyncio.sleep(delay + jitter)
            except Exception as exc:
                last_exc = exc
                break

        raise RuntimeError(
            f"SOURCE_TIMEOUT:{self.config.connector_id} after {self.MAX_RETRIES} attempts"
        ) from last_exc

    # ------------------------------------------------------------------
    # HTTP transports (shared by all subclasses)
    # ------------------------------------------------------------------

    def _auth_headers(self) -> Dict[str, str]:
        """Build Authorization header from config.credential_ref."""
        cred_ref = self.config.credential_ref
        if cred_ref.startswith("env://"):
            import os
            token = os.environ.get(cred_ref[6:], "")
        else:
            token = cred_ref  # raw token for dev/mock mode

        if self.config.auth_type == "bearer":
            return {"Authorization": f"Bearer {token}"}
        elif self.config.auth_type == "basic":
            import base64
            encoded = base64.b64encode(token.encode()).decode()
            return {"Authorization": f"Basic {encoded}"}
        return {}

    async def _http_get(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """Perform an authenticated GET request. Returns parsed JSON."""
        session = await self._get_session()
        url = self.config.base_url.rstrip("/") + path
        async with session.get(
            url, params=params or {}, headers=self._auth_headers()
        ) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def _graphql(
        self, query: str, variables: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Perform an authenticated GraphQL POST request.

        GraphQL-level errors (response["errors"]) are raised as RuntimeError.
        Returns response["data"].
        """
        session = await self._get_session()
        url = self.config.base_url.rstrip("/") + self.config.graphql_path
        payload = {"query": query, "variables": variables or {}}
        async with session.post(
            url, json=payload, headers={**self._auth_headers(), "Content-Type": "application/json"}
        ) as resp:
            resp.raise_for_status()
            body = await resp.json()

        if "errors" in body:
            raise RuntimeError(
                f"GraphQL error from {self.config.connector_id}: {body['errors']}"
            )
        return body.get("data", {})

    async def _paginate_graphql(
        self,
        query_template: str,
        variables: Dict[str, Any],
        data_path: str,  # e.g. "repository.pullRequests"
    ) -> List[Dict]:
        """
        Cursor-based pagination for GraphQL APIs (GitHub, Linear).

        Expects the GraphQL response to include pageInfo.endCursor and
        pageInfo.hasNextPage at data_path. Accumulates all pages.
        """
        all_nodes: List[Dict] = []
        cursor = None

        while True:
            if cursor:
                variables["cursor"] = cursor
            data = await self._graphql(query_template, variables)

            # Navigate nested path: "repository.pullRequests" → data["repository"]["pullRequests"]
            node = data
            for key in data_path.split("."):
                node = node[key]

            all_nodes.extend(node.get("nodes", []))
            page_info = node.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")

        return all_nodes

    async def _paginate_rest(
        self,
        path: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict]:
        """
        Link-header pagination for REST APIs (Jira, etc.).
        Follows 'rel="next"' links until exhausted.
        """
        all_items: List[Dict] = []
        session = await self._get_session()
        url: Optional[str] = self.config.base_url.rstrip("/") + path

        while url:
            async with session.get(
                url, params=params or {}, headers=self._auth_headers()
            ) as resp:
                resp.raise_for_status()
                body = await resp.json()

                # Normalize: list or {"values": [...]} (Jira)
                if isinstance(body, list):
                    all_items.extend(body)
                elif isinstance(body, dict):
                    all_items.extend(body.get("values", body.get("issues", [])))

                # Follow Link header for next page; clear params after first call
                link_header = resp.headers.get("Link", "")
                url = _parse_next_link(link_header)
                params = None

        return all_items


def _parse_next_link(link_header: str) -> Optional[str]:
    """Extract URL from 'Link: <url>; rel="next"' header."""
    for part in link_header.split(","):
        part = part.strip()
        if 'rel="next"' in part:
            url_part = part.split(";")[0].strip()
            return url_part.strip("<>")
    return None
