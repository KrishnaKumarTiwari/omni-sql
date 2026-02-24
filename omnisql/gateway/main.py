from __future__ import annotations
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import redis.asyncio as aioredis
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from pydantic import BaseModel

from omnisql.cache.redis_cache import RedisCache
from omnisql.connectors.base import AsyncBaseConnector
from omnisql.connectors.github import AsyncGitHubConnector
from omnisql.connectors.jira import AsyncJiraConnector
from omnisql.connectors.linear import AsyncLinearConnector
from omnisql.engine.federated_engine import AsyncFederatedEngine
from omnisql.governance.redis_rate_limiter import RedisRateLimiter
from omnisql.security.oidc import OIDCValidator
from omnisql.security.opa_client import OPAClient
from omnisql.tenant.models import ConnectorConfig
from omnisql.tenant.registry import TenantRegistry

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Metrics (same names as prototype for backward compatibility)
# ---------------------------------------------------------------------------
QUERY_COUNT = Counter(
    "omnisql_queries_total",
    "Total SQL queries processed",
    ["status", "tenant_id"],
)
QUERY_LATENCY = Histogram(
    "omnisql_query_latency_seconds",
    "Query execution latency",
    ["tenant_id"],
    buckets=[0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

# ---------------------------------------------------------------------------
# Shared process-level resources (populated in lifespan)
# ---------------------------------------------------------------------------
_registry: Optional[TenantRegistry] = None
_engine: Optional[AsyncFederatedEngine] = None
_oidc: Optional[OIDCValidator] = None
_opa: Optional[OPAClient] = None
_redis: Optional[aioredis.Redis] = None


def _make_mock_connector_config(connector_id: str) -> ConnectorConfig:
    """Build a mock ConnectorConfig for dev/demo mode."""
    return ConnectorConfig(
        connector_id=connector_id,
        base_url="mock",
        auth_type="bearer",
        credential_ref="",
        rate_limit_capacity=50,
        rate_limit_refill_rate=10.0,
        freshness_ttl_ms=60_000,
        pushable_filters=["status", "team_id", "project", "priority"],
    )


def _build_connectors(
    cache: RedisCache, rate_limiter: RedisRateLimiter
) -> Dict[str, AsyncBaseConnector]:
    """Build the global connector map (shared across all tenants in demo mode)."""
    mock_cfg = lambda cid: _make_mock_connector_config(cid)
    return {
        "github": AsyncGitHubConnector(mock_cfg("github"), rate_limiter, cache),
        "jira": AsyncJiraConnector(mock_cfg("jira"), rate_limiter, cache),
        "linear": AsyncLinearConnector(mock_cfg("linear"), rate_limiter, cache),
    }


def _init_tracing() -> None:
    """
    Initialize OpenTelemetry tracing.

    - OTEL_EXPORTER_OTLP_ENDPOINT set → OTLP HTTP exporter (Jaeger, Tempo, etc.)
    - Otherwise → ConsoleSpanExporter (visible in server stdout for demo)
    """
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import (
            SimpleSpanProcessor,
            BatchSpanProcessor,
            ConsoleSpanExporter,
        )
        from opentelemetry.sdk.resources import Resource

        resource = Resource.create({
            "service.name": "omnisql-gateway",
            "service.version": "1.0.0",
        })
        provider = TracerProvider(resource=resource)

        otlp_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
        if otlp_endpoint:
            try:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                    OTLPSpanExporter,
                )
                exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
                provider.add_span_processor(BatchSpanProcessor(exporter))
                logger.info("OpenTelemetry: OTLP exporter → %s", otlp_endpoint)
            except ImportError:
                logger.warning(
                    "opentelemetry-exporter-otlp-proto-http not installed; "
                    "falling back to console"
                )
                provider.add_span_processor(
                    SimpleSpanProcessor(ConsoleSpanExporter())
                )
        else:
            # Console exporter for local demo visibility
            provider.add_span_processor(
                SimpleSpanProcessor(ConsoleSpanExporter())
            )
            logger.info("OpenTelemetry: ConsoleSpanExporter (set OTEL_EXPORTER_OTLP_ENDPOINT for production)")

        trace.set_tracer_provider(provider)
    except Exception as exc:
        logger.warning("OpenTelemetry init failed (non-fatal): %s", exc)


# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _registry, _engine, _oidc, _opa, _redis

    # 0. Tracing (must be first — other modules read the global provider)
    _init_tracing()

    # 1. Tenant registry
    config_dir = os.environ.get("TENANT_CONFIG_DIR", "configs/tenants")
    _registry = TenantRegistry(config_dir=config_dir)
    try:
        _registry.load_all()
    except FileNotFoundError:
        logger.warning("Tenant config dir not found: %s — no tenants loaded", config_dir)

    # 2. Redis (with graceful fallback for local dev without Redis)
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    try:
        _redis = aioredis.from_url(redis_url, decode_responses=False)
        await _redis.ping()
        logger.info("Redis connected: %s", redis_url)
    except Exception as exc:
        logger.warning("Redis unavailable (%s) — cache/rate-limit disabled", exc)
        _redis = None

    cache = RedisCache(_redis) if _redis else _NullCache()
    rate_limiter = RedisRateLimiter(_redis) if _redis else _NullRateLimiter()

    # 3. Connectors + engine
    connectors = _build_connectors(cache, rate_limiter)
    _engine = AsyncFederatedEngine(connectors, cache, rate_limiter)

    # 4. Security
    jwks_url = os.environ.get("JWKS_URL", "")
    audience = os.environ.get("JWT_AUDIENCE", "omnisql-dev")
    _oidc = OIDCValidator(jwks_url=jwks_url, audience=audience)

    opa_url = os.environ.get("OPA_URL", "")
    _opa = OPAClient(opa_url=opa_url)

    logger.info(
        "OmniSQL production gateway started. Tenants: %s",
        _registry.all_tenant_ids(),
    )

    yield

    # Shutdown: close connections
    for conn in connectors.values():
        await conn.close()
    if _redis:
        await _redis.aclose()
    if _opa:
        await _opa.close()
    logger.info("OmniSQL gateway shut down.")


app = FastAPI(title="OmniSQL Production Gateway", version="1.0.0", lifespan=lifespan)


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    sql: str
    metadata: Optional[Dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/v1/query")
async def execute_query(
    request: QueryRequest,
    x_tenant_id: str = Header(..., description="Tenant identifier (e.g. acme_corp)"),
    authorization: str = Header(..., description="Bearer <token>"),
):
    """
    Execute a federated SQL query.

    Headers:
      X-Tenant-ID:   Required. Identifies the tenant.
      Authorization: Required. 'Bearer <token>' (dev tokens: token_dev, token_qa, token_web_dev).

    Returns 400 for unknown tables, 401 for invalid token, 429 for rate limit, 500 for engine errors.
    """
    trace_id = (request.metadata or {}).get("trace_id", str(uuid.uuid4()))
    max_staleness_ms = (request.metadata or {}).get("max_staleness_ms", 0)

    # 1. Resolve tenant
    tenant_cfg = _registry.get(x_tenant_id) if _registry else None
    if not tenant_cfg:
        # In demo mode without YAML configs, synthesize a default tenant
        tenant_cfg = _demo_tenant(x_tenant_id)

    # 2. Authenticate
    token = authorization.removeprefix("Bearer ").strip()
    try:
        security_ctx = await _oidc.validate(token, tenant_cfg)
    except HTTPException:
        QUERY_COUNT.labels(status="401", tenant_id=x_tenant_id).inc()
        raise

    # 3. Execute
    start_time = time.time()
    try:
        result = await _engine.execute_query(
            sql=request.sql,
            tenant_cfg=tenant_cfg,
            security_ctx=security_ctx,
            max_staleness_ms=max_staleness_ms,
        )
    except Exception as exc:
        QUERY_COUNT.labels(status="500", tenant_id=x_tenant_id).inc()
        raise HTTPException(status_code=500, detail=str(exc))

    duration = time.time() - start_time

    # 4. Handle known error codes from engine
    if "error" in result:
        status_code = result.get("status_code", 500)
        QUERY_COUNT.labels(status=str(status_code), tenant_id=x_tenant_id).inc()
        if status_code == 429:
            return JSONResponse(
                status_code=429,
                headers={"Retry-After": "5"},
                content={
                    "error": "RATE_LIMIT_EXHAUSTED",
                    "details": "Downstream connector budget exhausted. "
                               "Retry after the indicated interval or use a higher "
                               "max_staleness_ms to serve from cache.",
                    "retry_after_seconds": 5,
                    "trace_id": trace_id,
                },
            )
        if status_code == 504:
            return JSONResponse(
                status_code=504,
                content={
                    "error": "SOURCE_TIMEOUT",
                    "details": "Upstream SaaS connector did not respond within deadline.",
                    "trace_id": trace_id,
                },
            )
        raise HTTPException(status_code=status_code, detail=result["error"])

    QUERY_LATENCY.labels(tenant_id=x_tenant_id).observe(duration)
    QUERY_COUNT.labels(status="200", tenant_id=x_tenant_id).inc()

    result["trace_id"] = trace_id
    return result


@app.get("/health")
async def health():
    """Kubernetes liveness/readiness probe."""
    checks: Dict[str, str] = {}

    # Redis
    if _redis:
        try:
            await _redis.ping()
            checks["redis"] = "ok"
        except Exception as exc:
            checks["redis"] = f"error: {exc}"
    else:
        checks["redis"] = "disabled"

    # Tenant registry
    checks["tenants"] = str(_registry.count()) if _registry else "0"

    all_ok = all(v in ("ok", "disabled") or v.isdigit() for v in checks.values())
    return JSONResponse(
        status_code=200 if all_ok else 503,
        content={"status": "ok" if all_ok else "degraded", "checks": checks},
    )


@app.get("/metrics")
async def metrics():
    """Prometheus scrape endpoint."""
    return JSONResponse(
        content=generate_latest().decode(), media_type=CONTENT_TYPE_LATEST
    )


# ---------------------------------------------------------------------------
# Demo tenant (fallback when no YAML configs are loaded)
# ---------------------------------------------------------------------------

def _demo_tenant(tenant_id: str):
    """
    Synthesize a demo TenantConfig using mock connectors.
    Matches the prototype's behavior exactly so the web console works
    without any YAML config files.
    """
    from omnisql.tenant.models import (
        TenantConfig, ConnectorConfig, RLSRule, CLSRule
    )
    return TenantConfig(
        tenant_id=tenant_id,
        display_name=f"Demo Tenant ({tenant_id})",
        api_budget=1000,
        connector_configs={
            "github": ConnectorConfig(
                connector_id="github", base_url="mock", auth_type="bearer",
                credential_ref="", rate_limit_capacity=50, rate_limit_refill_rate=10.0,
                freshness_ttl_ms=30_000, pushable_filters=["status", "team_id", "author"],
            ),
            "jira": ConnectorConfig(
                connector_id="jira", base_url="mock", auth_type="basic",
                credential_ref="", rate_limit_capacity=50, rate_limit_refill_rate=10.0,
                freshness_ttl_ms=60_000, pushable_filters=["status", "project", "priority"],
            ),
            "linear": ConnectorConfig(
                connector_id="linear", base_url="mock", auth_type="bearer",
                credential_ref="", rate_limit_capacity=50, rate_limit_refill_rate=0.5,
                freshness_ttl_ms=60_000, pushable_filters=["status"],
            ),
        },
        rls_rules=[
            RLSRule(connector_id="github", rule_expr="team_id == user.team_id"),
            RLSRule(connector_id="jira", rule_expr="project.lower() == user.team_id"),
        ],
        cls_rules=[
            CLSRule(connector_id="github", column="author_email", action="hash_hmac",
                    condition="user.pii_access == false"),
            CLSRule(connector_id="github", column="author", action="block",
                    condition='user.role == "qa"'),
        ],
        table_registry={
            "github.pull_requests": {"connector": "github", "fetch_key": "all_prs"},
            "jira.issues":          {"connector": "jira",   "fetch_key": "all_issues"},
            "linear.issues":        {"connector": "linear", "fetch_key": "all_issues"},
        },
    )


# ---------------------------------------------------------------------------
# Null implementations for Redis-unavailable mode
# ---------------------------------------------------------------------------

class _NullCache:
    """No-op cache used when Redis is unavailable (local dev without Docker)."""
    async def get(self, *a, **kw): return None
    async def put(self, *a, **kw): pass
    async def get_stats(self, *a, **kw): return {"redis": "disabled"}
    async def ping(self): return False


class _NullRateLimiter:
    """No-op rate limiter — always allows (for local dev without Redis)."""
    async def consume(self, *a, **kw): return True
    async def get_status(self, *a, **kw): return {"remaining": 9999, "capacity": 9999, "connector_id": ""}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
