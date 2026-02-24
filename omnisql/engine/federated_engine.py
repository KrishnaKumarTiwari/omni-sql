from __future__ import annotations
import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import duckdb
import pandas as pd
from opentelemetry import trace

from omnisql.cache.redis_cache import RedisCache
from omnisql.connectors.base import AsyncBaseConnector
from omnisql.governance.redis_rate_limiter import RedisRateLimiter
from omnisql.planner.models import ExecutionDAG, FetchNode
from omnisql.planner.query_planner import QueryPlanner
from omnisql.tenant.models import TenantConfig

logger = logging.getLogger(__name__)
tracer = trace.get_tracer("omnisql.engine")


class AsyncFederatedEngine:
    """
    Production replacement for FederatedEngine (prototype/engine.py).

    Key differences:
    - Per-request DuckDB connection (not shared — prototype's shared conn is not
      thread-safe when views are registered concurrently under load).
    - Connector fetches fan out via asyncio.gather() → latency = max(APIs), not sum.
    - QueryPlanner replaces string-matching _detect_tables() / _extract_filters().
    - Cache + rate limiting go through Redis (distributed across pods).
    - RLS/CLS driven by tenant config rules, not hardcoded if/else.
    - OpenTelemetry tracing spans for full request lifecycle observability.

    Flow per request:
      parse → plan → parallel fetch (DAG waves) → barrier →
      RLS/CLS → register DuckDB views → execute SQL → return results
    """

    def __init__(
        self,
        connectors: Dict[str, AsyncBaseConnector],
        cache: RedisCache,
        rate_limiter: RedisRateLimiter,
    ) -> None:
        self._connectors = connectors
        self._cache = cache
        self._rate_limiter = rate_limiter

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def execute_query(
        self,
        sql: str,
        tenant_cfg: TenantConfig,
        security_ctx: Any,   # TenantSecurityContext (avoid circular import)
        max_staleness_ms: int = 0,
    ) -> Dict[str, Any]:
        """
        Full async pipeline with tracing:
        1. Plan SQL → ExecutionDAG
        2. Execute DAG (parallel fan-out across waves)
        3. Apply RLS + CLS per source
        4. Register DuckDB temp views
        5. Execute rewritten SQL in DuckDB
        6. Return rows + metadata + connector_timings
        """
        from omnisql.security.enforcer import apply_rls, apply_cls

        with tracer.start_as_current_span(
            "engine.execute_query",
            attributes={
                "sql": sql,
                "tenant_id": tenant_cfg.tenant_id,
                "user_id": getattr(security_ctx, "user_id", ""),
                "max_staleness_ms": max_staleness_ms,
            },
        ) as root_span:
            warnings: List[str] = []
            connector_timings: Dict[str, Dict[str, Any]] = {}

            # 1. Plan
            plan_start = time.time()
            with tracer.start_as_current_span("engine.plan"):
                planner = QueryPlanner(tenant_cfg)
                try:
                    dag = planner.plan(sql)
                except ValueError as exc:
                    return {"error": str(exc), "status_code": 400}
            planning_ms = int((time.time() - plan_start) * 1000)

            # 2. Execute DAG
            fetch_start = time.time()
            try:
                node_results = await self._execute_dag(
                    dag, tenant_cfg, max_staleness_ms, connector_timings,
                )
            except RuntimeError as exc:
                error_str = str(exc)
                if "RATE_LIMIT_EXHAUSTED" in error_str:
                    return {"error": error_str, "status_code": 429}
                if "SOURCE_TIMEOUT" in error_str:
                    return {"error": error_str, "status_code": 504}
                return {"error": error_str, "status_code": 500}
            fetch_total_ms = int((time.time() - fetch_start) * 1000)

            # 3. RLS + CLS — applied to each source's data before DuckDB sees it
            security_start = time.time()
            secured_datasets: Dict[str, List[Dict]] = {}
            raw_datasets: Dict[str, List[Dict]] = {}
            actual_freshness_ms = 0
            rate_limit_status: Dict = {}

            with tracer.start_as_current_span("engine.security"):
                for view_name, result in node_results.items():
                    data = result["data"]
                    connector_id = result["connector_id"]
                    actual_freshness_ms = max(
                        actual_freshness_ms, result.get("freshness_ms", 0)
                    )
                    rate_limit_status = result.get("rate_limit_status", {})

                    # Check for stale data warnings from connectors
                    if result.get("stale"):
                        warnings.append("STALE_DATA")

                    raw_datasets[view_name] = data
                    raw_count = len(data)
                    data = await apply_rls(connector_id, data, security_ctx)
                    data = await apply_cls(connector_id, data, security_ctx)
                    secured_datasets[view_name] = data

                    # ENTITLEMENT_DENIED: RLS filtered all rows from a non-empty source
                    if raw_count > 0 and len(data) == 0:
                        warnings.append("ENTITLEMENT_DENIED")

            security_ms = int((time.time() - security_start) * 1000)

            # 4. Register DuckDB views (per-request connection — thread-safe)
            duckdb_start = time.time()
            con = duckdb.connect(database=":memory:")
            try:
                with tracer.start_as_current_span("engine.duckdb"):
                    self._register_views(
                        con, secured_datasets, raw_datasets=raw_datasets
                    )
                    try:
                        result_df = con.execute(dag.rewritten_sql).df()
                    except Exception as exc:
                        return {
                            "error": f"SQL execution error: {exc}",
                            "status_code": 400,
                        }
            finally:
                con.close()
            duckdb_ms = int((time.time() - duckdb_start) * 1000)

            # 5. Build response
            total_ms = planning_ms + fetch_total_ms + security_ms + duckdb_ms
            root_span.set_attribute("engine.total_ms", total_ms)
            root_span.set_attribute("engine.planning_ms", planning_ms)
            root_span.set_attribute("engine.fetch_ms", fetch_total_ms)
            root_span.set_attribute("engine.security_ms", security_ms)
            root_span.set_attribute("engine.duckdb_ms", duckdb_ms)
            root_span.set_attribute("engine.rows_returned", len(result_df))

            cache_stats = await self._cache.get_stats(tenant_cfg.tenant_id)

            # De-duplicate warnings
            unique_warnings = list(dict.fromkeys(warnings))

            response: Dict[str, Any] = {
                "rows": result_df.to_dict(orient="records"),
                "columns": result_df.columns.tolist(),
                "freshness_ms": actual_freshness_ms,
                "rate_limit_status": rate_limit_status,
                "cache_stats": cache_stats,
                "from_cache": all(
                    r.get("from_cache") for r in node_results.values()
                ),
                "connector_timings": connector_timings,
                "timing": {
                    "total_ms": total_ms,
                    "planning_ms": planning_ms,
                    "fetch_ms": fetch_total_ms,
                    "security_ms": security_ms,
                    "duckdb_ms": duckdb_ms,
                },
            }
            if unique_warnings:
                response["warnings"] = unique_warnings

            return response

    # ------------------------------------------------------------------
    # DAG execution
    # ------------------------------------------------------------------

    async def _execute_dag(
        self,
        dag: ExecutionDAG,
        tenant_cfg: TenantConfig,
        max_staleness_ms: int,
        connector_timings: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict]:
        """
        Execute the DAG level by level.

        For each wave returned by dag.get_levels():
          - Launch all nodes concurrently with asyncio.gather()
          - Barrier: wait for all nodes in the wave to complete
          - Collect results keyed by view_name
        """
        all_results: Dict[str, Dict] = {}
        levels = dag.get_levels()

        logger.info(
            "Executing DAG: %d nodes in %d wave(s) for tenant=%s",
            len(dag.nodes), len(levels), tenant_cfg.tenant_id,
        )

        with tracer.start_as_current_span(
            "engine.execute_dag",
            attributes={"dag.nodes": len(dag.nodes), "dag.waves": len(levels)},
        ):
            for wave_idx, wave in enumerate(levels):
                logger.debug(
                    "Wave %d/%d: %s",
                    wave_idx + 1, len(levels),
                    [n.id for n in wave],
                )

                wave_results = await asyncio.gather(*[
                    self._execute_node(
                        node, tenant_cfg, max_staleness_ms, connector_timings,
                    )
                    for node in wave
                ])

                for view_name, result in wave_results:
                    all_results[view_name] = result

        return all_results

    async def _execute_node(
        self,
        node: FetchNode,
        tenant_cfg: TenantConfig,
        max_staleness_ms: int,
        connector_timings: Dict[str, Dict[str, Any]],
    ) -> tuple[str, Dict]:
        """
        Execute a single FetchNode with tracing.
        """
        connector = self._connectors.get(node.connector_id)
        if not connector:
            raise RuntimeError(
                f"No connector registered for '{node.connector_id}' "
                f"in tenant '{tenant_cfg.tenant_id}'"
            )

        node_start = time.time()
        with tracer.start_as_current_span(
            f"engine.fetch.{node.connector_id}",
            attributes={
                "connector.id": node.connector_id,
                "connector.table": node.table_name,
                "connector.pushdown_filters": str(node.pushdown_filters),
            },
        ) as span:
            result = await connector.get_data(
                tenant_id=tenant_cfg.tenant_id,
                fetch_key=node.fetch_key,
                query_context={
                    "filters": node.pushdown_filters,
                    "fetch_key": node.fetch_key,
                },
                max_staleness_ms=max_staleness_ms,
                filters=node.pushdown_filters if node.pushdown_filters else None,
            )

            node_ms = int((time.time() - node_start) * 1000)
            span.set_attribute("connector.total_ms", node_ms)
            span.set_attribute("connector.from_cache", result.get("from_cache", False))
            span.set_attribute("connector.rows", len(result.get("data", [])))

            # Record timing for response metadata
            connector_timings[node.connector_id] = {
                "fetch_ms": node_ms,
                "from_cache": result.get("from_cache", False),
                "rows": len(result.get("data", [])),
                "stale": result.get("stale", False),
            }

        return node.view_name, {
            "data": result["data"],
            "connector_id": node.connector_id,
            "freshness_ms": result.get("freshness_ms", 0),
            "from_cache": result.get("from_cache", False),
            "stale": result.get("stale", False),
            "rate_limit_status": result.get("rate_limit_status", {}),
        }

    # ------------------------------------------------------------------
    # DuckDB view registration
    # ------------------------------------------------------------------

    def _register_views(
        self,
        con: duckdb.DuckDBPyConnection,
        datasets: Dict[str, List[Dict]],
        raw_datasets: Optional[Dict[str, List[Dict]]] = None,
    ) -> None:
        """
        Register each dataset as a DuckDB temporary view.

        If RLS produces an empty list, we still need a schema-aware empty
        DataFrame so DuckDB can execute JOINs without "table not found" errors.
        Column names are inferred from raw_datasets (pre-RLS) when available.
        """
        for view_name, data in datasets.items():
            if data:
                df = pd.DataFrame(data)
            else:
                raw = (raw_datasets or {}).get(view_name, [])
                if raw:
                    df = pd.DataFrame(columns=list(raw[0].keys()))
                else:
                    df = pd.DataFrame({"_empty": pd.Series([], dtype="object")})
            con.register(view_name, df)
            logger.debug("Registered view: %s (%d rows)", view_name, len(data))
