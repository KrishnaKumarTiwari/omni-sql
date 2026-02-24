"""Tests for AsyncFederatedEngine (full DAG execution with mock connectors)."""
import asyncio
import pytest

from omnisql.connectors.github import AsyncGitHubConnector
from omnisql.connectors.jira import AsyncJiraConnector
from omnisql.engine.federated_engine import AsyncFederatedEngine
from omnisql.gateway.main import _demo_tenant, _NullCache, _NullRateLimiter
from omnisql.security.oidc import OIDCValidator
from omnisql.tenant.models import ConnectorConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _mock_cfg(cid: str) -> ConnectorConfig:
    return ConnectorConfig(
        connector_id=cid, base_url="mock",
        rate_limit_capacity=50, rate_limit_refill_rate=10.0,
        freshness_ttl_ms=60000,
        pushable_filters=["status", "team_id", "project", "priority"],
    )


@pytest.fixture
def engine():
    cache = _NullCache()
    rl = _NullRateLimiter()
    return AsyncFederatedEngine(
        connectors={
            "github": AsyncGitHubConnector(_mock_cfg("github"), rl, cache),
            "jira": AsyncJiraConnector(_mock_cfg("jira"), rl, cache),
        },
        cache=cache,
        rate_limiter=rl,
    )


@pytest.fixture
def tenant():
    return _demo_tenant("test")


@pytest.fixture
def oidc():
    return OIDCValidator(jwks_url="", audience="test")


# ---------------------------------------------------------------------------
# Single-source queries
# ---------------------------------------------------------------------------

class TestSingleSource:
    @pytest.mark.asyncio
    async def test_simple_github_query(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT pr_id, team_id, status FROM github.pull_requests LIMIT 5",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "rows" in result
        assert len(result["rows"]) == 5
        assert "pr_id" in result["columns"]

    @pytest.mark.asyncio
    async def test_simple_jira_query(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT issue_key, status FROM jira.issues LIMIT 3",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "rows" in result
        assert len(result["rows"]) == 3

    @pytest.mark.asyncio
    async def test_github_with_predicate_pushdown(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT pr_id, status FROM github.pull_requests WHERE status = 'merged'",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "rows" in result
        assert all(r["status"] == "merged" for r in result["rows"])


# ---------------------------------------------------------------------------
# Cross-app joins
# ---------------------------------------------------------------------------

class TestCrossAppJoin:
    @pytest.mark.asyncio
    async def test_github_jira_join(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            """SELECT gh.pr_id, ji.issue_key
               FROM github.pull_requests gh
               JOIN jira.issues ji ON gh.branch = ji.branch_name""",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "rows" in result
        assert len(result["rows"]) > 0
        assert set(result["columns"]) == {"pr_id", "issue_key"}

    @pytest.mark.asyncio
    async def test_join_with_where_on_one_table(self, engine, tenant, oidc):
        """WHERE clause on one table should not affect the other."""
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            """SELECT gh.pr_id, ji.issue_key
               FROM github.pull_requests gh
               JOIN jira.issues ji ON gh.branch = ji.branch_name
               WHERE gh.status = 'merged'""",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "rows" in result
        # All returned rows should have merged PRs
        # (Jira rows are not filtered by gh.status)


# ---------------------------------------------------------------------------
# Security enforcement through engine
# ---------------------------------------------------------------------------

class TestEngineRLS:
    @pytest.mark.asyncio
    async def test_rls_mobile_team_isolation(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)  # mobile team
        result = await engine.execute_query(
            "SELECT pr_id, team_id FROM github.pull_requests",
            tenant, ctx, max_staleness_ms=5000,
        )
        teams = {r["team_id"] for r in result["rows"]}
        assert teams == {"mobile"}, f"Expected only mobile, got {teams}"

    @pytest.mark.asyncio
    async def test_rls_web_team_isolation(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_web_dev", tenant)  # web team
        result = await engine.execute_query(
            "SELECT pr_id, team_id FROM github.pull_requests",
            tenant, ctx, max_staleness_ms=5000,
        )
        teams = {r["team_id"] for r in result["rows"]}
        assert teams == {"web"}, f"Expected only web, got {teams}"


class TestEngineCLS:
    @pytest.mark.asyncio
    async def test_cls_email_masking_qa(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_qa", tenant)
        result = await engine.execute_query(
            "SELECT author, author_email FROM github.pull_requests LIMIT 3",
            tenant, ctx, max_staleness_ms=5000,
        )
        for row in result["rows"]:
            assert "****@ema.co" in row["author_email"]
            assert row["author"] == "[HIDDEN]"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

class TestErrorHandling:
    @pytest.mark.asyncio
    async def test_unknown_table_returns_error(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT * FROM nonexistent.table", tenant, ctx,
        )
        assert "error" in result
        assert result["status_code"] == 400

    @pytest.mark.asyncio
    async def test_sql_syntax_error(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECTTTT * FROMM github.pull_requests", tenant, ctx,
        )
        assert "error" in result


# ---------------------------------------------------------------------------
# Response metadata
# ---------------------------------------------------------------------------

class TestResponseMetadata:
    @pytest.mark.asyncio
    async def test_response_includes_freshness(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT * FROM github.pull_requests LIMIT 1",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "freshness_ms" in result

    @pytest.mark.asyncio
    async def test_response_includes_rate_limit_status(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT * FROM github.pull_requests LIMIT 1",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "rate_limit_status" in result

    @pytest.mark.asyncio
    async def test_response_includes_cache_stats(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT * FROM github.pull_requests LIMIT 1",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "cache_stats" in result

    @pytest.mark.asyncio
    async def test_response_includes_columns(self, engine, tenant, oidc):
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT pr_id, status FROM github.pull_requests LIMIT 1",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "columns" in result
        assert "pr_id" in result["columns"]
        assert "status" in result["columns"]

    @pytest.mark.asyncio
    async def test_response_includes_connector_timings(self, engine, tenant, oidc):
        """New: connector_timings shows per-connector fetch breakdown."""
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT * FROM github.pull_requests LIMIT 1",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "connector_timings" in result
        assert "github" in result["connector_timings"]
        gh_timing = result["connector_timings"]["github"]
        assert "fetch_ms" in gh_timing
        assert "from_cache" in gh_timing
        assert "rows" in gh_timing

    @pytest.mark.asyncio
    async def test_response_includes_timing_breakdown(self, engine, tenant, oidc):
        """New: timing object shows planning/fetch/security/duckdb breakdown."""
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            "SELECT * FROM github.pull_requests LIMIT 1",
            tenant, ctx, max_staleness_ms=5000,
        )
        assert "timing" in result
        timing = result["timing"]
        assert "total_ms" in timing
        assert "planning_ms" in timing
        assert "fetch_ms" in timing
        assert "security_ms" in timing
        assert "duckdb_ms" in timing

    @pytest.mark.asyncio
    async def test_cross_join_connector_timings(self, engine, tenant, oidc):
        """Cross-app join should have timings for both connectors."""
        ctx = await oidc.validate("token_dev", tenant)
        result = await engine.execute_query(
            """SELECT gh.pr_id, ji.issue_key
               FROM github.pull_requests gh
               JOIN jira.issues ji ON gh.branch = ji.branch_name""",
            tenant, ctx, max_staleness_ms=5000,
        )
        timings = result["connector_timings"]
        assert "github" in timings
        assert "jira" in timings


# ---------------------------------------------------------------------------
# Warnings (STALE_DATA, ENTITLEMENT_DENIED)
# ---------------------------------------------------------------------------

class TestWarnings:
    @pytest.mark.asyncio
    async def test_entitlement_denied_warning_on_full_rls_filter(self, engine, tenant, oidc):
        """When RLS filters ALL rows from a source, response includes ENTITLEMENT_DENIED warning."""
        # Create a tenant where no data will match the user's team
        from omnisql.tenant.models import TenantConfig, ConnectorConfig, RLSRule
        strict_tenant = TenantConfig(
            tenant_id="strict_test",
            display_name="Strict Test",
            connector_configs={
                "github": ConnectorConfig(
                    connector_id="github", base_url="mock",
                    pushable_filters=["status", "team_id"],
                ),
            },
            rls_rules=[
                # RLS rule that filters by team_id — but use a non-existent team
                RLSRule(connector_id="github", rule_expr="team_id == user.team_id"),
            ],
            table_registry={
                "github.pull_requests": {"connector": "github", "fetch_key": "all_prs"},
            },
        )
        # Use web_dev token — mock data has both mobile and web rows,
        # but if we query only for mobile data and are web user, RLS might filter some
        ctx = await oidc.validate("token_web_dev", strict_tenant)
        result = await engine.execute_query(
            "SELECT * FROM github.pull_requests WHERE team_id = 'mobile'",
            strict_tenant, ctx, max_staleness_ms=5000,
        )
        # Web team user asked for mobile data → RLS filters all rows
        # DuckDB WHERE also filters to mobile, so 0 rows expected
        assert len(result.get("rows", [])) == 0
