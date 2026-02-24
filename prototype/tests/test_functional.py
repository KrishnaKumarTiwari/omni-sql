"""
Functional End-to-End Tests

Tests verify complete query execution flow including
federated joins, data correctness, and metadata responses.
"""

import pytest
from prototype.engine import FederatedEngine
from prototype.utils.security import SecurityEnforcer


class TestQueryExecution:
    """Test end-to-end query execution"""

    def test_simple_github_query(self):
        """Execute simple query against GitHub connector"""
        engine = FederatedEngine()
        user_context = SecurityEnforcer.authenticate("token_dev")

        result = engine.execute_query(user_context, max_staleness_ms=0, request_sql="SELECT * FROM github.pull_requests")

        assert "rows" in result
        assert "columns" in result
        assert len(result["rows"]) > 0
        # Should contain GitHub-specific columns
        assert "pr_id" in result["columns"]
        assert "author" in result["columns"]
        # Should NOT contain Jira columns (single-source query)
        assert "issue_key" not in result["columns"]

    def test_simple_jira_query(self):
        """Execute simple query against Jira connector"""
        engine = FederatedEngine()
        user_context = SecurityEnforcer.authenticate("token_dev")

        result = engine.execute_query(user_context, max_staleness_ms=0, request_sql="SELECT * FROM jira.issues")

        assert "rows" in result
        assert len(result["rows"]) > 0
        assert "issue_key" in result["columns"]
        # Should NOT contain GitHub columns
        assert "pr_id" not in result["columns"]

    def test_cross_app_join(self):
        """Execute federated join between GitHub and Jira"""
        engine = FederatedEngine()
        user_context = SecurityEnforcer.authenticate("token_dev")

        sql = """SELECT gh.pr_id, gh.author, gh.branch, ji.issue_key, ji.status as jira_status
                 FROM github.pull_requests gh
                 JOIN jira.issues ji ON gh.branch = ji.branch_name"""
        result = engine.execute_query(user_context, max_staleness_ms=0, request_sql=sql)

        # Should have joined data
        assert "rows" in result
        assert len(result["rows"]) > 0

        first_row = result["rows"][0]
        assert "pr_id" in first_row
        assert "author" in first_row
        assert "issue_key" in first_row
        assert "jira_status" in first_row

    def test_team_isolation_in_results(self):
        """Mobile team should only see mobile data"""
        engine = FederatedEngine()
        mobile_context = SecurityEnforcer.authenticate("token_dev")  # mobile team
        web_context = SecurityEnforcer.authenticate("token_web_dev")  # web team

        mobile_result = engine.execute_query(mobile_context, max_staleness_ms=0, request_sql="SELECT * FROM github.pull_requests")
        web_result = engine.execute_query(web_context, max_staleness_ms=0, request_sql="SELECT * FROM github.pull_requests")

        mobile_count = len(mobile_result.get("rows", []))
        web_count = len(web_result.get("rows", []))

        # Both should have data
        assert mobile_count > 0
        assert web_count > 0

        # All mobile results should be mobile team
        for row in mobile_result["rows"]:
            assert row["team_id"] == "mobile"

        # All web results should be web team
        for row in web_result["rows"]:
            assert row["team_id"] == "web"

    def test_predicate_pushdown(self):
        """Verify predicate pushdown filters are applied"""
        engine = FederatedEngine()
        user_context = SecurityEnforcer.authenticate("token_dev")

        sql = "SELECT * FROM github.pull_requests gh WHERE gh.status = 'merged'"
        result = engine.execute_query(user_context, max_staleness_ms=0, request_sql=sql)

        rows = result.get("rows", [])
        assert len(rows) > 0
        # All returned rows should have status=merged
        for row in rows:
            assert row["status"] == "merged"

    def test_unrecognized_table_returns_error(self):
        """Querying an unknown table should return an error"""
        engine = FederatedEngine()
        user_context = SecurityEnforcer.authenticate("token_dev")

        result = engine.execute_query(user_context, max_staleness_ms=0, request_sql="SELECT * FROM unknown.table")

        assert "error" in result


class TestMetadataResponse:
    """Test query response metadata"""

    def test_response_includes_freshness(self):
        """Response should include freshness_ms"""
        engine = FederatedEngine()
        user_context = SecurityEnforcer.authenticate("token_dev")

        result = engine.execute_query(user_context, max_staleness_ms=0, request_sql="SELECT * FROM github.pull_requests")

        assert "freshness_ms" in result
        assert isinstance(result["freshness_ms"], (int, float))

    def test_response_includes_cache_stats(self):
        """Response should include cache stats"""
        engine = FederatedEngine()
        user_context = SecurityEnforcer.authenticate("token_dev")

        result = engine.execute_query(user_context, max_staleness_ms=0, request_sql="SELECT * FROM github.pull_requests")

        assert "cache_stats" in result
        assert "hits" in result["cache_stats"]

    def test_response_includes_rate_limit_status(self):
        """Response should include rate limit status"""
        engine = FederatedEngine()
        user_context = SecurityEnforcer.authenticate("token_dev")

        result = engine.execute_query(user_context, max_staleness_ms=0, request_sql="SELECT * FROM github.pull_requests")

        assert "rate_limit_status" in result
        assert "remaining" in result["rate_limit_status"]
        assert "capacity" in result["rate_limit_status"]
