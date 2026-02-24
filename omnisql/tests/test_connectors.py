"""Tests for async connectors (mock mode — no real API calls)."""
import pytest

from omnisql.connectors.github import AsyncGitHubConnector, _MOCK_PRS
from omnisql.connectors.jira import AsyncJiraConnector, _MOCK_ISSUES
from omnisql.connectors.linear import AsyncLinearConnector
from omnisql.gateway.main import _NullCache, _NullRateLimiter
from omnisql.tenant.models import ConnectorConfig


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _cfg(cid: str) -> ConnectorConfig:
    return ConnectorConfig(
        connector_id=cid, base_url="mock",
        pushable_filters=["status", "team_id", "project"],
    )


@pytest.fixture
def gh():
    return AsyncGitHubConnector(_cfg("github"), _NullRateLimiter(), _NullCache())


@pytest.fixture
def jira():
    return AsyncJiraConnector(_cfg("jira"), _NullRateLimiter(), _NullCache())


@pytest.fixture
def linear():
    return AsyncLinearConnector(_cfg("linear"), _NullRateLimiter(), _NullCache())


# ---------------------------------------------------------------------------
# GitHub Connector
# ---------------------------------------------------------------------------

class TestGitHubConnector:
    def test_mock_data_count(self):
        assert len(_MOCK_PRS) == 120

    def test_mock_data_schema(self):
        pr = _MOCK_PRS[0]
        required_fields = {
            "pr_id", "author", "author_email", "branch", "status",
            "review_status", "team_id", "created_at", "assignee",
            "additions", "deletions", "merged_at",
        }
        assert required_fields.issubset(set(pr.keys()))

    @pytest.mark.asyncio
    async def test_fetch_all(self, gh):
        data = await gh.fetch_data({"filters": {}})
        assert len(data) == 120

    @pytest.mark.asyncio
    async def test_fetch_with_status_filter(self, gh):
        data = await gh.fetch_data({"filters": {"status": "merged"}})
        assert all(r["status"] == "merged" for r in data)
        assert len(data) == 40  # 120 / 3 statuses

    @pytest.mark.asyncio
    async def test_fetch_with_team_filter(self, gh):
        data = await gh.fetch_data({"filters": {"team_id": "mobile"}})
        assert all(r["team_id"] == "mobile" for r in data)

    @pytest.mark.asyncio
    async def test_get_data_returns_expected_keys(self, gh):
        result = await gh.get_data(
            tenant_id="t1", fetch_key="all_prs",
            query_context={"filters": {}}, max_staleness_ms=5000,
        )
        assert "data" in result
        assert "freshness_ms" in result
        assert "from_cache" in result
        assert "rate_limit_status" in result

    def test_branch_naming_convention(self):
        """Branches must match Jira's branch_name for JOIN to work."""
        branches = {pr["branch"] for pr in _MOCK_PRS}
        assert any("feature/mobile/" in b for b in branches)
        assert any("feature/web/" in b for b in branches)


# ---------------------------------------------------------------------------
# Jira Connector
# ---------------------------------------------------------------------------

class TestJiraConnector:
    def test_mock_data_count(self):
        assert len(_MOCK_ISSUES) == 120

    def test_mock_data_schema(self):
        issue = _MOCK_ISSUES[0]
        required_fields = {
            "issue_key", "summary", "status", "priority",
            "assignee", "story_points", "branch_name", "project",
        }
        assert required_fields.issubset(set(issue.keys()))

    @pytest.mark.asyncio
    async def test_fetch_all(self, jira):
        data = await jira.fetch_data({"filters": {}})
        assert len(data) == 120

    @pytest.mark.asyncio
    async def test_fetch_with_status_filter(self, jira):
        data = await jira.fetch_data({"filters": {"status": "In Progress"}})
        assert all(r["status"] == "In Progress" for r in data)

    @pytest.mark.asyncio
    async def test_fetch_with_project_filter(self, jira):
        data = await jira.fetch_data({"filters": {"project": "MOBILE"}})
        assert all(r["project"] == "MOBILE" for r in data)

    def test_branch_naming_matches_github(self):
        """Jira branch_name must overlap with GitHub branch for JOINs."""
        jira_branches = {i["branch_name"] for i in _MOCK_ISSUES}
        gh_branches = {pr["branch"] for pr in _MOCK_PRS}
        overlap = jira_branches & gh_branches
        assert len(overlap) > 0, "No branch overlap between GitHub and Jira mock data"


# ---------------------------------------------------------------------------
# Linear Connector
# ---------------------------------------------------------------------------

class TestLinearConnector:
    @pytest.mark.asyncio
    async def test_fetch_all(self, linear):
        data = await linear.fetch_data({"filters": {}})
        assert len(data) == 3

    @pytest.mark.asyncio
    async def test_fetch_with_status_filter(self, linear):
        data = await linear.fetch_data({"filters": {"status": "In Progress"}})
        assert len(data) == 1
        assert data[0]["title"] == "Fix OIDC Loop"


# ---------------------------------------------------------------------------
# Cross-connector data alignment
# ---------------------------------------------------------------------------

class TestDataAlignment:
    def test_branch_overlap_sufficient_for_joins(self):
        """At least 20 branches must overlap for meaningful join results."""
        gh_branches = {pr["branch"] for pr in _MOCK_PRS}
        jira_branches = {i["branch_name"] for i in _MOCK_ISSUES}
        overlap = gh_branches & jira_branches
        assert len(overlap) >= 20, f"Only {len(overlap)} overlapping branches"

    def test_team_distribution(self):
        """All 5 teams must be represented in mock data."""
        gh_teams = {pr["team_id"] for pr in _MOCK_PRS}
        assert gh_teams == {"mobile", "web", "api", "infra", "data"}

    def test_status_distribution(self):
        """All statuses present in mock data."""
        gh_statuses = {pr["status"] for pr in _MOCK_PRS}
        assert gh_statuses == {"open", "merged", "closed"}
        jira_statuses = {i["status"] for i in _MOCK_ISSUES}
        assert jira_statuses == {"To Do", "In Progress", "Done", "Blocked"}
