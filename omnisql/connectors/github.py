from __future__ import annotations
import random
from typing import Any, Dict, List

from omnisql.connectors.base import AsyncBaseConnector

# GraphQL v4 query with cursor-based pagination and filter pushdown.
_GITHUB_PRS_QUERY = """
query($owner: String!, $repo: String!, $states: [PullRequestState!], $first: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(states: $states, first: $first, after: $cursor) {
      nodes {
        number
        title
        author { login }
        headRefName
        state
        createdAt
        mergedAt
        additions
        deletions
        reviewDecision
        assignees(first: 1) { nodes { login } }
        labels(first: 3) { nodes { name } }
      }
      pageInfo { endCursor hasNextPage }
    }
  }
}
"""

# Mock data: same schema as prototype for test compatibility.
_TEAMS = ["mobile", "web", "api", "infra", "data"]
_STATUSES = ["open", "merged", "closed"]


def _mock_prs(n: int = 120) -> List[Dict]:
    rng = random.Random(42)
    rows = []
    for i in range(1, n + 1):
        team = _TEAMS[i % len(_TEAMS)]
        status = _STATUSES[i % len(_STATUSES)]
        rows.append({
            "pr_id": f"PR-{i:03d}",
            "author": f"dev_{team}_{i % 5}",
            "author_email": f"dev_{team}_{i % 5}@company.com",
            "branch": f"feature/{team}/task-{i}",
            "status": status,
            "review_status": rng.choice(["approved", "changes_requested", "pending"]),
            "team_id": team,
            "created_at": f"2024-0{(i % 9) + 1}-01T00:00:00Z",
            "assignee": f"lead_{team}",
            "additions": rng.randint(10, 500),
            "deletions": rng.randint(5, 200),
            "merged_at": f"2024-0{(i % 9) + 1}-15T00:00:00Z" if status == "merged" else None,
        })
    return rows


_MOCK_PRS = _mock_prs()


class AsyncGitHubConnector(AsyncBaseConnector):
    """
    Async GitHub connector.

    Demo mode  (config.base_url == "mock"): returns _MOCK_PRS (120 records),
    same data as the prototype. Supports status filter pushdown.

    Production mode: uses GitHub GraphQL API v4. Requires:
      config.base_url     = "https://api.github.com"
      config.auth_type    = "bearer"
      config.credential_ref = "env://GITHUB_TOKEN"
    Connector YAML must also include owner/repo under extra_params.
    """

    async def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        filters = query_context.get("filters", {})

        if self.config.base_url == "mock":
            return self._mock_fetch(filters)

        # Production: GraphQL v4
        owner = getattr(self.config, "owner", "octocat")
        repo = getattr(self.config, "repo", "hello-world")
        status = filters.get("status", "").upper()
        states = [status] if status in ("OPEN", "MERGED", "CLOSED") else ["OPEN", "MERGED", "CLOSED"]

        nodes = await self._paginate_graphql(
            _GITHUB_PRS_QUERY,
            variables={"owner": owner, "repo": repo, "states": states, "first": self.config.page_size},
            data_path="repository.pullRequests",
        )
        return [self._normalize_record(n) for n in nodes]

    def _mock_fetch(self, filters: Dict[str, Any]) -> List[Dict]:
        data = list(_MOCK_PRS)
        status = filters.get("status")
        if status:
            data = [r for r in data if r["status"] == status]
        team_id = filters.get("team_id")
        if team_id:
            data = [r for r in data if r["team_id"] == team_id]
        return data

    def _normalize_record(self, raw: Dict) -> Dict:
        """Map GitHub GraphQL response to OmniSQL canonical PR schema."""
        return {
            "pr_id": f"PR-{raw.get('number', 0):03d}",
            "author": (raw.get("author") or {}).get("login", "unknown"),
            "author_email": "",  # not exposed by GitHub API
            "branch": raw.get("headRefName", ""),
            "status": raw.get("state", "").lower(),
            "review_status": (raw.get("reviewDecision") or "pending").lower(),
            "team_id": "",  # set by RLS context or team label lookup
            "created_at": raw.get("createdAt", ""),
            "assignee": (((raw.get("assignees") or {}).get("nodes") or [{}])[0]).get("login", ""),
            "additions": raw.get("additions", 0),
            "deletions": raw.get("deletions", 0),
            "merged_at": raw.get("mergedAt"),
        }
