from __future__ import annotations
from typing import Any, Dict, List

from omnisql.connectors.base import AsyncBaseConnector

_LINEAR_ISSUES_QUERY = """
query($filter: IssueFilter, $first: Int!, $cursor: String) {
  issues(filter: $filter, first: $first, after: $cursor) {
    nodes {
      id
      title
      state { name }
      assignee { name }
      team { name }
      priority
      createdAt
    }
    pageInfo { endCursor hasNextPage }
  }
}
"""

_MOCK_LINEAR_ISSUES = [
    {"id": "LIN-1", "title": "Implement YAML Parser", "status": "Todo", "assignee": None, "team": "platform"},
    {"id": "LIN-2", "title": "Fix OIDC Loop",         "status": "In Progress", "assignee": "alice", "team": "infra"},
    {"id": "LIN-3", "title": "Add GraphQL connector", "status": "Done",        "assignee": "bob",   "team": "core"},
]


class AsyncLinearConnector(AsyncBaseConnector):
    """
    Async Linear connector (GraphQL-only API).

    Demo mode  (config.base_url == "mock"): returns _MOCK_LINEAR_ISSUES.
    Production: uses Linear GraphQL API.
      config.base_url     = "https://api.linear.app"
      config.transport    = "graphql"
      config.auth_type    = "bearer"
      config.credential_ref = "env://LINEAR_API_KEY"
    """

    async def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        filters = query_context.get("filters", {})

        if self.config.base_url == "mock":
            data = list(_MOCK_LINEAR_ISSUES)
            if "status" in filters:
                data = [r for r in data if r["status"] == filters["status"]]
            return data

        # Build Linear filter object
        linear_filter: Dict = {}
        if "status" in filters:
            linear_filter["state"] = {"name": {"eq": filters["status"]}}

        nodes = await self._paginate_graphql(
            _LINEAR_ISSUES_QUERY,
            variables={"filter": linear_filter, "first": self.config.page_size},
            data_path="issues",
        )
        return [self._normalize_record(n) for n in nodes]

    def _normalize_record(self, raw: Dict) -> Dict:
        return {
            "id": raw.get("id", ""),
            "title": raw.get("title", ""),
            "status": (raw.get("state") or {}).get("name", ""),
            "assignee": (raw.get("assignee") or {}).get("name"),
            "team": (raw.get("team") or {}).get("name", ""),
            "priority": raw.get("priority", 0),
        }
