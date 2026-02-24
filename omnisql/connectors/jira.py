from __future__ import annotations
import random
from typing import Any, Dict, List
from urllib.parse import urlencode

from omnisql.connectors.base import AsyncBaseConnector

_PROJECTS = ["MOBILE", "WEB", "API", "INFRA", "DATA"]
_STATUSES = ["To Do", "In Progress", "Done", "Blocked"]
_PRIORITIES = ["High", "Medium", "Low", "Critical"]


def _mock_issues(n: int = 120) -> List[Dict]:
    rng = random.Random(99)
    rows = []
    for i in range(1, n + 1):
        proj = _PROJECTS[i % len(_PROJECTS)]
        status = _STATUSES[i % len(_STATUSES)]
        rows.append({
            "issue_key": f"PRJ-{i:03d}",
            "summary": f"Task {i} for {proj}",
            "status": status,
            "priority": _PRIORITIES[i % len(_PRIORITIES)],
            "assignee": f"lead_{proj.lower()}",
            "story_points": rng.choice([1, 2, 3, 5, 8, 13]),
            "branch_name": f"feature/{proj.lower()}/task-{i}",
            "project": proj,
        })
    return rows


_MOCK_ISSUES = _mock_issues()


class AsyncJiraConnector(AsyncBaseConnector):
    """
    Async Jira connector.

    Demo mode  (config.base_url == "mock"): returns _MOCK_ISSUES.
    Production mode: uses Jira REST API v3 (/rest/api/3/search) with JQL pushdown.
      config.base_url     = "https://mycompany.atlassian.net"
      config.auth_type    = "basic"
      config.credential_ref = "env://JIRA_API_TOKEN"  (email:token base64-encoded)
    """

    async def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        filters = query_context.get("filters", {})

        if self.config.base_url == "mock":
            return self._mock_fetch(filters)

        # Production: REST v3 with JQL pushdown
        jql_parts = []
        if "status" in filters:
            jql_parts.append(f'status = "{filters["status"]}"')
        if "project" in filters:
            jql_parts.append(f'project = "{filters["project"].upper()}"')
        if "priority" in filters:
            jql_parts.append(f'priority = "{filters["priority"]}"')

        jql = " AND ".join(jql_parts) if jql_parts else "order by created DESC"

        items = await self._paginate_rest(
            "/rest/api/3/search",
            params={"jql": jql, "maxResults": self.config.page_size, "startAt": 0},
        )
        return [self._normalize_record(r) for r in items]

    def _mock_fetch(self, filters: Dict[str, Any]) -> List[Dict]:
        data = list(_MOCK_ISSUES)
        if "status" in filters:
            data = [r for r in data if r["status"] == filters["status"]]
        if "project" in filters:
            data = [r for r in data if r["project"].lower() == filters["project"].lower()]
        return data

    def _normalize_record(self, raw: Dict) -> Dict:
        """Map Jira API response to OmniSQL canonical issue schema."""
        fields = raw.get("fields", raw)
        return {
            "issue_key": raw.get("key", ""),
            "summary": fields.get("summary", ""),
            "status": (fields.get("status") or {}).get("name", ""),
            "priority": (fields.get("priority") or {}).get("name", ""),
            "assignee": ((fields.get("assignee") or {}).get("displayName", "")),
            "story_points": fields.get("story_points", fields.get("customfield_10016", 0)),
            "branch_name": fields.get("customfield_10000", ""),
            "project": (fields.get("project") or {}).get("key", ""),
        }
