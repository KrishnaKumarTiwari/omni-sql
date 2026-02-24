import duckdb
import pandas as pd
from typing import Dict, Any, List
from prototype.connectors.github import GitHubConnector
from prototype.connectors.jira import JiraConnector
from prototype.connectors.generic import GenericConnector
from prototype.utils.security import SecurityEnforcer
from prototype.cache.freshness import FreshnessCache

# Example Declarative Manifest for Linear (Onboarded with zero code)
LINEAR_MANIFEST = {
    "id": "linear",
    "rate_limit": {"capacity": 50, "refill_rate": 0.5},
    "tables": [
        {
            "name": "issues",
            "columns": {
                "id": "$.id",
                "title": "$.title",
                "status": "$.status"
            }
        }
    ],
    "mock_data": {
        "all_issues": [
            {"id": "LIN-1", "title": "Implement YAML Parser", "status": "Todo"},
            {"id": "LIN-2", "title": "Fix OIDC Loop", "status": "In Progress"}
        ]
    }
}

# Maps SQL table names to connector config
TABLE_REGISTRY = {
    "github.pull_requests": {"connector": "github", "fetch_key": "all_prs"},
    "jira.issues": {"connector": "jira", "fetch_key": "all_issues"},
    "linear.issues": {"connector": "linear", "fetch_key": "all_issues"},
}


class FederatedEngine:
    def __init__(self):
        self.connectors = {
            "github": GitHubConnector(),
            "jira": JiraConnector(),
            "linear": GenericConnector(LINEAR_MANIFEST),
        }
        self.con = duckdb.connect(database=':memory:')
        self.cache = FreshnessCache(default_ttl_ms=60000)  # 60s default TTL

    def _detect_tables(self, sql: str) -> List[str]:
        """Detect which virtual tables the SQL references."""
        return [t for t in TABLE_REGISTRY if t in sql]

    def _rewrite_sql(self, sql: str) -> str:
        """Replace dotted table names (e.g. github.pull_requests) with
        DuckDB-compatible view names (e.g. github_pull_requests)."""
        result = sql
        for table_name in TABLE_REGISTRY:
            result = result.replace(table_name, table_name.replace(".", "_"))
        return result

    def _extract_filters(self, sql: str, connector_name: str) -> Dict[str, str]:
        """Extract predicate-pushdown filters from SQL for a given connector.
        In production this would use an AST parser (sqlglot); the prototype
        does simple string matching for known patterns."""
        filters = {}
        if connector_name == "github":
            if "gh.status = 'merged'" in sql:
                filters["status"] = "merged"
            elif "gh.status = 'open'" in sql:
                filters["status"] = "open"
            elif "gh.status = 'closed'" in sql:
                filters["status"] = "closed"
        elif connector_name == "jira":
            if "jira.status = 'In Progress'" in sql or "ji.status = 'In Progress'" in sql:
                filters["status"] = "In Progress"
            elif "jira.status = 'Done'" in sql or "ji.status = 'Done'" in sql:
                filters["status"] = "Done"
        return filters

    def execute_query(self, user_context: Dict[str, Any], max_staleness_ms: int = 0, request_sql: str = "") -> Dict[str, Any]:
        """Execute the user's SQL against federated sources.

        Flow:
        1. Detect which sources the SQL references
        2. Fetch data from each source (with cache + rate-limit checks)
        3. Apply RLS/CLS security filters
        4. Register each source as a DuckDB temp view
        5. Rewrite dotted table names and execute the actual user SQL
        """
        # 1. Detect which sources are needed
        referenced = self._detect_tables(request_sql)
        if not referenced:
            return {
                "error": "No recognized tables in query. Available: github.pull_requests, jira.issues, linear.issues",
                "status_code": 400,
            }

        actual_freshness_ms = 0
        rate_limit_status = {}

        # 2. For each source: fetch -> security -> register as DuckDB view
        for table_name in referenced:
            info = TABLE_REGISTRY[table_name]
            connector = self.connectors[info["connector"]]
            filters = self._extract_filters(request_sql, info["connector"])

            # Check cache
            cached = self.cache.get(info["connector"], max_staleness_ms, filters=filters)
            if cached:
                data, age = cached
                actual_freshness_ms = max(actual_freshness_ms, age)
                rate_limit_status = connector.rate_limiter.get_status()
            else:
                resp = connector.get_data(info["fetch_key"], {"filters": filters}, max_staleness_ms)
                if "error" in resp:
                    return resp
                data = resp["data"]
                self.cache.put(info["connector"], data, filters=filters)
                actual_freshness_ms = max(actual_freshness_ms, resp.get("freshness_ms", 0))
                rate_limit_status = resp.get("rate_limit_status", {})

            # Apply RLS + CLS
            data = SecurityEnforcer.apply_rls(info["connector"], data, user_context)
            data = SecurityEnforcer.apply_cls(info["connector"], data, user_context)

            # Register as DuckDB view
            view_name = table_name.replace(".", "_")
            self.con.register(view_name, pd.DataFrame(data))

        # 3. Rewrite SQL and execute in DuckDB
        rewritten = self._rewrite_sql(request_sql)
        try:
            result_df = self.con.execute(rewritten).df()
        except Exception as e:
            return {
                "error": f"SQL execution error: {str(e)}",
                "status_code": 400,
            }

        return {
            "rows": result_df.to_dict(orient='records'),
            "columns": result_df.columns.tolist(),
            "freshness_ms": actual_freshness_ms,
            "rate_limit_status": rate_limit_status,
            "cache_stats": self.cache.get_stats(),
        }
