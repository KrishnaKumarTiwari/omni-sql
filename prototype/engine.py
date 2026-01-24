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

class FederatedEngine:
    def __init__(self):
        self.gh = GitHubConnector()
        self.jira = JiraConnector()
        self.linear = GenericConnector(LINEAR_MANIFEST)
        self.con = duckdb.connect(database=':memory:')
        self.cache = FreshnessCache(default_ttl_ms=60000)  # 60s default TTL

    def execute_query(self, user_context: Dict[str, Any], max_staleness_ms: int = 0, request_sql: str = "") -> Dict[str, Any]:
        """
        Executes the recruitment intelligence join scenario with caching.
        For a real system, this would involve a SQL parser.
        """
        # Track actual freshness (max age of data used)
        actual_freshness_ms = 0
        
        # 0. Extract Predicates (Simulated SQL Parsing)
        # In a real system, this would come from the AST
        gh_filters = {}
        jira_filters = {}
        
        # Simple string matching for prototype
        if "gh.status = 'merged'" in request_sql:
            gh_filters["status"] = "merged"
        if "jira.status = 'In Progress'" in request_sql:
            jira_filters["status"] = "In Progress"
        if "jira.status != 'Done'" in request_sql:
            # For negation, we might not push down depending on connector capability
            pass

        # 1. Fetch from GitHub (with Cache & Pushdown)
        cached_gh = self.cache.get("github", max_staleness_ms, filters=gh_filters)
        if cached_gh:
            gh_data, gh_age = cached_gh
            actual_freshness_ms = max(actual_freshness_ms, gh_age)
            gh_resp = {"data": gh_data, "freshness_ms": gh_age, "rate_limit_status": self.gh.rate_limiter.get_status()}
        else:
            gh_resp = self.gh.get_data("all_prs", {"filters": gh_filters}, max_staleness_ms)
            if "error" in gh_resp:
                return gh_resp
            self.cache.put("github", gh_resp["data"], filters=gh_filters)
            actual_freshness_ms = max(actual_freshness_ms, gh_resp.get("freshness_ms", 0))
        
        # Apply Security to GH
        gh_data = gh_resp["data"]
        gh_data = SecurityEnforcer.apply_rls("github", gh_data, user_context)
        gh_data = SecurityEnforcer.apply_cls("github", gh_data, user_context)

        # 2. Fetch from Jira (with Cache & Pushdown)
        cached_jira = self.cache.get("jira", max_staleness_ms, filters=jira_filters)
        if cached_jira:
            jira_data, jira_age = cached_jira
            actual_freshness_ms = max(actual_freshness_ms, jira_age)
            jira_resp = {"data": jira_data, "freshness_ms": jira_age, "rate_limit_status": self.jira.rate_limiter.get_status()}
        else:
            jira_resp = self.jira.get_data("all_issues", {"filters": jira_filters}, max_staleness_ms)
            if "error" in jira_resp:
                return jira_resp
            self.cache.put("jira", jira_resp["data"], filters=jira_filters)
            actual_freshness_ms = max(actual_freshness_ms, jira_resp.get("freshness_ms", 0))
            
        # Apply Security to Jira
        jira_data = jira_resp["data"]
        jira_data = SecurityEnforcer.apply_rls("jira", jira_data, user_context)
        jira_data = SecurityEnforcer.apply_cls("jira", jira_data, user_context)

        # 3. Fetch from Linear (Declarative / Zero-Code)
        linear_resp = self.linear.get_data("all_issues", {"endpoint": "all_issues"}, max_staleness_ms)
        if "error" in linear_resp:
            return linear_resp

        # Apply Security to Linear
        linear_data = linear_resp["data"]
        actual_freshness_ms = max(actual_freshness_ms, linear_resp.get("freshness_ms", 0))
        # (Security logic would be dynamic in production)

        # Handle empty results from security
        if not gh_data or not jira_data or not linear_data:
            return {
                "rows": [],
                "columns": ["pr_id", "author", "branch", "issue_key", "jira_status", "linear_id"],
                "freshness_ms": actual_freshness_ms,
                "rate_limit_status": gh_resp["rate_limit_status"],
                "cache_stats": self.cache.get_stats()
            }

        # 4. Join in DuckDB
        gh_df = pd.DataFrame(gh_data)
        jira_df = pd.DataFrame(jira_data)
        linear_df = pd.DataFrame(linear_data)

        # Register dataframes as temp views
        self.con.register('github_prs', gh_df)
        self.con.register('jira_issues', jira_df)
        self.con.register('linear_issues', linear_df)

        # Execute the federated query (Now including Linear)
        sql = """
        SELECT 
            gh.pr_id, 
            gh.author, 
            gh.branch, 
            ji.issue_key, 
            ji.status as jira_status,
            lin.id as linear_id
        FROM github_prs gh
        JOIN jira_issues ji ON gh.branch = ji.branch_name
        LEFT JOIN linear_issues lin ON ji.issue_key = lin.id
        WHERE ji.status = 'In Progress'
        """
        result_df = self.con.execute(sql).df()

        return {
            "rows": result_df.to_dict(orient='records'),
            "columns": result_df.columns.tolist(),
            "freshness_ms": actual_freshness_ms,
            "rate_limit_status": gh_resp["rate_limit_status"],
            "cache_stats": self.cache.get_stats()
        }
