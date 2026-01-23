import duckdb
import pandas as pd
from typing import Dict, Any, List
from prototype.connectors.github import GitHubConnector
from prototype.connectors.jira import JiraConnector
from prototype.connectors.generic import GenericConnector
from prototype.utils.security import SecurityEnforcer

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

    def execute_query(self, user_context: Dict[str, Any], max_staleness_ms: int = 0) -> Dict[str, Any]:
        """
        Executes the recruitment intelligence join scenario.
        For a real system, this would involve a SQL parser.
        """
        # 1. Fetch from GitHub (with Governance)
        gh_resp = self.gh.get_data("all_prs", {}, max_staleness_ms)
        if "error" in gh_resp:
            return gh_resp
        
        # Apply Security to GH
        gh_data = gh_resp["data"]
        gh_data = SecurityEnforcer.apply_rls("github", gh_data, user_context)
        gh_data = SecurityEnforcer.apply_cls("github", gh_data, user_context)

        # 2. Fetch from Jira (with Governance)
        jira_resp = self.jira.get_data("all_issues", {}, max_staleness_ms)
        if "error" in jira_resp:
            return jira_resp
            
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
        # (Security logic would be dynamic in production)

        # Handle empty results from security
        if not gh_data or not jira_data or not linear_data:
            return {
                "rows": [],
                "columns": ["pr_id", "author", "branch", "issue_key", "jira_status", "linear_id"],
                "freshness_ms": max(gh_resp.get("freshness_ms", 0), jira_resp.get("freshness_ms", 0), linear_resp.get("freshness_ms", 0)),
                "rate_limit_status": gh_resp["rate_limit_status"]
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
            "freshness_ms": max(gh_resp.get("freshness_ms", 0), jira_resp.get("freshness_ms", 0), linear_resp.get("freshness_ms", 0)),
            "rate_limit_status": gh_resp["rate_limit_status"]
        }
