import duckdb
import pandas as pd
from typing import Dict, Any, List
from prototype.connectors.github import GitHubConnector
from prototype.connectors.jira import JiraConnector
from prototype.utils.security import SecurityEnforcer

class FederatedEngine:
    def __init__(self):
        self.gh = GitHubConnector()
        self.jira = JiraConnector()
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

        # Handle empty results from security
        if not gh_data or not jira_data:
            return {
                "rows": [],
                "columns": ["pr_id", "author", "branch", "issue_key", "jira_status"],
                "freshness_ms": max(gh_resp.get("freshness_ms", 0), jira_resp.get("freshness_ms", 0)),
                "rate_limit_status": gh_resp["rate_limit_status"]
            }

        # 3. Join in DuckDB
        gh_df = pd.DataFrame(gh_data)
        jira_df = pd.DataFrame(jira_data)

        # Register dataframes as temp views
        self.con.register('github_prs', gh_df)
        self.con.register('jira_issues', jira_df)

        # Execute the federated query
        sql = """
        SELECT 
            gh.pr_id, 
            gh.author, 
            gh.branch, 
            ji.issue_key, 
            ji.status as jira_status
        FROM github_prs gh
        JOIN jira_issues ji ON gh.branch = ji.branch_name
        WHERE ji.status = 'In Progress'
        """
        result_df = self.con.execute(sql).df()

        return {
            "rows": result_df.to_dict(orient='records'),
            "columns": result_df.columns.tolist(),
            "freshness_ms": max(gh_resp.get("freshness_ms", 0), jira_resp.get("freshness_ms", 0)),
            "rate_limit_status": gh_resp["rate_limit_status"] # Simplified: return GH status
        }
