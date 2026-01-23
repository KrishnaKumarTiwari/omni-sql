from typing import List, Dict, Any
from prototype.connectors.base import BaseConnector

class JiraConnector(BaseConnector):
    def __init__(self):
        # 50 req capacity, 10/sec refill
        super().__init__(name="jira", rate_limit_capacity=50, refill_rate=10, cache_ttl=60)
        
    def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Mock data for Jira Issues.
        Each issue has a branch_name to join with GitHub PRs.
        """
        return [
            {"issue_key": "PRJ-101", "summary": "Fix authentication bottleneck", "status": "In Progress", "priority": "CRITICAL", "assignee": "senior_dev", "story_points": 5, "branch_name": "PRJ-101-fix-auth", "project": "AUTH"},
            {"issue_key": "PRJ-102", "summary": "UI Polish for dashboard", "status": "In Progress", "priority": "MEDIUM", "assignee": "ui_lead", "story_points": 3, "branch_name": "PRJ-102-ui-fixes", "project": "UI"},
            {"issue_key": "PRJ-103", "summary": "API v2 Migration", "status": "Done", "priority": "HIGH", "assignee": "api_ninja", "story_points": 8, "branch_name": "PRJ-103-api-v2", "project": "API"},
            {"issue_key": "PRJ-104", "summary": "Develop Mobile App Beta", "status": "In Progress", "priority": "HIGH", "assignee": "dev_a", "story_points": 13, "branch_name": "PRJ-104-mobile-app", "project": "MOBILE"},
            {"issue_key": "PRJ-105", "summary": "Cloud Provisioning Setup", "status": "To Do", "priority": "HIGHEST", "assignee": "sre_master", "story_points": 5, "branch_name": "PRJ-105-infra-as-code", "project": "INFRA"},
            {"issue_key": "PRJ-106", "summary": "Emergency Security Patch", "status": "Done", "priority": "BLOCKER", "assignee": "sec_lead", "story_points": 1, "branch_name": "PRJ-106-security-patch", "project": "SEC"},
        ]
