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
            {"issue_key": "PRJ-101", "summary": "Fix authentication bottleneck", "status": "In Progress", "branch_name": "PRJ-101-fix-auth", "project": "AUTH"},
            {"issue_key": "PRJ-102", "summary": "UI Polish for dashboard", "status": "To Do", "branch_name": "PRJ-102-ui-fixes", "project": "UI"},
            {"issue_key": "PRJ-103", "summary": "API v2 Migration", "status": "Done", "branch_name": "PRJ-103-api-v2", "project": "API"},
            {"issue_key": "PRJ-104", "summary": "Develop Mobile App Beta", "status": "In Progress", "branch_name": "PRJ-104-mobile-app", "project": "MOBILE"},
        ]
