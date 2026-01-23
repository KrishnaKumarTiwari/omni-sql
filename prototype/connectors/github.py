from typing import List, Dict, Any
from prototype.connectors.base import BaseConnector

class GitHubConnector(BaseConnector):
    def __init__(self):
        # 50 req capacity, 10/sec refill
        super().__init__(name="github", rate_limit_capacity=50, refill_rate=10, cache_ttl=30)
        
    def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Mock data for Pull Requests.
        Each PR has a branch name to join with Jira issues.
        """
        return [
            {"pr_id": 1, "author": "dev_a", "author_email": "dev_a@ema.co", "branch": "PRJ-101-fix-auth", "status": "open", "review_status": "APPROVED", "team_id": "mobile", "created_at": "2026-01-20T10:00:00Z", "assignee": "senior_dev", "additions": 145, "deletions": 12},
            {"pr_id": 2, "author": "dev_b", "author_email": "dev_b@ema.co", "branch": "PRJ-102-ui-fixes", "status": "open", "review_status": "CHANGES_REQUESTED", "team_id": "web", "created_at": "2026-01-21T11:00:00Z", "assignee": "ui_lead", "additions": 45, "deletions": 2},
            {"pr_id": 3, "author": "dev_c", "author_email": "dev_c@ema.co", "branch": "PRJ-103-api-v2", "status": "merged", "review_status": "APPROVED", "team_id": "api", "created_at": "2026-01-22T09:00:00Z", "merged_at": "2026-01-22T15:30:00Z", "assignee": "api_ninja", "additions": 890, "deletions": 412},
            {"pr_id": 4, "author": "dev_a", "author_email": "dev_a@ema.co", "branch": "PRJ-104-mobile-app", "status": "open", "review_status": "PENDING", "team_id": "mobile", "created_at": "2026-01-23T14:00:00Z", "assignee": "dev_a", "additions": 320, "deletions": 45},
            {"pr_id": 5, "author": "dev_d", "author_email": "dev_d@ema.co", "branch": "PRJ-105-infra-as-code", "status": "open", "review_status": "PENDING", "team_id": "infra", "created_at": "2026-01-24T08:00:00Z", "assignee": "sre_master", "additions": 1200, "deletions": 50},
            {"pr_id": 6, "author": "dev_b", "author_email": "dev_b@ema.co", "branch": "PRJ-106-security-patch", "status": "merged", "review_status": "APPROVED", "team_id": "web", "created_at": "2026-01-24T09:30:00Z", "merged_at": "2026-01-24T10:15:00Z", "assignee": "sec_lead", "additions": 12, "deletions": 8},
        ]
