from typing import List, Dict, Any
from prototype.connectors.base import BaseConnector

class GitHubConnector(BaseConnector):
    def __init__(self):
        # 50 req capacity, 10/sec refill
        super().__init__(name="github", rate_limit_capacity=50, refill_rate=10, cache_ttl=30)
        
    def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Mock data for Pull Requests (100+ records).
        Each PR has a branch name to join with Jira issues.
        """
        teams = ["mobile", "web", "api", "infra", "data"]
        statuses = ["open", "merged", "closed"]
        review_statuses = ["APPROVED", "CHANGES_REQUESTED", "PENDING", "COMMENTED"]
        authors = ["dev_a", "dev_b", "dev_c", "dev_d", "dev_e", "dev_f", "dev_g", "dev_h"]
        
        data = []
        for i in range(1, 121):  # Generate 120 records
            team = teams[i % len(teams)]
            status = statuses[i % len(statuses)]
            review = review_statuses[i % len(review_statuses)]
            author = authors[i % len(authors)]
            
            record = {
                "pr_id": i,
                "author": author,
                "author_email": f"{author}@ema.co",
                "branch": f"PRJ-{i:03d}-feature-{team}",
                "status": status,
                "review_status": review,
                "team_id": team,
                "created_at": f"2026-01-{(i % 28) + 1:02d}T{(i % 24):02d}:00:00Z",
                "assignee": f"lead_{team}",
                "additions": (i * 13) % 1000,
                "deletions": (i * 7) % 200,
            }
            
            if status == "merged":
                record["merged_at"] = f"2026-01-{(i % 28) + 1:02d}T{((i + 5) % 24):02d}:00:00Z"
            
            data.append(record)
        
        return [
            record for record in data
            if self._apply_filters(record, query_context.get("filters", {}))
        ]

    def _apply_filters(self, record: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """Apply simple equality filters."""
        if not filters:
            return True
            
        for key, value in filters.items():
            # Handle mapped fields if necessary, for now direct match
            if key in record and str(record[key]) != str(value):
                return False
        return True
