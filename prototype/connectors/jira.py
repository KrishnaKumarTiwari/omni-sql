from typing import List, Dict, Any
from prototype.connectors.base import BaseConnector

class JiraConnector(BaseConnector):
    def __init__(self):
        # 50 req capacity, 10/sec refill
        super().__init__(name="jira", rate_limit_capacity=50, refill_rate=10, cache_ttl=60)
        
    def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Mock data for Jira Issues (100+ records).
        Each issue has a branch_name to join with GitHub PRs.
        """
        projects = ["MOBILE", "WEB", "API", "INFRA", "DATA"]
        statuses = ["To Do", "In Progress", "Done", "Blocked"]
        priorities = ["LOW", "MEDIUM", "HIGH", "CRITICAL", "BLOCKER"]
        
        data = []
        for i in range(1, 121):  # Generate 120 records
            project = projects[i % len(projects)]
            status = statuses[i % len(statuses)]
            priority = priorities[i % len(priorities)]
            
            record = {
                "issue_key": f"PRJ-{i:03d}",
                "summary": f"Feature {i}: Implement {project.lower()} enhancement",
                "status": status,
                "priority": priority,
                "assignee": f"lead_{project.lower()}",
                "story_points": (i % 13) + 1,
                "branch_name": f"PRJ-{i:03d}-feature-{project.lower()}",
                "project": project,
            }
            
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
