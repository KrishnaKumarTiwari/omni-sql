from typing import Dict, Any, List
from prototype.connectors.base import BaseConnector

class GenericConnector(BaseConnector):
    """
    A declarative connector that takes a 'manifest' (config) 
    and simulates fetching data based on that manifest.
    In a real system, this would make actual REST calls via requests/aiohttp.
    """
    def __init__(self, manifest: Dict[str, Any]):
        super().__init__(
            name=manifest["id"],
            rate_limit_capacity=manifest.get("rate_limit", {}).get("capacity", 100),
            refill_rate=manifest.get("rate_limit", {}).get("refill_rate", 1.0)
        )
        self.manifest = manifest
        # Mock data indexed by endpoint
        self.mock_db = manifest.get("mock_data", {})

    def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Simulates dynamic endpoint resolution and data projection.
        """
        endpoint = query_context.get("endpoint", "/")
        raw_data = self.mock_db.get(endpoint, [])
        
        # Simulate 'selector' logic (e.g. $.data.nodes)
        # Simplified: just return the list
        
        # Simulate 'column mapping' logic
        mappings = self.manifest.get("tables", [{}])[0].get("columns", {})
        if not mappings:
            return raw_data
            
        projected = []
        for row in raw_data:
            new_row = {}
            for target_col, source_path in mappings.items():
                # source_path is something like '$.id'
                key = source_path.replace("$.", "")
                new_row[target_col] = row.get(key, "[N/A]")
            projected.append(new_row)
            
        return projected
