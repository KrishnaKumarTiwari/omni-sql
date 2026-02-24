from __future__ import annotations
from typing import Any, Dict, List

from omnisql.connectors.base import AsyncBaseConnector


class AsyncGenericConnector(AsyncBaseConnector):
    """
    Zero-code connector for standard REST/GraphQL APIs defined by a YAML manifest.

    The manifest is embedded in the tenant config (or a separate YAML file)
    and defines: endpoints, column mappings, mock_data.

    This is the declarative connector path — same concept as the prototype's
    GenericConnector but async and tenant-aware.
    """

    def __init__(self, manifest: Dict[str, Any], *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._manifest = manifest

    async def fetch_data(self, query_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        fetch_key = query_context.get("fetch_key", "")
        filters = query_context.get("filters", {})

        # Use mock_data if present (dev/demo mode)
        mock = self._manifest.get("mock_data", {})
        if fetch_key in mock:
            data = mock[fetch_key]
        else:
            data = []

        # Apply column projection from manifest tables
        tables = self._manifest.get("tables", [])
        columns: Dict[str, str] = {}
        for tbl in tables:
            columns.update(tbl.get("columns", {}))

        if columns:
            projected = []
            for row in data:
                new_row = {}
                for col_name, json_path in columns.items():
                    key = json_path.lstrip("$.")
                    new_row[col_name] = row.get(key, row.get(col_name))
                projected.append(new_row)
            data = projected

        # Apply simple filter pushdown
        for field, value in filters.items():
            data = [r for r in data if r.get(field) == value]

        return data
