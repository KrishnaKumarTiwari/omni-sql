from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class FetchNode:
    """
    A single unit of work in the execution DAG.
    Represents one connector fetch operation for one SQL table reference.

    depends_on is empty for Phase 1 (all sources independent → single parallel wave).
    Phase 2 will populate it for subquery dependencies.
    """

    id: str                                         # "node_github_0"
    connector_id: str                               # "github"
    fetch_key: str                                  # "all_prs"
    table_name: str                                 # "github.pull_requests"
    view_name: str                                  # "github_pull_requests" (DuckDB view)
    pushdown_filters: Dict[str, Any] = field(default_factory=dict)
    duckdb_filters: Dict[str, Any] = field(default_factory=dict)
    depends_on: List[str] = field(default_factory=list)   # node IDs that must run first


@dataclass
class ExecutionDAG:
    """
    Directed acyclic graph of FetchNodes.

    get_levels() returns execution waves via Kahn's BFS topological sort.
    Each wave is a list of nodes that can run in parallel with asyncio.gather().

    Phase 1: all nodes have depends_on=[] → single wave → pure parallel fan-out.
    Phase 2: multi-wave for subquery dependencies (structure already supports it).
    """

    nodes: List[FetchNode] = field(default_factory=list)
    rewritten_sql: str = ""   # SQL with dotted table names replaced by view names

    def add_node(self, node: FetchNode) -> None:
        self.nodes.append(node)

    def add_dependency(self, dependent_id: str, depends_on_id: str) -> None:
        """Mark that node `dependent_id` cannot start until `depends_on_id` completes."""
        for node in self.nodes:
            if node.id == dependent_id:
                if depends_on_id not in node.depends_on:
                    node.depends_on.append(depends_on_id)
                return
        raise ValueError(f"Node not found: {dependent_id}")

    def get_levels(self) -> List[List[FetchNode]]:
        """
        Kahn's BFS-based topological sort. Returns execution waves.

        Each wave contains nodes whose dependencies are all satisfied
        by nodes in earlier waves. Nodes within a wave run in parallel.

        Raises:
            ValueError: if the graph contains a cycle.
        """
        if not self.nodes:
            return []

        node_map = {n.id: n for n in self.nodes}
        # in-degree per node (number of unfulfilled dependencies)
        in_degree: Dict[str, int] = {n.id: len(n.depends_on) for n in self.nodes}

        levels: List[List[FetchNode]] = []
        remaining = set(node_map.keys())

        while remaining:
            # Current wave: all nodes with in_degree == 0
            wave = [node_map[nid] for nid in remaining if in_degree[nid] == 0]
            if not wave:
                raise ValueError(
                    f"ExecutionDAG has a cycle among nodes: {remaining}"
                )
            levels.append(wave)

            # Reduce in-degree for nodes that depend on this wave
            for node in wave:
                remaining.discard(node.id)
                for candidate_id in remaining:
                    if node.id in node_map[candidate_id].depends_on:
                        in_degree[candidate_id] -= 1

        return levels
