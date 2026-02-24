from __future__ import annotations
import logging
from typing import Any, Dict, List, Tuple

import sqlglot
import sqlglot.expressions as exp

from omnisql.planner.models import ExecutionDAG, FetchNode
from omnisql.tenant.models import TenantConfig

logger = logging.getLogger(__name__)


class QueryPlanner:
    """
    Translates a SQL string into an ExecutionDAG using sqlglot AST parsing.

    Replaces the prototype's string-matching _detect_tables() and
    _extract_filters() with proper AST traversal that handles:
    - Table aliases (e.g. 'github.pull_requests gh')
    - Mixed-case column names
    - Multiple WHERE conditions
    - All dialects sqlglot understands

    Phase 1: all FetchNodes have depends_on=[] → single parallel wave.
    Phase 2 extension point: _detect_subquery_deps() (not yet implemented).
    """

    def __init__(self, tenant_cfg: TenantConfig) -> None:
        self._cfg = tenant_cfg

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def plan(self, sql: str) -> ExecutionDAG:
        """
        Parse SQL and produce an ExecutionDAG.

        Raises:
            ValueError: if SQL references a table not in tenant's table_registry.
            sqlglot.errors.ParseError: if SQL is syntactically invalid.
        """
        try:
            ast = sqlglot.parse_one(sql, read="duckdb")
        except Exception as exc:
            raise ValueError(f"SQL parse error: {exc}") from exc

        # Collect table refs AND their aliases in one pass
        table_refs, alias_map = self._extract_table_refs_with_aliases(ast)
        if not table_refs:
            raise ValueError(
                "No recognized tables in query. "
                f"Available: {', '.join(self._cfg.table_registry.keys())}"
            )

        dag = ExecutionDAG(rewritten_sql=self._rewrite_sql(sql, table_refs))

        for i, table_name in enumerate(table_refs):
            registry_entry = self._cfg.table_registry.get(table_name)
            if not registry_entry:
                raise ValueError(
                    f"Unknown table: '{table_name}'. "
                    f"Available: {', '.join(self._cfg.table_registry.keys())}"
                )

            connector_id = registry_entry["connector"]
            fetch_key = registry_entry.get("fetch_key", "all")
            connector_cfg = self._cfg.connector_configs.get(connector_id)

            # Only classify predicates that belong to THIS table's alias
            table_aliases = alias_map.get(table_name, set())
            pushdown, duckdb_side = self._classify_predicates(
                ast,
                table_name,
                connector_cfg.pushable_filters if connector_cfg else [],
                table_aliases=table_aliases,
            )

            node = FetchNode(
                id=f"node_{connector_id}_{i}",
                connector_id=connector_id,
                fetch_key=fetch_key,
                table_name=table_name,
                view_name=table_name.replace(".", "_"),
                pushdown_filters=pushdown,
                duckdb_filters=duckdb_side,
                depends_on=[],   # Phase 1: always parallel
            )
            dag.add_node(node)

        return dag

    # ------------------------------------------------------------------
    # AST helpers
    # ------------------------------------------------------------------

    def _extract_table_refs_with_aliases(
        self, ast: exp.Expression
    ) -> Tuple[List[str], Dict[str, set]]:
        """
        Walk the AST and collect:
        - ordered list of table names that exist in tenant's table_registry
        - mapping of table_name → set of aliases used in the query

        For 'FROM github.pull_requests gh', alias_map["github.pull_requests"] = {"gh", "github_pull_requests"}
        Unaliased references add the table name itself as the "alias".
        """
        seen: List[str] = []
        alias_map: Dict[str, set] = {}

        for table_node in ast.find_all(exp.Table):
            db = table_node.args.get("db")
            name = table_node.args.get("this")
            if db and name:
                full_name = f"{db.name}.{name.name}"
            elif name:
                full_name = name.name
            else:
                continue

            if full_name not in self._cfg.table_registry:
                continue

            if full_name not in seen:
                seen.append(full_name)
                alias_map[full_name] = set()

            # Collect alias (e.g., 'gh' in 'github.pull_requests gh')
            alias_node = table_node.args.get("alias")
            if alias_node:
                alias_str = alias_node.name if hasattr(alias_node, "name") else str(alias_node)
                alias_map[full_name].add(alias_str.lower())

            # The view name (dotted → underscored) is also a valid reference
            alias_map[full_name].add(full_name.replace(".", "_"))

        return seen, alias_map

    def _extract_table_refs(self, ast: exp.Expression) -> List[str]:
        """Backward-compat wrapper (used in _rewrite_sql)."""
        refs, _ = self._extract_table_refs_with_aliases(ast)
        return refs

    def _classify_predicates(
        self,
        ast: exp.Expression,
        table_name: str,
        pushable_fields: List[str],
        table_aliases: set | None = None,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Split WHERE predicates into:
        - pushdown_filters: simple EQ on pushable fields belonging to THIS table
        - duckdb_filters:   everything else (evaluated by DuckDB post-fetch)

        Only simple EQ predicates (col = 'val') on pushable fields are pushed down.
        Predicates qualified with another table's alias are excluded.
        """
        pushdown: Dict[str, Any] = {}
        duckdb_side: Dict[str, Any] = {}
        table_aliases = table_aliases or set()

        where = ast.find(exp.Where)
        if not where:
            return pushdown, duckdb_side

        for eq_node in where.find_all(exp.EQ):
            left = eq_node.left
            right = eq_node.right

            if not isinstance(right, (exp.Literal, exp.Boolean)):
                continue

            col_name, qualifier = self._extract_col_and_qualifier(left)
            if col_name is None:
                continue

            # If the predicate is qualified with a table alias (e.g., gh.status),
            # only push it down for the table that owns that alias.
            if qualifier and qualifier.lower() not in table_aliases:
                continue  # belongs to another table

            value = right.this if isinstance(right, exp.Literal) else right.name

            if col_name in pushable_fields:
                pushdown[col_name] = value
            else:
                duckdb_side[col_name] = value

        return pushdown, duckdb_side

    def _extract_col_and_qualifier(
        self, node: exp.Expression
    ) -> Tuple[str | None, str | None]:
        """
        Return (column_name, table_qualifier) from a Column expression.
        For 'gh.status': returns ('status', 'gh').
        For bare 'status': returns ('status', None).
        """
        if isinstance(node, exp.Column):
            col = node.name.lower() if node.name else None
            tbl = node.table.lower() if node.table else None
            return col, tbl
        return None, None

    def _extract_col_name(self, node: exp.Expression) -> str | None:
        """Backward-compat: extract bare column name."""
        col, _ = self._extract_col_and_qualifier(node)
        return col

    def _rewrite_sql(self, sql: str, table_names: List[str]) -> str:
        """
        Replace dotted table names with DuckDB-compatible view names.
        'github.pull_requests' → 'github_pull_requests'

        Simple string replace is safe here: table names are validated
        against the registry before this step.
        """
        result = sql
        # Sort by length descending so longer names are replaced first
        # (prevents partial replacements of substrings).
        for table_name in sorted(table_names, key=len, reverse=True):
            result = result.replace(table_name, table_name.replace(".", "_"))
        return result
