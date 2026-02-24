"""Tests for QueryPlanner, FetchNode, and ExecutionDAG."""
import pytest

from omnisql.planner.models import FetchNode, ExecutionDAG
from omnisql.planner.query_planner import QueryPlanner
from omnisql.tenant.models import TenantConfig, ConnectorConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _demo_tenant() -> TenantConfig:
    return TenantConfig(
        tenant_id="test",
        display_name="Test",
        connector_configs={
            "github": ConnectorConfig(
                connector_id="github", base_url="mock",
                pushable_filters=["status", "team_id", "author"],
            ),
            "jira": ConnectorConfig(
                connector_id="jira", base_url="mock",
                pushable_filters=["status", "project", "priority"],
            ),
            "linear": ConnectorConfig(
                connector_id="linear", base_url="mock",
                pushable_filters=["status"],
            ),
        },
        table_registry={
            "github.pull_requests": {"connector": "github", "fetch_key": "all_prs"},
            "jira.issues": {"connector": "jira", "fetch_key": "all_issues"},
            "linear.issues": {"connector": "linear", "fetch_key": "all_issues"},
        },
    )


# ---------------------------------------------------------------------------
# ExecutionDAG
# ---------------------------------------------------------------------------

class TestExecutionDAG:
    def test_single_node(self):
        dag = ExecutionDAG()
        dag.add_node(FetchNode(id="n1", connector_id="gh", fetch_key="prs",
                                table_name="github.pull_requests", view_name="github_pull_requests"))
        levels = dag.get_levels()
        assert len(levels) == 1
        assert levels[0][0].id == "n1"

    def test_parallel_nodes(self):
        dag = ExecutionDAG()
        dag.add_node(FetchNode(id="n1", connector_id="gh", fetch_key="prs",
                                table_name="a", view_name="a"))
        dag.add_node(FetchNode(id="n2", connector_id="ji", fetch_key="iss",
                                table_name="b", view_name="b"))
        levels = dag.get_levels()
        assert len(levels) == 1
        assert len(levels[0]) == 2

    def test_dependent_nodes(self):
        dag = ExecutionDAG()
        dag.add_node(FetchNode(id="n1", connector_id="gh", fetch_key="prs",
                                table_name="a", view_name="a"))
        dag.add_node(FetchNode(id="n2", connector_id="ji", fetch_key="iss",
                                table_name="b", view_name="b", depends_on=["n1"]))
        levels = dag.get_levels()
        assert len(levels) == 2
        assert levels[0][0].id == "n1"
        assert levels[1][0].id == "n2"

    def test_diamond_dependency(self):
        """A → B, A → C, B → D, C → D"""
        dag = ExecutionDAG()
        dag.add_node(FetchNode(id="A", connector_id="c", fetch_key="k", table_name="t", view_name="v"))
        dag.add_node(FetchNode(id="B", connector_id="c", fetch_key="k", table_name="t", view_name="v", depends_on=["A"]))
        dag.add_node(FetchNode(id="C", connector_id="c", fetch_key="k", table_name="t", view_name="v", depends_on=["A"]))
        dag.add_node(FetchNode(id="D", connector_id="c", fetch_key="k", table_name="t", view_name="v", depends_on=["B", "C"]))
        levels = dag.get_levels()
        assert len(levels) == 3
        assert levels[0][0].id == "A"
        assert set(n.id for n in levels[1]) == {"B", "C"}
        assert levels[2][0].id == "D"

    def test_cycle_detection(self):
        dag = ExecutionDAG()
        dag.add_node(FetchNode(id="A", connector_id="c", fetch_key="k", table_name="t", view_name="v", depends_on=["B"]))
        dag.add_node(FetchNode(id="B", connector_id="c", fetch_key="k", table_name="t", view_name="v", depends_on=["A"]))
        with pytest.raises(ValueError, match="cycle"):
            dag.get_levels()

    def test_empty_dag(self):
        dag = ExecutionDAG()
        assert dag.get_levels() == []


# ---------------------------------------------------------------------------
# QueryPlanner
# ---------------------------------------------------------------------------

class TestQueryPlanner:
    def setup_method(self):
        self.planner = QueryPlanner(_demo_tenant())

    # Table detection
    def test_single_table(self):
        dag = self.planner.plan("SELECT * FROM github.pull_requests")
        assert len(dag.nodes) == 1
        assert dag.nodes[0].connector_id == "github"

    def test_two_tables_join(self):
        dag = self.planner.plan(
            "SELECT * FROM github.pull_requests gh "
            "JOIN jira.issues ji ON gh.branch = ji.branch_name"
        )
        assert len(dag.nodes) == 2
        connectors = {n.connector_id for n in dag.nodes}
        assert connectors == {"github", "jira"}

    def test_three_tables(self):
        dag = self.planner.plan(
            "SELECT * FROM github.pull_requests gh "
            "JOIN jira.issues ji ON gh.branch = ji.branch_name "
            "JOIN linear.issues li ON ji.issue_key = li.id"
        )
        assert len(dag.nodes) == 3

    def test_unknown_table_raises(self):
        with pytest.raises(ValueError, match="No recognized tables"):
            self.planner.plan("SELECT * FROM salesforce.contacts")

    # SQL rewriting
    def test_rewrite_dotted_names(self):
        dag = self.planner.plan("SELECT * FROM github.pull_requests")
        assert "github_pull_requests" in dag.rewritten_sql
        assert "github.pull_requests" not in dag.rewritten_sql

    # Predicate pushdown
    def test_pushdown_single_predicate(self):
        dag = self.planner.plan("SELECT * FROM github.pull_requests WHERE status = 'merged'")
        assert dag.nodes[0].pushdown_filters == {"status": "merged"}

    def test_pushdown_respects_alias(self):
        """gh.status = 'merged' should push down to GitHub, NOT Jira."""
        dag = self.planner.plan(
            "SELECT * FROM github.pull_requests gh "
            "JOIN jira.issues ji ON gh.branch = ji.branch_name "
            "WHERE gh.status = 'merged'"
        )
        gh = next(n for n in dag.nodes if n.connector_id == "github")
        ji = next(n for n in dag.nodes if n.connector_id == "jira")
        assert gh.pushdown_filters == {"status": "merged"}
        assert ji.pushdown_filters == {}

    def test_pushdown_both_tables(self):
        """Each table gets its own pushdown filters."""
        dag = self.planner.plan(
            "SELECT * FROM github.pull_requests gh "
            "JOIN jira.issues ji ON gh.branch = ji.branch_name "
            "WHERE gh.status = 'merged' AND ji.status = 'In Progress'"
        )
        gh = next(n for n in dag.nodes if n.connector_id == "github")
        ji = next(n for n in dag.nodes if n.connector_id == "jira")
        assert gh.pushdown_filters == {"status": "merged"}
        assert ji.pushdown_filters == {"status": "In Progress"}

    def test_non_pushable_field_goes_to_duckdb(self):
        dag = self.planner.plan(
            "SELECT * FROM github.pull_requests WHERE review_status = 'approved'"
        )
        # review_status is NOT in pushable_filters
        assert dag.nodes[0].pushdown_filters == {}
        assert dag.nodes[0].duckdb_filters.get("review_status") == "approved"

    def test_no_where_clause(self):
        dag = self.planner.plan("SELECT * FROM github.pull_requests LIMIT 10")
        assert dag.nodes[0].pushdown_filters == {}
        assert dag.nodes[0].duckdb_filters == {}

    # DAG structure
    def test_all_nodes_independent_phase1(self):
        """Phase 1: no dependency edges — all nodes in one parallel wave."""
        dag = self.planner.plan(
            "SELECT * FROM github.pull_requests gh "
            "JOIN jira.issues ji ON gh.branch = ji.branch_name"
        )
        levels = dag.get_levels()
        assert len(levels) == 1
        assert len(levels[0]) == 2

    def test_view_names_correct(self):
        dag = self.planner.plan(
            "SELECT * FROM github.pull_requests gh "
            "JOIN jira.issues ji ON gh.branch = ji.branch_name"
        )
        view_names = {n.view_name for n in dag.nodes}
        assert view_names == {"github_pull_requests", "jira_issues"}
