"""
Tests for FlowForge DAG
========================
Covers node addition, edge management, cycle detection, topological sort,
ready-node discovery, validation, and serialisation.
"""

import pytest

from flowforge.core.dag import DAG
from flowforge.core.node import Node
from flowforge.enums import NodeStatus
from flowforge.exceptions import (
    CyclicDependencyError,
    DuplicateNodeError,
    InvalidNodeError,
)


# ── Helpers ──────────────────────────────────────────────────────────

def noop(ctx):
    """No-op function for test nodes."""
    return None


def make_dag_linear():
    """A → B → C"""
    dag = DAG("linear")
    dag.add_node(Node("a", noop))
    dag.add_node(Node("b", noop))
    dag.add_node(Node("c", noop))
    dag.add_edge("a", "b")
    dag.add_edge("b", "c")
    return dag


def make_dag_diamond():
    """
         A
        / \
       B   C
        \ /
         D
    """
    dag = DAG("diamond")
    for nid in ("a", "b", "c", "d"):
        dag.add_node(Node(nid, noop))
    dag.add_edge("a", "b")
    dag.add_edge("a", "c")
    dag.add_edge("b", "d")
    dag.add_edge("c", "d")
    return dag


# ── Node management ─────────────────────────────────────────────────

class TestNodeManagement:

    def test_add_node(self):
        dag = DAG("test")
        dag.add_node(Node("n1", noop))
        assert dag.has_node("n1")
        assert dag.node_count == 1

    def test_add_duplicate_node_raises(self):
        dag = DAG("test")
        dag.add_node(Node("n1", noop))
        with pytest.raises(DuplicateNodeError):
            dag.add_node(Node("n1", noop))

    def test_get_node(self):
        dag = DAG("test")
        node = Node("n1", noop, name="First")
        dag.add_node(node)
        assert dag.get_node("n1").name == "First"

    def test_get_missing_node_raises(self):
        dag = DAG("test")
        with pytest.raises(InvalidNodeError):
            dag.get_node("nonexistent")

    def test_remove_node(self):
        dag = make_dag_linear()
        dag.remove_node("b")
        assert not dag.has_node("b")
        assert dag.node_count == 2
        # Edges involving b should be gone
        assert "b" not in dag.get_dependents("a")

    def test_remove_missing_node_raises(self):
        dag = DAG("test")
        with pytest.raises(InvalidNodeError):
            dag.remove_node("ghost")

    def test_node_ids_order(self):
        dag = DAG("test")
        for nid in ("x", "y", "z"):
            dag.add_node(Node(nid, noop))
        assert dag.node_ids == ["x", "y", "z"]

    def test_contains(self):
        dag = DAG("test")
        dag.add_node(Node("n1", noop))
        assert "n1" in dag
        assert "n2" not in dag


# ── Edge management ──────────────────────────────────────────────────

class TestEdgeManagement:

    def test_add_edge(self):
        dag = DAG("test")
        dag.add_node(Node("a", noop))
        dag.add_node(Node("b", noop))
        dag.add_edge("a", "b")
        assert dag.edge_count == 1
        assert "b" in dag.get_dependents("a")
        assert "a" in dag.get_dependencies("b")

    def test_add_edge_invalid_source_raises(self):
        dag = DAG("test")
        dag.add_node(Node("b", noop))
        with pytest.raises(InvalidNodeError):
            dag.add_edge("ghost", "b")

    def test_add_edge_invalid_target_raises(self):
        dag = DAG("test")
        dag.add_node(Node("a", noop))
        with pytest.raises(InvalidNodeError):
            dag.add_edge("a", "ghost")

    def test_duplicate_edge_is_idempotent(self):
        dag = DAG("test")
        dag.add_node(Node("a", noop))
        dag.add_node(Node("b", noop))
        dag.add_edge("a", "b")
        dag.add_edge("a", "b")  # Should not raise
        assert dag.edge_count == 1

    def test_chaining(self):
        dag = DAG("test")
        result = dag.add_node(Node("a", noop)).add_node(Node("b", noop))
        assert result is dag


# ── Cycle detection ──────────────────────────────────────────────────

class TestCycleDetection:

    def test_self_loop_raises(self):
        dag = DAG("test")
        dag.add_node(Node("a", noop))
        with pytest.raises(CyclicDependencyError):
            dag.add_edge("a", "a")

    def test_direct_cycle_raises(self):
        dag = DAG("test")
        dag.add_node(Node("a", noop))
        dag.add_node(Node("b", noop))
        dag.add_edge("a", "b")
        with pytest.raises(CyclicDependencyError):
            dag.add_edge("b", "a")

    def test_indirect_cycle_raises(self):
        dag = DAG("test")
        for nid in ("a", "b", "c"):
            dag.add_node(Node(nid, noop))
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        with pytest.raises(CyclicDependencyError):
            dag.add_edge("c", "a")

    def test_valid_dag_no_cycle(self):
        dag = make_dag_diamond()
        # Should not raise
        assert not dag._has_cycle()


# ── Topological sort ─────────────────────────────────────────────────

class TestTopologicalSort:

    def test_linear_sort(self):
        dag = make_dag_linear()
        order = dag.topological_sort()
        assert order.index("a") < order.index("b") < order.index("c")

    def test_diamond_sort(self):
        dag = make_dag_diamond()
        order = dag.topological_sort()
        assert order.index("a") < order.index("b")
        assert order.index("a") < order.index("c")
        assert order.index("b") < order.index("d")
        assert order.index("c") < order.index("d")

    def test_single_node(self):
        dag = DAG("test")
        dag.add_node(Node("solo", noop))
        assert dag.topological_sort() == ["solo"]

    def test_disconnected_nodes(self):
        dag = DAG("test")
        dag.add_node(Node("x", noop))
        dag.add_node(Node("y", noop))
        order = dag.topological_sort()
        assert set(order) == {"x", "y"}


# ── Ready nodes ──────────────────────────────────────────────────────

class TestReadyNodes:

    def test_root_nodes_are_ready(self):
        dag = make_dag_linear()
        ready = dag.get_ready_nodes()
        assert len(ready) == 1
        assert ready[0].node_id == "a"

    def test_no_ready_when_deps_incomplete(self):
        dag = make_dag_linear()
        # b should not be ready until a is terminal
        ready_ids = [n.node_id for n in dag.get_ready_nodes()]
        assert "b" not in ready_ids

    def test_ready_after_dep_completes(self):
        dag = make_dag_linear()
        dag.get_node("a").status = NodeStatus.COMPLETED
        ready = dag.get_ready_nodes()
        assert len(ready) == 1
        assert ready[0].node_id == "b"

    def test_diamond_parallel_ready(self):
        dag = make_dag_diamond()
        dag.get_node("a").status = NodeStatus.COMPLETED
        ready_ids = sorted(n.node_id for n in dag.get_ready_nodes())
        assert ready_ids == ["b", "c"]

    def test_diamond_final_ready(self):
        dag = make_dag_diamond()
        for nid in ("a", "b", "c"):
            dag.get_node(nid).status = NodeStatus.COMPLETED
        ready = dag.get_ready_nodes()
        assert len(ready) == 1
        assert ready[0].node_id == "d"


# ── Validation ───────────────────────────────────────────────────────

class TestValidation:

    def test_empty_dag(self):
        dag = DAG("empty")
        issues = dag.validate()
        assert any("no nodes" in issue for issue in issues)

    def test_valid_dag(self):
        dag = make_dag_linear()
        assert dag.validate() == []

    def test_root_and_leaf_discovery(self):
        dag = make_dag_diamond()
        roots = dag.get_root_nodes()
        leaves = dag.get_leaf_nodes()
        assert len(roots) == 1
        assert roots[0].node_id == "a"
        assert len(leaves) == 1
        assert leaves[0].node_id == "d"


# ── Serialisation ────────────────────────────────────────────────────

class TestSerialisation:

    def test_to_dict(self):
        dag = make_dag_linear()
        data = dag.to_dict()
        assert data["name"] == "linear"
        assert "a" in data["nodes"]
        assert "b" in data["edges"]["a"]

    def test_reset(self):
        dag = make_dag_linear()
        dag.get_node("a").status = NodeStatus.COMPLETED
        dag.get_node("b").status = NodeStatus.FAILED
        dag.reset()
        for node in dag.nodes.values():
            assert node.status == NodeStatus.PENDING


# ── Repr ─────────────────────────────────────────────────────────────

class TestRepr:

    def test_repr(self):
        dag = make_dag_linear()
        r = repr(dag)
        assert "linear" in r
        assert "3" in r  # 3 nodes
