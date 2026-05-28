"""
Tests for FlowForge Checkpoint Manager
========================================
Covers save, restore, list, delete, and clear operations.
"""

import pytest

from flowforge.core.dag import DAG
from flowforge.core.node import Node
from flowforge.core.context import ExecutionContext
from flowforge.checkpoint.manager import CheckpointManager
from flowforge.exceptions import CheckpointError
from flowforge.enums import NodeStatus


# ── Helpers ──────────────────────────────────────────────────────────

def make_test_dag():
    dag = DAG("test_workflow")
    dag.add_node(Node("a", lambda ctx: None))
    dag.add_node(Node("b", lambda ctx: None))
    dag.add_edge("a", "b")
    return dag


def make_test_context():
    ctx = ExecutionContext({"env": "test"}, workflow_id="test_workflow")
    ctx.set_node_result("a", {"data": [1, 2, 3]})
    return ctx


# ── Save ─────────────────────────────────────────────────────────────

class TestSave:

    def test_save_returns_checkpoint_id(self):
        mgr = CheckpointManager()
        dag = make_test_dag()
        ctx = make_test_context()
        cp_id = mgr.save(dag, ctx)
        assert isinstance(cp_id, str)
        assert len(cp_id) > 0

    def test_save_increments_count(self):
        mgr = CheckpointManager()
        dag = make_test_dag()
        ctx = make_test_context()
        mgr.save(dag, ctx)
        mgr.save(dag, ctx)
        assert len(mgr) == 2

    def test_save_with_metadata(self):
        mgr = CheckpointManager()
        dag = make_test_dag()
        ctx = make_test_context()
        cp_id = mgr.save(dag, ctx, metadata={"reason": "manual_pause"})
        cp = mgr.get(cp_id)
        assert cp.metadata["reason"] == "manual_pause"


# ── Restore ──────────────────────────────────────────────────────────

class TestRestore:

    def test_restore_returns_state(self):
        mgr = CheckpointManager()
        dag = make_test_dag()
        dag.get_node("a").status = NodeStatus.COMPLETED
        ctx = make_test_context()

        cp_id = mgr.save(dag, ctx)
        dag_state, ctx_snapshot = mgr.restore(cp_id)

        assert "a" in dag_state["nodes"]
        assert dag_state["nodes"]["a"]["status"] == "COMPLETED"
        assert ctx_snapshot["data"]["env"] == "test"
        assert ctx_snapshot["node_results"]["a"] == {"data": [1, 2, 3]}

    def test_restore_nonexistent_raises(self):
        mgr = CheckpointManager()
        with pytest.raises(CheckpointError):
            mgr.restore("nonexistent-id")

    def test_restore_is_deep_copy(self):
        mgr = CheckpointManager()
        dag = make_test_dag()
        ctx = make_test_context()
        cp_id = mgr.save(dag, ctx)

        dag_state1, _ = mgr.restore(cp_id)
        dag_state2, _ = mgr.restore(cp_id)

        # Should be separate objects
        assert dag_state1 is not dag_state2


# ── List ─────────────────────────────────────────────────────────────

class TestListCheckpoints:

    def test_list_all(self):
        mgr = CheckpointManager()
        dag = make_test_dag()
        ctx = make_test_context()
        mgr.save(dag, ctx)
        mgr.save(dag, ctx)

        cps = mgr.list_checkpoints()
        assert len(cps) == 2

    def test_list_filtered_by_workflow(self):
        mgr = CheckpointManager()
        dag1 = DAG("workflow_a")
        dag1.add_node(Node("x", lambda ctx: None))
        dag2 = DAG("workflow_b")
        dag2.add_node(Node("y", lambda ctx: None))
        ctx = ExecutionContext()

        mgr.save(dag1, ctx)
        mgr.save(dag2, ctx)

        cps_a = mgr.list_checkpoints("workflow_a")
        assert len(cps_a) == 1
        assert cps_a[0].workflow_name == "workflow_a"

    def test_list_sorted_newest_first(self):
        import time
        mgr = CheckpointManager()
        dag = make_test_dag()
        ctx = make_test_context()

        id1 = mgr.save(dag, ctx)
        time.sleep(0.01)
        id2 = mgr.save(dag, ctx)

        cps = mgr.list_checkpoints()
        assert cps[0].checkpoint_id == id2  # Newest first


# ── Delete ───────────────────────────────────────────────────────────

class TestDelete:

    def test_delete_existing(self):
        mgr = CheckpointManager()
        dag = make_test_dag()
        ctx = make_test_context()
        cp_id = mgr.save(dag, ctx)

        assert mgr.delete(cp_id) is True
        assert len(mgr) == 0

    def test_delete_nonexistent(self):
        mgr = CheckpointManager()
        assert mgr.delete("ghost") is False


# ── Clear ────────────────────────────────────────────────────────────

class TestClear:

    def test_clear_all(self):
        mgr = CheckpointManager()
        dag = make_test_dag()
        ctx = make_test_context()
        mgr.save(dag, ctx)
        mgr.save(dag, ctx)

        count = mgr.clear()
        assert count == 2
        assert len(mgr) == 0

    def test_clear_by_workflow(self):
        mgr = CheckpointManager()
        dag1 = DAG("wf_a")
        dag1.add_node(Node("x", lambda ctx: None))
        dag2 = DAG("wf_b")
        dag2.add_node(Node("y", lambda ctx: None))
        ctx = ExecutionContext()

        mgr.save(dag1, ctx)
        mgr.save(dag2, ctx)

        count = mgr.clear("wf_a")
        assert count == 1
        assert len(mgr) == 1


# ── Context snapshot roundtrip ───────────────────────────────────────

class TestContextSnapshot:

    def test_snapshot_roundtrip(self):
        ctx = ExecutionContext({"key": "value"}, workflow_id="wf_1")
        ctx.set_node_result("step1", [1, 2, 3])

        snap = ctx.snapshot()
        restored = ExecutionContext.from_snapshot(snap)

        assert restored.get("key") == "value"
        assert restored.get_node_result("step1") == [1, 2, 3]
        assert restored.workflow_id == "wf_1"
