"""
Tests for FlowForge WorkflowEngine
====================================
Covers sequential execution, parallel execution, failure handling,
fail-fast mode, cancellation, and result collection.
"""

import pytest
import time
import threading

from flowforge.core.dag import DAG
from flowforge.core.node import Node
from flowforge.core.context import ExecutionContext
from flowforge.core.engine import WorkflowEngine
from flowforge.enums import NodeStatus, WorkflowStatus


# ── Helpers ──────────────────────────────────────────────────────────

def make_step(value):
    """Create a simple step that returns a value."""
    def step(ctx):
        return value
    return step


def make_slow_step(seconds, value):
    """Create a step that sleeps then returns a value."""
    def step(ctx):
        time.sleep(seconds)
        return value
    return step


def make_fail_step(error_cls=ValueError, msg="boom"):
    """Create a step that always raises."""
    def step(ctx):
        raise error_cls(msg)
    return step


def make_accumulator_step(node_id, source_ids):
    """Create a step that sums results from source nodes."""
    def step(ctx):
        total = sum(ctx.get_node_result(sid, 0) for sid in source_ids)
        return total
    return step


# ── Sequential execution ─────────────────────────────────────────────

class TestSequentialExecution:

    def test_linear_pipeline(self):
        dag = DAG("linear")
        dag.add_node(Node("a", make_step(10)))
        dag.add_node(Node("b", make_step(20)))
        dag.add_node(Node("c", make_step(30)))
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")

        engine = WorkflowEngine(max_workers=1)
        result = engine.run(dag)

        assert result.is_success
        assert result.status == WorkflowStatus.COMPLETED
        assert result.node_results["a"] == 10
        assert result.node_results["b"] == 20
        assert result.node_results["c"] == 30

    def test_data_passing_between_nodes(self):
        def extract(ctx):
            return [1, 2, 3]

        def transform(ctx):
            data = ctx.get_node_result("extract")
            return [x * 2 for x in data]

        def load(ctx):
            data = ctx.get_node_result("transform")
            return sum(data)

        dag = DAG("etl")
        dag.add_node(Node("extract", extract))
        dag.add_node(Node("transform", transform))
        dag.add_node(Node("load", load))
        dag.add_edge("extract", "transform")
        dag.add_edge("transform", "load")

        result = WorkflowEngine().run(dag)

        assert result.is_success
        assert result.node_results["extract"] == [1, 2, 3]
        assert result.node_results["transform"] == [2, 4, 6]
        assert result.node_results["load"] == 12

    def test_single_node(self):
        dag = DAG("solo")
        dag.add_node(Node("only", make_step(42)))

        result = WorkflowEngine().run(dag)
        assert result.is_success
        assert result.node_results["only"] == 42


# ── Parallel execution ───────────────────────────────────────────────

class TestParallelExecution:

    def test_independent_nodes_run_in_parallel(self):
        """Two independent nodes should run concurrently."""
        dag = DAG("parallel")
        dag.add_node(Node("a", make_slow_step(0.1, "a_result")))
        dag.add_node(Node("b", make_slow_step(0.1, "b_result")))

        engine = WorkflowEngine(max_workers=2)
        start = time.time()
        result = engine.run(dag)
        elapsed = time.time() - start

        assert result.is_success
        # If parallel, should take ~0.1s not ~0.2s
        assert elapsed < 0.25

    def test_diamond_execution(self):
        """
             A (10)
            / \
           B   C
           |   |
            \ /
             D (sum of B + C)
        """
        dag = DAG("diamond")
        dag.add_node(Node("a", make_step(10)))
        dag.add_node(Node("b", lambda ctx: ctx.get_node_result("a") * 2))
        dag.add_node(Node("c", lambda ctx: ctx.get_node_result("a") * 3))
        dag.add_node(Node("d", make_accumulator_step("d", ["b", "c"])))
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")

        result = WorkflowEngine(max_workers=2).run(dag)

        assert result.is_success
        assert result.node_results["a"] == 10
        assert result.node_results["b"] == 20
        assert result.node_results["c"] == 30
        assert result.node_results["d"] == 50


# ── Failure handling ─────────────────────────────────────────────────

class TestFailureHandling:

    def test_node_failure_marks_workflow_failed(self):
        dag = DAG("fail")
        dag.add_node(Node("a", make_step(1)))
        dag.add_node(Node("b", make_fail_step()))
        dag.add_edge("a", "b")

        result = WorkflowEngine().run(dag)

        assert not result.is_success
        assert result.status == WorkflowStatus.FAILED
        assert "b" in result.errors

    def test_fail_fast_prevents_downstream(self):
        """In fail-fast mode, nodes downstream of a failure shouldn't run."""
        dag = DAG("fail_fast")
        dag.add_node(Node("a", make_fail_step()))
        dag.add_node(Node("b", make_step("should_not_run")))
        dag.add_edge("a", "b")

        result = WorkflowEngine(fail_fast=True).run(dag)

        assert result.status == WorkflowStatus.FAILED
        assert "b" not in result.node_results

    def test_non_fail_fast_continues_independent(self):
        """Independent branches should continue when fail_fast=False."""
        dag = DAG("no_fail_fast")
        dag.add_node(Node("good", make_step("ok")))
        dag.add_node(Node("bad", make_fail_step()))

        result = WorkflowEngine(fail_fast=False).run(dag)

        # Both ran; one succeeded, one failed
        assert result.status == WorkflowStatus.FAILED
        assert result.node_results.get("good") == "ok"
        assert "bad" in result.errors


# ── Context ──────────────────────────────────────────────────────────

class TestContextIntegration:

    def test_shared_context_data(self):
        def write_ctx(ctx):
            ctx.set("shared_key", "hello")
            return "wrote"

        def read_ctx(ctx):
            return ctx.get("shared_key")

        dag = DAG("ctx_test")
        dag.add_node(Node("writer", write_ctx))
        dag.add_node(Node("reader", read_ctx))
        dag.add_edge("writer", "reader")

        result = WorkflowEngine().run(dag)

        assert result.is_success
        assert result.node_results["reader"] == "hello"

    def test_initial_context_data(self):
        def use_env(ctx):
            return ctx.get("env")

        dag = DAG("env_test")
        dag.add_node(Node("step", use_env))

        ctx = ExecutionContext({"env": "production"})
        result = WorkflowEngine().run(dag, ctx)

        assert result.node_results["step"] == "production"


# ── Duration tracking ────────────────────────────────────────────────

class TestDuration:

    def test_duration_is_positive(self):
        dag = DAG("timing")
        dag.add_node(Node("step", make_slow_step(0.05, "done")))

        result = WorkflowEngine().run(dag)

        assert result.duration_seconds > 0
        assert result.started_at is not None
        assert result.completed_at is not None


# ── Validation ───────────────────────────────────────────────────────

class TestValidation:

    def test_empty_dag_raises(self):
        from flowforge.exceptions import WorkflowValidationError

        dag = DAG("empty")
        engine = WorkflowEngine()

        with pytest.raises(WorkflowValidationError):
            engine.run(dag)
