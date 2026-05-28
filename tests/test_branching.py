"""
Tests for FlowForge Branching Conditions
==========================================
Covers LambdaCondition, ResultCondition, AlwaysTrue/False, composites,
and integration with the engine (node skipping).
"""

import pytest

from flowforge.core.context import ExecutionContext
from flowforge.core.dag import DAG
from flowforge.core.node import Node
from flowforge.core.engine import WorkflowEngine
from flowforge.branching.conditions import (
    Condition,
    LambdaCondition,
    ResultCondition,
    AlwaysTrue,
    AlwaysFalse,
    AndCondition,
    OrCondition,
    NotCondition,
)
from flowforge.enums import NodeStatus, WorkflowStatus


# ── LambdaCondition ──────────────────────────────────────────────────

class TestLambdaCondition:

    def test_true_predicate(self):
        cond = LambdaCondition(lambda ctx: True)
        assert cond.evaluate(ExecutionContext())

    def test_false_predicate(self):
        cond = LambdaCondition(lambda ctx: False)
        assert not cond.evaluate(ExecutionContext())

    def test_context_aware(self):
        cond = LambdaCondition(lambda ctx: ctx.get("env") == "prod")
        ctx = ExecutionContext({"env": "prod"})
        assert cond.evaluate(ctx)

        ctx2 = ExecutionContext({"env": "dev"})
        assert not cond.evaluate(ctx2)

    def test_repr(self):
        cond = LambdaCondition(lambda ctx: True, description="is_prod")
        assert "is_prod" in repr(cond)


# ── ResultCondition ──────────────────────────────────────────────────

class TestResultCondition:

    def setup_method(self):
        self.ctx = ExecutionContext()
        self.ctx.set_node_result("validator", True)
        self.ctx.set_node_result("counter", 42)
        self.ctx.set_node_result("items", [1, 2, 3])

    def test_eq(self):
        cond = ResultCondition("validator", expected_value=True, operator="eq")
        assert cond.evaluate(self.ctx)

    def test_neq(self):
        cond = ResultCondition("validator", expected_value=False, operator="neq")
        assert cond.evaluate(self.ctx)

    def test_gt(self):
        cond = ResultCondition("counter", expected_value=10, operator="gt")
        assert cond.evaluate(self.ctx)

    def test_lt(self):
        cond = ResultCondition("counter", expected_value=100, operator="lt")
        assert cond.evaluate(self.ctx)

    def test_gte(self):
        cond = ResultCondition("counter", expected_value=42, operator="gte")
        assert cond.evaluate(self.ctx)

    def test_lte(self):
        cond = ResultCondition("counter", expected_value=42, operator="lte")
        assert cond.evaluate(self.ctx)

    def test_contains(self):
        cond = ResultCondition("items", expected_value=2, operator="contains")
        assert cond.evaluate(self.ctx)

    def test_missing_node_returns_false(self):
        cond = ResultCondition("ghost", expected_value=True)
        assert not cond.evaluate(self.ctx)

    def test_invalid_operator_raises(self):
        with pytest.raises(ValueError):
            ResultCondition("x", expected_value=1, operator="nope")


# ── AlwaysTrue / AlwaysFalse ─────────────────────────────────────────

class TestConstantConditions:

    def test_always_true(self):
        assert AlwaysTrue().evaluate(ExecutionContext())

    def test_always_false(self):
        assert not AlwaysFalse().evaluate(ExecutionContext())


# ── Composite conditions ─────────────────────────────────────────────

class TestCompositeConditions:

    def test_and(self):
        cond = AlwaysTrue() & AlwaysFalse()
        assert isinstance(cond, AndCondition)
        assert not cond.evaluate(ExecutionContext())

    def test_and_both_true(self):
        cond = AlwaysTrue() & AlwaysTrue()
        assert cond.evaluate(ExecutionContext())

    def test_or(self):
        cond = AlwaysTrue() | AlwaysFalse()
        assert isinstance(cond, OrCondition)
        assert cond.evaluate(ExecutionContext())

    def test_or_both_false(self):
        cond = AlwaysFalse() | AlwaysFalse()
        assert not cond.evaluate(ExecutionContext())

    def test_not(self):
        cond = ~AlwaysTrue()
        assert isinstance(cond, NotCondition)
        assert not cond.evaluate(ExecutionContext())

    def test_complex_composite(self):
        # (True AND False) OR (NOT False) → False OR True → True
        cond = (AlwaysTrue() & AlwaysFalse()) | (~AlwaysFalse())
        assert cond.evaluate(ExecutionContext())


# ── Engine integration ───────────────────────────────────────────────

class TestConditionEngineIntegration:

    def test_node_skipped_when_condition_false(self):
        dag = DAG("cond_test")
        dag.add_node(Node("a", lambda ctx: "ok"))
        dag.add_node(
            Node("b", lambda ctx: "should_skip", condition=AlwaysFalse())
        )
        dag.add_edge("a", "b")

        result = WorkflowEngine().run(dag)

        assert result.is_success
        assert dag.get_node("b").status == NodeStatus.SKIPPED
        assert "b" not in result.node_results

    def test_node_runs_when_condition_true(self):
        dag = DAG("cond_true")
        dag.add_node(
            Node("a", lambda ctx: "ran", condition=AlwaysTrue())
        )

        result = WorkflowEngine().run(dag)

        assert result.is_success
        assert result.node_results["a"] == "ran"

    def test_conditional_branching(self):
        """Only the 'success_path' should run based on validator result."""
        dag = DAG("branch")
        dag.add_node(Node("validate", lambda ctx: True))
        dag.add_node(
            Node(
                "success_path",
                lambda ctx: "success!",
                condition=ResultCondition("validate", expected_value=True),
            )
        )
        dag.add_node(
            Node(
                "failure_path",
                lambda ctx: "failure!",
                condition=ResultCondition("validate", expected_value=False),
            )
        )
        dag.add_edge("validate", "success_path")
        dag.add_edge("validate", "failure_path")

        result = WorkflowEngine().run(dag)

        assert result.is_success
        assert result.node_results.get("success_path") == "success!"
        assert "failure_path" not in result.node_results
        assert dag.get_node("failure_path").status == NodeStatus.SKIPPED
