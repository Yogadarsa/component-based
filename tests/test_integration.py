"""
Tests for FlowForge Integration Scenarios
===========================================
End-to-end tests simulating real-world workflow patterns.
"""

import pytest
import time

from flowforge import (
    WorkflowBuilder,
    WorkflowEngine,
    DAG,
    Node,
    ExecutionContext,
    EventBus,
    EventType,
    FixedRetryPolicy,
    ExponentialBackoffPolicy,
    TimeoutPolicy,
    LambdaCondition,
    ResultCondition,
    CheckpointManager,
    NodeStatus,
    WorkflowStatus,
)


# ── ETL Pipeline ─────────────────────────────────────────────────────

class TestETLPipeline:

    def test_full_etl_with_builder(self):
        """Classic Extract → Transform → Load pipeline via builder."""

        def extract(ctx):
            return {"users": [{"name": "Alice"}, {"name": "Bob"}]}

        def transform(ctx):
            raw = ctx.get_node_result("extract")
            return [u["name"].upper() for u in raw["users"]]

        def load(ctx):
            records = ctx.get_node_result("transform")
            return f"Loaded {len(records)} records"

        events_log = []

        wf = (
            WorkflowBuilder("etl_pipeline")
            .add_step("extract", extract)
            .add_step("transform", transform, depends_on=["extract"])
            .add_step("load", load, depends_on=["transform"])
            .on_complete(lambda e: events_log.append("done"))
            .build()
        )

        result = wf.run()

        assert result.is_success
        assert result.node_results["extract"]["users"][0]["name"] == "Alice"
        assert result.node_results["transform"] == ["ALICE", "BOB"]
        assert "2 records" in result.node_results["load"]
        assert "done" in events_log


# ── Parallel Fan-out / Fan-in ────────────────────────────────────────

class TestFanOutFanIn:

    def test_parallel_processing_with_aggregation(self):
        """
        Split work into parallel streams, then aggregate.

              fetch
             /  |  \
           p1  p2  p3
             \  |  /
            aggregate
        """

        def fetch(ctx):
            return [10, 20, 30]

        def process_chunk(index):
            def _process(ctx):
                data = ctx.get_node_result("fetch")
                return data[index] * 2
            return _process

        def aggregate(ctx):
            results = [
                ctx.get_node_result(f"process_{i}") for i in range(3)
            ]
            return sum(results)

        wf = (
            WorkflowBuilder("fan_out")
            .add_step("fetch", fetch)
            .add_step("process_0", process_chunk(0), depends_on=["fetch"])
            .add_step("process_1", process_chunk(1), depends_on=["fetch"])
            .add_step("process_2", process_chunk(2), depends_on=["fetch"])
            .add_step(
                "aggregate",
                aggregate,
                depends_on=["process_0", "process_1", "process_2"],
            )
            .max_workers(3)
            .build()
        )

        result = wf.run()

        assert result.is_success
        assert result.node_results["process_0"] == 20
        assert result.node_results["process_1"] == 40
        assert result.node_results["process_2"] == 60
        assert result.node_results["aggregate"] == 120


# ── Conditional Branching ────────────────────────────────────────────

class TestConditionalBranching:

    def test_if_else_branching(self):
        """
        validate → success_path (if valid)
                 → error_path   (if not valid)
        """

        def validate(ctx):
            return ctx.get("data_quality", "good")

        def success_path(ctx):
            return "Processed successfully"

        def error_path(ctx):
            return "Sent to error queue"

        wf = (
            WorkflowBuilder("branching")
            .add_step("validate", validate)
            .add_step(
                "success_path",
                success_path,
                depends_on=["validate"],
                condition=ResultCondition("validate", expected_value="good"),
            )
            .add_step(
                "error_path",
                error_path,
                depends_on=["validate"],
                condition=ResultCondition("validate", expected_value="bad"),
            )
            .build()
        )

        # Good data
        result = wf.run(ExecutionContext({"data_quality": "good"}))
        assert result.is_success
        assert result.node_results.get("success_path") == "Processed successfully"
        assert "error_path" not in result.node_results


# ── Retry with Recovery ──────────────────────────────────────────────

class TestRetryRecovery:

    def test_transient_failure_recovery(self):
        """Node fails twice then succeeds on third attempt."""
        call_count = {"n": 0}

        def flaky_step(ctx):
            call_count["n"] += 1
            if call_count["n"] < 3:
                raise ConnectionError("Transient failure")
            return "recovered"

        wf = (
            WorkflowBuilder("retry_test")
            .add_step(
                "flaky",
                flaky_step,
                retry=FixedRetryPolicy(max_retries=3, delay=0.01),
            )
            .build()
        )

        result = wf.run()

        assert result.is_success
        assert result.node_results["flaky"] == "recovered"
        assert call_count["n"] == 3

    def test_exhausted_retries_fail(self):
        """Node fails more times than max_retries allows."""

        def always_fail(ctx):
            raise RuntimeError("permanent")

        wf = (
            WorkflowBuilder("exhaust_test")
            .add_step(
                "doomed",
                always_fail,
                retry=FixedRetryPolicy(max_retries=2, delay=0.01),
            )
            .build()
        )

        result = wf.run()

        assert not result.is_success
        assert "doomed" in result.errors


# ── Event Tracking ───────────────────────────────────────────────────

class TestEventTracking:

    def test_full_lifecycle_events(self):
        """Track all events emitted during a workflow run."""
        bus = EventBus()
        bus.enable_history()

        dag = DAG("events_test")
        dag.add_node(Node("a", lambda ctx: "ok"))

        engine = WorkflowEngine(event_bus=bus)
        result = engine.run(dag)

        event_types = [e.event_type for e in bus.history]

        assert EventType.WORKFLOW_STARTED in event_types
        assert EventType.NODE_STARTED in event_types
        assert EventType.NODE_COMPLETED in event_types
        assert EventType.WORKFLOW_COMPLETED in event_types


# ── Checkpoint + Resume ──────────────────────────────────────────────

class TestCheckpointResume:

    def test_save_and_restore_checkpoint(self):
        """Verify that checkpoint state is correctly captured."""
        mgr = CheckpointManager()

        dag = DAG("cp_test")
        dag.add_node(Node("a", lambda ctx: 42))
        dag.add_node(Node("b", lambda ctx: 84))
        dag.add_edge("a", "b")

        # Run first node manually
        engine = WorkflowEngine(checkpoint_manager=mgr)
        result = engine.run(dag)

        # Save checkpoint after full run
        ctx = result.context
        cp_id = mgr.save(dag, ctx)

        # Verify checkpoint exists
        cps = mgr.list_checkpoints("cp_test")
        assert len(cps) == 1

        # Restore
        dag_state, ctx_snap = mgr.restore(cp_id)
        assert dag_state["nodes"]["a"]["status"] == "COMPLETED"
        assert ctx_snap["node_results"]["a"] == 42


# ── Complex Multi-Pattern Workflow ───────────────────────────────────

class TestComplexWorkflow:

    def test_mixed_patterns(self):
        """
        Combines: sequential → parallel → conditional → aggregation.

              init
             /    \
          fast    slow
             \    /
            decide
            /    \
        pathA   pathB (conditional)
            \    /
            finish
        """

        def init(ctx):
            ctx.set("mode", "fast")
            return "initialized"

        def fast_process(ctx):
            return 10

        def slow_process(ctx):
            return 20

        def decide(ctx):
            fast_r = ctx.get_node_result("fast")
            slow_r = ctx.get_node_result("slow")
            return fast_r + slow_r  # 30

        def path_a(ctx):
            return "path_a_result"

        def path_b(ctx):
            return "path_b_result"

        def finish(ctx):
            results = []
            for nid in ("path_a", "path_b"):
                r = ctx.get_node_result(nid)
                if r is not None:
                    results.append(r)
            return f"finished with {len(results)} path(s)"

        wf = (
            WorkflowBuilder("complex")
            .add_step("init", init)
            .add_step("fast", fast_process, depends_on=["init"])
            .add_step("slow", slow_process, depends_on=["init"])
            .add_step("decide", decide, depends_on=["fast", "slow"])
            .add_step(
                "path_a",
                path_a,
                depends_on=["decide"],
                condition=ResultCondition("decide", expected_value=20, operator="gt"),
            )
            .add_step(
                "path_b",
                path_b,
                depends_on=["decide"],
                condition=ResultCondition("decide", expected_value=50, operator="gt"),
            )
            .add_step("finish", finish, depends_on=["path_a", "path_b"])
            .max_workers(2)
            .build()
        )

        result = wf.run()

        assert result.is_success
        assert result.node_results["decide"] == 30
        assert result.node_results["path_a"] == "path_a_result"
        # path_b skipped because 30 is not > 50
        assert "path_b" not in result.node_results
        assert "1 path(s)" in result.node_results["finish"]
