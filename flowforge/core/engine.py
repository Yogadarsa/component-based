"""
FlowForge Workflow Engine
==========================
The central execution engine that runs a workflow DAG.

The engine orchestrates node execution by:
1. Computing a topological order
2. Discovering ready nodes (dependencies satisfied)
3. Running ready nodes — in parallel when possible
4. Applying retry policies and timeout enforcement
5. Evaluating conditional gates (skip or proceed)
6. Emitting lifecycle events
7. Supporting pause / resume / cancel
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from flowforge.core.context import ExecutionContext
from flowforge.core.dag import DAG
from flowforge.core.node import Node
from flowforge.enums import EventType, NodeStatus, WorkflowStatus
from flowforge.events.hooks import EventBus
from flowforge.checkpoint.manager import CheckpointManager
from flowforge.exceptions import (
    FlowForgeError,
    NodeExecutionError,
    WorkflowTimeoutError,
    WorkflowValidationError,
)

logger = logging.getLogger("flowforge.engine")


class WorkflowResult:
    """
    Immutable result object returned after a workflow completes.

    Attributes
    ----------
    status : WorkflowStatus
        Final status of the workflow.
    context : ExecutionContext
        The execution context with all data and node results.
    node_results : dict
        Mapping of ``node_id → result`` for completed nodes.
    errors : dict
        Mapping of ``node_id → exception`` for failed nodes.
    duration_seconds : float
        Wall-clock time for the entire workflow run.
    started_at : datetime
        When execution began.
    completed_at : datetime
        When execution finished.
    """

    __slots__ = (
        "status",
        "context",
        "node_results",
        "errors",
        "duration_seconds",
        "started_at",
        "completed_at",
    )

    def __init__(
        self,
        status: WorkflowStatus,
        context: ExecutionContext,
        node_results: Dict[str, Any],
        errors: Dict[str, Exception],
        duration_seconds: float,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        self.status = status
        self.context = context
        self.node_results = node_results
        self.errors = errors
        self.duration_seconds = duration_seconds
        self.started_at = started_at
        self.completed_at = completed_at

    @property
    def is_success(self) -> bool:
        """Return ``True`` if the workflow completed without errors."""
        return self.status == WorkflowStatus.COMPLETED

    def __repr__(self) -> str:
        return (
            f"WorkflowResult(status={self.status.name}, "
            f"completed={len(self.node_results)}, "
            f"failed={len(self.errors)}, "
            f"duration={self.duration_seconds:.3f}s)"
        )


class WorkflowEngine:
    """
    Executes a workflow DAG with full lifecycle management.

    Parameters
    ----------
    max_workers : int
        Maximum threads for parallel node execution (default 4).
    event_bus : EventBus, optional
        Custom event bus (a new one is created if omitted).
    checkpoint_manager : CheckpointManager, optional
        Custom checkpoint manager (a new one is created if omitted).
    fail_fast : bool
        If ``True`` (default), the workflow stops as soon as any node
        fails fatally. If ``False``, independent branches continue.

    Examples
    --------
    >>> engine = WorkflowEngine()
    >>> result = engine.run(my_dag)
    >>> print(result.status)
    WorkflowStatus.COMPLETED
    """

    def __init__(
        self,
        max_workers: int = 4,
        event_bus: Optional[EventBus] = None,
        checkpoint_manager: Optional[CheckpointManager] = None,
        fail_fast: bool = True,
    ) -> None:
        self.max_workers = max_workers
        self.event_bus = event_bus or EventBus()
        self.checkpoint_manager = checkpoint_manager or CheckpointManager()
        self.fail_fast = fail_fast

        # Internal state
        self._status = WorkflowStatus.CREATED
        self._lock = threading.Lock()
        self._cancel_event = threading.Event()
        self._pause_event = threading.Event()
        self._fail_fast_triggered = False
        self._errors: Dict[str, Exception] = {}
        self._started_at: Optional[datetime] = None
        self._completed_at: Optional[datetime] = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def status(self) -> WorkflowStatus:
        """Current workflow status."""
        return self._status

    def run(
        self,
        dag: DAG,
        context: Optional[ExecutionContext] = None,
    ) -> WorkflowResult:
        """
        Execute the workflow DAG.

        Parameters
        ----------
        dag : DAG
            The workflow graph to execute.
        context : ExecutionContext, optional
            Shared context; a fresh one is created if omitted.

        Returns
        -------
        WorkflowResult
            Summary of the execution.

        Raises
        ------
        WorkflowValidationError
            If the DAG fails validation.
        """
        # Validate
        issues = dag.validate()
        if issues:
            raise WorkflowValidationError(issues)

        # Initialise
        ctx = context or ExecutionContext(workflow_id=dag.name)
        self._status = WorkflowStatus.RUNNING
        self._cancel_event.clear()
        self._pause_event.clear()
        self._fail_fast_triggered = False
        self._errors.clear()
        self._started_at = datetime.now()

        self.event_bus.emit(
            EventType.WORKFLOW_STARTED,
            data={"workflow": dag.name, "nodes": dag.node_ids},
        )

        logger.info("Workflow '%s' started with %d nodes", dag.name, dag.node_count)

        try:
            self._execute_dag(dag, ctx)
        except FlowForgeError:
            self._status = WorkflowStatus.FAILED
        except Exception as exc:
            logger.exception("Unexpected error in workflow '%s'", dag.name)
            self._status = WorkflowStatus.FAILED

        self._completed_at = datetime.now()
        duration = (self._completed_at - self._started_at).total_seconds()

        # Determine final status
        if self._cancel_event.is_set() and not self._fail_fast_triggered:
            self._status = WorkflowStatus.CANCELLED
        elif self._pause_event.is_set():
            self._status = WorkflowStatus.PAUSED
        elif self._errors:
            self._status = WorkflowStatus.FAILED
        elif self._status == WorkflowStatus.RUNNING:
            self._status = WorkflowStatus.COMPLETED

        # Collect results
        node_results = {
            nid: node.result
            for nid, node in dag.nodes.items()
            if node.status == NodeStatus.COMPLETED
        }

        # Emit completion event
        if self._status == WorkflowStatus.COMPLETED:
            self.event_bus.emit(
                EventType.WORKFLOW_COMPLETED,
                data={"workflow": dag.name, "duration": duration},
            )
        elif self._status == WorkflowStatus.FAILED:
            self.event_bus.emit(
                EventType.WORKFLOW_FAILED,
                data={
                    "workflow": dag.name,
                    "errors": {k: str(v) for k, v in self._errors.items()},
                },
            )

        logger.info(
            "Workflow '%s' finished: %s (%.3fs)",
            dag.name,
            self._status.name,
            duration,
        )

        return WorkflowResult(
            status=self._status,
            context=ctx,
            node_results=node_results,
            errors=dict(self._errors),
            duration_seconds=duration,
            started_at=self._started_at,
            completed_at=self._completed_at,
        )

    def pause(self, dag: DAG, context: ExecutionContext) -> Optional[str]:
        """
        Pause the workflow and save a checkpoint.

        Returns the checkpoint ID, or ``None`` if pausing failed.
        """
        self._pause_event.set()
        self._status = WorkflowStatus.PAUSED
        self.event_bus.emit(EventType.WORKFLOW_PAUSED, data={"workflow": dag.name})
        try:
            cp_id = self.checkpoint_manager.save(dag, context)
            self.event_bus.emit(
                EventType.CHECKPOINT_SAVED,
                data={"checkpoint_id": cp_id},
            )
            logger.info("Workflow '%s' paused — checkpoint %s", dag.name, cp_id[:8])
            return cp_id
        except Exception as exc:
            logger.error("Failed to save checkpoint: %s", exc)
            return None

    def resume(
        self,
        dag: DAG,
        checkpoint_id: str,
    ) -> WorkflowResult:
        """
        Resume a paused workflow from a checkpoint.

        Parameters
        ----------
        dag : DAG
            The original DAG (with node callables still attached).
        checkpoint_id : str
            The checkpoint to restore from.
        """
        dag_state, ctx_snapshot = self.checkpoint_manager.restore(checkpoint_id)

        # Restore node statuses
        for node_id, node_data in dag_state.get("nodes", {}).items():
            if dag.has_node(node_id):
                node = dag.get_node(node_id)
                node.restore_status(
                    node_data["status"],
                    result=node_data.get("result"),
                )

        # Restore context
        ctx = ExecutionContext.from_snapshot(ctx_snapshot)

        self.event_bus.emit(
            EventType.CHECKPOINT_RESTORED,
            data={"checkpoint_id": checkpoint_id, "workflow": dag.name},
        )

        logger.info(
            "Resuming workflow '%s' from checkpoint %s",
            dag.name,
            checkpoint_id[:8],
        )

        # Re-run — the engine will skip completed/skipped nodes
        self._pause_event.clear()
        return self.run(dag, ctx)

    def cancel(self) -> None:
        """Cancel the running workflow."""
        self._cancel_event.set()
        self._status = WorkflowStatus.CANCELLED
        logger.info("Workflow cancel requested")

    # ------------------------------------------------------------------
    # Internal execution loop
    # ------------------------------------------------------------------

    def _execute_dag(self, dag: DAG, ctx: ExecutionContext) -> None:
        """
        Core execution loop: repeatedly discover ready nodes and run them.
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            while True:
                # Check for cancel/pause
                if self._cancel_event.is_set():
                    self.event_bus.emit(
                        EventType.WORKFLOW_CANCELLED,
                        data={"workflow": dag.name},
                    )
                    return
                if self._pause_event.is_set():
                    return

                ready_nodes = dag.get_ready_nodes()

                if not ready_nodes:
                    # No ready nodes — either done or deadlocked
                    pending = [
                        n
                        for n in dag.nodes.values()
                        if n.status == NodeStatus.PENDING
                    ]
                    if pending and self._errors and self.fail_fast:
                        # Nodes remain but upstream failures prevent them
                        return
                    if not pending:
                        return  # All done
                    # Shouldn't happen in a valid DAG
                    logger.warning(
                        "Deadlock: %d pending nodes but none are ready",
                        len(pending),
                    )
                    return

                # Submit all ready nodes in parallel
                futures: Dict[Future, Node] = {}
                for node in ready_nodes:
                    node.status = NodeStatus.READY
                    future = pool.submit(self._execute_node, node, dag, ctx)
                    futures[future] = node

                # Wait for this batch
                for future in as_completed(futures):
                    node = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        # Already handled inside _execute_node
                        pass

                    if (
                        self.fail_fast
                        and self._errors
                        and not self._cancel_event.is_set()
                    ):
                        self._fail_fast_triggered = True
                        self._cancel_event.set()
                        return

    def _execute_node(
        self,
        node: Node,
        dag: DAG,
        ctx: ExecutionContext,
    ) -> None:
        """
        Execute a single node, handling conditions, retries, and timeouts.
        """
        # Evaluate condition
        if node.condition is not None:
            try:
                should_run = node.condition.evaluate(ctx)
            except Exception:
                should_run = False
            if not should_run:
                node.status = NodeStatus.SKIPPED
                logger.info("Node '%s' skipped (condition = False)", node.node_id)
                self.event_bus.emit(
                    EventType.NODE_SKIPPED,
                    node_id=node.node_id,
                )
                return

        # Mark running
        node.status = NodeStatus.RUNNING
        node.metrics.mark_started()
        self.event_bus.emit(EventType.NODE_STARTED, node_id=node.node_id)

        logger.info("Node '%s' started", node.node_id)

        # Retry loop
        from flowforge.policies.retry import NoRetryPolicy

        retry_policy = node.retry_policy or NoRetryPolicy()
        attempt = 0

        while True:
            try:
                # Execute with optional timeout
                if node.timeout:
                    result = node.timeout.execute(
                        node.func, ctx, node_id=node.node_id
                    )
                else:
                    result = node.func(ctx)

                # Success
                node.result = result
                node.status = NodeStatus.COMPLETED
                node.metrics.mark_completed()
                ctx.set_node_result(node.node_id, result)

                self.event_bus.emit(
                    EventType.NODE_COMPLETED,
                    node_id=node.node_id,
                    data={"duration": node.metrics.duration_seconds},
                )
                logger.info(
                    "Node '%s' completed (%.3fs)",
                    node.node_id,
                    node.metrics.duration_seconds,
                )
                return

            except Exception as exc:
                attempt += 1
                node.metrics.increment_retry()

                if retry_policy.should_retry(attempt, exc):
                    delay = retry_policy.get_delay(attempt)
                    node.status = NodeStatus.RETRYING
                    self.event_bus.emit(
                        EventType.NODE_RETRYING,
                        node_id=node.node_id,
                        data={"attempt": attempt, "delay": delay, "error": str(exc)},
                    )
                    logger.warning(
                        "Node '%s' retrying (attempt %d/%d, delay %.1fs): %s",
                        node.node_id,
                        attempt,
                        retry_policy.max_retries,
                        delay,
                        exc,
                    )
                    time.sleep(delay)
                    node.status = NodeStatus.RUNNING
                    continue

                # Fatal failure
                node.status = NodeStatus.FAILED
                node.error = exc
                node.metrics.mark_completed()

                wrapped = NodeExecutionError(node.node_id, exc)
                with self._lock:
                    self._errors[node.node_id] = wrapped

                self.event_bus.emit(
                    EventType.NODE_FAILED,
                    node_id=node.node_id,
                    data={"error": str(exc), "attempts": attempt},
                )
                logger.error(
                    "Node '%s' failed after %d attempt(s): %s",
                    node.node_id,
                    attempt,
                    exc,
                )
                return
