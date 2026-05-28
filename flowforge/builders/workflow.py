"""
FlowForge Workflow Builder
===========================
Fluent builder API and decorator for constructing workflows with
minimal boilerplate.

The builder validates the DAG on ``build()`` and returns a ready-to-run
``(DAG, WorkflowEngine)`` pair.
"""

from __future__ import annotations

import functools
from typing import Any, Callable, Dict, List, Optional, Tuple, TYPE_CHECKING

from flowforge.core.dag import DAG
from flowforge.core.engine import WorkflowEngine
from flowforge.core.node import Node
from flowforge.enums import EventType
from flowforge.events.hooks import EventBus
from flowforge.checkpoint.manager import CheckpointManager
from flowforge.exceptions import WorkflowValidationError

if TYPE_CHECKING:
    from flowforge.branching.conditions import Condition
    from flowforge.policies.retry import RetryPolicy
    from flowforge.policies.timeout import TimeoutPolicy


class WorkflowBuilder:
    """
    Fluent builder for constructing and configuring a workflow.

    Provides a chainable API that reads almost like a recipe:

    >>> wf = (WorkflowBuilder("etl")
    ...     .add_step("extract", extract_fn)
    ...     .add_step("transform", transform_fn, depends_on=["extract"])
    ...     .add_step("load", load_fn, depends_on=["transform"])
    ...     .on_complete(notify_team)
    ...     .build())
    >>> result = wf.run()

    Parameters
    ----------
    name : str
        Name for the workflow.
    description : str, optional
        Optional description.
    """

    def __init__(self, name: str, description: str = "") -> None:
        self._name = name
        self._description = description
        self._steps: List[Dict[str, Any]] = []
        self._event_bus = EventBus()
        self._checkpoint_manager = CheckpointManager()
        self._max_workers = 4
        self._fail_fast = True

    # ------------------------------------------------------------------
    # Step definition
    # ------------------------------------------------------------------

    def add_step(
        self,
        step_id: str,
        func: Callable,
        *,
        name: Optional[str] = None,
        depends_on: Optional[List[str]] = None,
        retry: Optional["RetryPolicy"] = None,
        timeout: Optional["TimeoutPolicy"] = None,
        condition: Optional["Condition"] = None,
        metadata: Optional[dict] = None,
    ) -> "WorkflowBuilder":
        """
        Add a step to the workflow.

        Parameters
        ----------
        step_id : str
            Unique step identifier.
        func : callable
            The function to execute (must accept ``ExecutionContext``).
        name : str, optional
            Human-readable name.
        depends_on : list of str, optional
            IDs of steps that must complete before this one runs.
        retry : RetryPolicy, optional
            Retry policy for this step.
        timeout : TimeoutPolicy, optional
            Timeout policy for this step.
        condition : Condition, optional
            Conditional gate for this step.
        metadata : dict, optional
            Arbitrary metadata.

        Returns
        -------
        WorkflowBuilder
            ``self`` for chaining.
        """
        self._steps.append(
            {
                "id": step_id,
                "func": func,
                "name": name,
                "depends_on": depends_on or [],
                "retry": retry,
                "timeout": timeout,
                "condition": condition,
                "metadata": metadata,
            }
        )
        return self

    # ------------------------------------------------------------------
    # Engine configuration
    # ------------------------------------------------------------------

    def max_workers(self, n: int) -> "WorkflowBuilder":
        """Set the maximum number of parallel workers."""
        self._max_workers = n
        return self

    def fail_fast(self, enabled: bool = True) -> "WorkflowBuilder":
        """Enable or disable fail-fast behaviour."""
        self._fail_fast = enabled
        return self

    def event_bus(self, bus: EventBus) -> "WorkflowBuilder":
        """Use a custom event bus."""
        self._event_bus = bus
        return self

    def checkpoint_manager(self, mgr: CheckpointManager) -> "WorkflowBuilder":
        """Use a custom checkpoint manager."""
        self._checkpoint_manager = mgr
        return self

    # ------------------------------------------------------------------
    # Event shortcuts
    # ------------------------------------------------------------------

    def on_start(self, callback: Callable) -> "WorkflowBuilder":
        """Register a callback for ``WORKFLOW_STARTED``."""
        self._event_bus.on(EventType.WORKFLOW_STARTED, callback)
        return self

    def on_complete(self, callback: Callable) -> "WorkflowBuilder":
        """Register a callback for ``WORKFLOW_COMPLETED``."""
        self._event_bus.on(EventType.WORKFLOW_COMPLETED, callback)
        return self

    def on_failure(self, callback: Callable) -> "WorkflowBuilder":
        """Register a callback for ``WORKFLOW_FAILED``."""
        self._event_bus.on(EventType.WORKFLOW_FAILED, callback)
        return self

    def on_node_complete(self, callback: Callable) -> "WorkflowBuilder":
        """Register a callback for ``NODE_COMPLETED``."""
        self._event_bus.on(EventType.NODE_COMPLETED, callback)
        return self

    def on_node_failure(self, callback: Callable) -> "WorkflowBuilder":
        """Register a callback for ``NODE_FAILED``."""
        self._event_bus.on(EventType.NODE_FAILED, callback)
        return self

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def build(self) -> "Workflow":
        """
        Construct the DAG and engine from the accumulated configuration.

        Returns
        -------
        Workflow
            A ready-to-run workflow object.

        Raises
        ------
        WorkflowValidationError
            If the resulting DAG is invalid.
        """
        dag = DAG(name=self._name, description=self._description)

        # Add nodes
        for step in self._steps:
            node = Node(
                node_id=step["id"],
                func=step["func"],
                name=step["name"],
                retry_policy=step["retry"],
                timeout=step["timeout"],
                condition=step["condition"],
                metadata=step["metadata"],
            )
            dag.add_node(node)

        # Add edges
        for step in self._steps:
            for dep_id in step["depends_on"]:
                dag.add_edge(dep_id, step["id"])

        # Validate
        issues = dag.validate()
        if issues:
            raise WorkflowValidationError(issues)

        engine = WorkflowEngine(
            max_workers=self._max_workers,
            event_bus=self._event_bus,
            checkpoint_manager=self._checkpoint_manager,
            fail_fast=self._fail_fast,
        )

        return Workflow(dag=dag, engine=engine)


class Workflow:
    """
    A fully-configured, ready-to-run workflow.

    Returned by :meth:`WorkflowBuilder.build`.
    """

    def __init__(self, dag: DAG, engine: WorkflowEngine) -> None:
        self.dag = dag
        self.engine = engine

    def run(self, context=None):
        """Execute the workflow and return a :class:`WorkflowResult`."""
        return self.engine.run(self.dag, context)

    def pause(self, context=None):
        """Pause the workflow and save a checkpoint."""
        from flowforge.core.context import ExecutionContext
        ctx = context or ExecutionContext()
        return self.engine.pause(self.dag, ctx)

    def resume(self, checkpoint_id: str):
        """Resume the workflow from a checkpoint."""
        return self.engine.resume(self.dag, checkpoint_id)

    def cancel(self):
        """Cancel the running workflow."""
        self.engine.cancel()

    @property
    def name(self) -> str:
        return self.dag.name

    def __repr__(self) -> str:
        return f"Workflow({self.dag.name!r}, nodes={self.dag.node_count})"


# ------------------------------------------------------------------
# Decorator API
# ------------------------------------------------------------------

# Global registry for decorator-based step definitions
_step_registry: Dict[str, Dict[str, Any]] = {}


def workflow_step(
    step_id: str,
    *,
    name: Optional[str] = None,
    depends_on: Optional[List[str]] = None,
    retry: Optional["RetryPolicy"] = None,
    timeout: Optional["TimeoutPolicy"] = None,
    condition: Optional["Condition"] = None,
):
    """
    Decorator that registers a function as a workflow step.

    Usage::

        @workflow_step("extract", name="Extract Data")
        def extract(ctx):
            return fetch_records()

        @workflow_step("transform", depends_on=["extract"])
        def transform(ctx):
            data = ctx.get_node_result("extract")
            return process(data)

    Then build the workflow from the registry::

        wf = WorkflowBuilder.from_registry("my_pipeline")
        result = wf.run()
    """

    def decorator(func: Callable) -> Callable:
        _step_registry[step_id] = {
            "id": step_id,
            "func": func,
            "name": name or func.__name__,
            "depends_on": depends_on or [],
            "retry": retry,
            "timeout": timeout,
            "condition": condition,
        }

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        wrapper._flowforge_step_id = step_id
        return wrapper

    return decorator


def build_from_registry(
    workflow_name: str, description: str = ""
) -> Workflow:
    """
    Build a :class:`Workflow` from all ``@workflow_step`` decorated functions.

    Parameters
    ----------
    workflow_name : str
        Name for the workflow.
    description : str, optional
        Optional description.
    """
    builder = WorkflowBuilder(workflow_name, description)
    for step_info in _step_registry.values():
        builder.add_step(
            step_id=step_info["id"],
            func=step_info["func"],
            name=step_info["name"],
            depends_on=step_info["depends_on"],
            retry=step_info["retry"],
            timeout=step_info["timeout"],
            condition=step_info["condition"],
        )
    return builder.build()


def clear_registry() -> None:
    """Clear the global step registry (useful for testing)."""
    _step_registry.clear()
