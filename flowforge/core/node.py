"""
FlowForge Node
===============
Represents a single executable step within a workflow DAG.

Each node wraps a callable (the user's business logic) along with metadata
such as retry policies, timeout, conditional gates, and execution state.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, List, Optional, TYPE_CHECKING

from flowforge.enums import NodeStatus

if TYPE_CHECKING:
    from flowforge.policies.retry import RetryPolicy
    from flowforge.policies.timeout import TimeoutPolicy
    from flowforge.branching.conditions import Condition


@dataclass
class NodeMetrics:
    """Timing and execution metrics collected during a node's run."""

    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    retry_count: int = 0
    duration_seconds: float = 0.0

    def mark_started(self) -> None:
        """Record the start time of node execution."""
        self.started_at = datetime.now()

    def mark_completed(self) -> None:
        """Record the completion time and compute duration."""
        self.completed_at = datetime.now()
        if self.started_at:
            self.duration_seconds = (
                self.completed_at - self.started_at
            ).total_seconds()

    def increment_retry(self) -> None:
        """Increment the retry counter."""
        self.retry_count += 1


class Node:
    """
    A single step in a workflow pipeline.

    A Node wraps a user-supplied callable and enriches it with lifecycle
    management, retry policies, timeout enforcement, conditional gating,
    and execution metrics.

    Parameters
    ----------
    node_id : str
        Unique identifier for this node within a DAG.
    func : callable
        The business-logic function to execute. Must accept an
        ``ExecutionContext`` as its first argument.
    name : str, optional
        Human-readable name (defaults to ``node_id``).
    retry_policy : RetryPolicy, optional
        Retry behaviour on failure.
    timeout : TimeoutPolicy, optional
        Maximum execution duration before forced cancellation.
    condition : Condition, optional
        A gate that must evaluate to ``True`` for this node to run;
        otherwise the node is ``SKIPPED``.
    metadata : dict, optional
        Arbitrary user-supplied metadata.

    Examples
    --------
    >>> def extract(ctx):
    ...     return {"records": [1, 2, 3]}
    >>> node = Node("extract", extract, name="Extract Data")
    >>> node.status
    <NodeStatus.PENDING: 1>
    """

    __slots__ = (
        "node_id",
        "func",
        "name",
        "retry_policy",
        "timeout",
        "condition",
        "status",
        "result",
        "error",
        "metrics",
        "metadata",
        "_dependencies",
        "_dependents",
    )

    def __init__(
        self,
        node_id: str,
        func: Callable,
        *,
        name: Optional[str] = None,
        retry_policy: Optional["RetryPolicy"] = None,
        timeout: Optional["TimeoutPolicy"] = None,
        condition: Optional["Condition"] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        self.node_id = node_id
        self.func = func
        self.name = name or node_id
        self.retry_policy = retry_policy
        self.timeout = timeout
        self.condition = condition
        self.status = NodeStatus.PENDING
        self.result: Any = None
        self.error: Optional[Exception] = None
        self.metrics = NodeMetrics()
        self.metadata = metadata or {}
        self._dependencies: List[str] = []
        self._dependents: List[str] = []

    # ------------------------------------------------------------------
    # Dependency management
    # ------------------------------------------------------------------

    def add_dependency(self, node_id: str) -> None:
        """Register ``node_id`` as a prerequisite for this node."""
        if node_id not in self._dependencies:
            self._dependencies.append(node_id)

    def add_dependent(self, node_id: str) -> None:
        """Register ``node_id`` as a downstream consumer of this node."""
        if node_id not in self._dependents:
            self._dependents.append(node_id)

    @property
    def dependencies(self) -> List[str]:
        """Return a copy of the dependency list."""
        return list(self._dependencies)

    @property
    def dependents(self) -> List[str]:
        """Return a copy of the dependents list."""
        return list(self._dependents)

    # ------------------------------------------------------------------
    # Status helpers
    # ------------------------------------------------------------------

    @property
    def is_terminal(self) -> bool:
        """Return ``True`` if the node is in a terminal state."""
        return self.status in (
            NodeStatus.COMPLETED,
            NodeStatus.FAILED,
            NodeStatus.SKIPPED,
        )

    @property
    def is_runnable(self) -> bool:
        """Return ``True`` if the node is eligible to execute."""
        return self.status in (NodeStatus.READY, NodeStatus.RETRYING)

    def reset(self) -> None:
        """Reset the node to its initial ``PENDING`` state."""
        self.status = NodeStatus.PENDING
        self.result = None
        self.error = None
        self.metrics = NodeMetrics()

    # ------------------------------------------------------------------
    # Serialisation helpers (for checkpointing)
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialise this node's state (excluding the callable)."""
        return {
            "node_id": self.node_id,
            "name": self.name,
            "status": self.status.name,
            "result": self.result,
            "error": str(self.error) if self.error else None,
            "metrics": {
                "created_at": self.metrics.created_at.isoformat(),
                "started_at": (
                    self.metrics.started_at.isoformat()
                    if self.metrics.started_at
                    else None
                ),
                "completed_at": (
                    self.metrics.completed_at.isoformat()
                    if self.metrics.completed_at
                    else None
                ),
                "retry_count": self.metrics.retry_count,
                "duration_seconds": self.metrics.duration_seconds,
            },
            "dependencies": self._dependencies,
            "dependents": self._dependents,
            "metadata": self.metadata,
        }

    def restore_status(self, status_name: str, result: Any = None) -> None:
        """Restore a node's status from a serialised checkpoint."""
        self.status = NodeStatus[status_name]
        self.result = result

    # ------------------------------------------------------------------
    # Dunder methods
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"Node(id={self.node_id!r}, name={self.name!r}, "
            f"status={self.status.name})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Node):
            return NotImplemented
        return self.node_id == other.node_id

    def __hash__(self) -> int:
        return hash(self.node_id)
