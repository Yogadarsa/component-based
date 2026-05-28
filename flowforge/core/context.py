"""
FlowForge Execution Context
============================
Thread-safe shared state container that flows through every node in a workflow.

The context serves three purposes:
1. **Shared data store** — nodes can publish and consume key-value pairs.
2. **Result bus** — every node's return value is automatically stored and
   accessible to downstream nodes.
3. **Checkpoint source** — the context can produce an immutable snapshot
   for checkpointing and later restoration.
"""

from __future__ import annotations

import copy
import threading
from datetime import datetime
from typing import Any, Dict, Optional


class ExecutionContext:
    """
    Thread-safe data container shared across all nodes in a workflow.

    Parameters
    ----------
    initial_data : dict, optional
        Key-value pairs available to every node from the start.
    workflow_id : str, optional
        Unique identifier for the workflow run that owns this context.

    Examples
    --------
    >>> ctx = ExecutionContext({"env": "production"})
    >>> ctx.set("batch_size", 100)
    >>> ctx.get("batch_size")
    100
    """

    def __init__(
        self,
        initial_data: Optional[Dict[str, Any]] = None,
        workflow_id: Optional[str] = None,
    ) -> None:
        self._lock = threading.Lock()
        self._data: Dict[str, Any] = dict(initial_data) if initial_data else {}
        self._node_results: Dict[str, Any] = {}
        self.workflow_id = workflow_id
        self.created_at = datetime.now()

    # ------------------------------------------------------------------
    # Public data access
    # ------------------------------------------------------------------

    def set(self, key: str, value: Any) -> None:
        """Store a value in the shared context (thread-safe)."""
        with self._lock:
            self._data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve a value from the shared context (thread-safe)."""
        with self._lock:
            return self._data.get(key, default)

    def has(self, key: str) -> bool:
        """Check whether a key exists in the shared context."""
        with self._lock:
            return key in self._data

    def remove(self, key: str) -> None:
        """Remove a key from the shared context."""
        with self._lock:
            self._data.pop(key, None)

    def keys(self):
        """Return all keys currently in the context."""
        with self._lock:
            return list(self._data.keys())

    # ------------------------------------------------------------------
    # Node result access
    # ------------------------------------------------------------------

    def set_node_result(self, node_id: str, result: Any) -> None:
        """Store the return value of a completed node (called by the engine)."""
        with self._lock:
            self._node_results[node_id] = result

    def get_node_result(self, node_id: str, default: Any = None) -> Any:
        """
        Retrieve the return value of a previously completed node.

        Parameters
        ----------
        node_id : str
            The ID of the node whose result is requested.
        default : Any
            Value returned if the node hasn't produced a result yet.
        """
        with self._lock:
            return self._node_results.get(node_id, default)

    def has_node_result(self, node_id: str) -> bool:
        """Check whether a node has stored a result."""
        with self._lock:
            return node_id in self._node_results

    @property
    def all_node_results(self) -> Dict[str, Any]:
        """Return a shallow copy of all node results."""
        with self._lock:
            return dict(self._node_results)

    # ------------------------------------------------------------------
    # Snapshot / checkpoint support
    # ------------------------------------------------------------------

    def snapshot(self) -> Dict[str, Any]:
        """
        Return a deep-copy snapshot of the entire context state.

        The snapshot is safe to store, serialise, or pass across threads.
        """
        with self._lock:
            return {
                "data": copy.deepcopy(self._data),
                "node_results": copy.deepcopy(self._node_results),
                "workflow_id": self.workflow_id,
                "created_at": self.created_at.isoformat(),
            }

    @classmethod
    def from_snapshot(cls, snapshot: Dict[str, Any]) -> "ExecutionContext":
        """
        Restore an ``ExecutionContext`` from a previously taken snapshot.

        Parameters
        ----------
        snapshot : dict
            The dictionary produced by :meth:`snapshot`.
        """
        ctx = cls(
            initial_data=snapshot.get("data", {}),
            workflow_id=snapshot.get("workflow_id"),
        )
        ctx._node_results = snapshot.get("node_results", {})
        if "created_at" in snapshot:
            ctx.created_at = datetime.fromisoformat(snapshot["created_at"])
        return ctx

    # ------------------------------------------------------------------
    # Merge
    # ------------------------------------------------------------------

    def merge(self, other: "ExecutionContext") -> None:
        """Merge another context's data and results into this one."""
        with self._lock:
            self._data.update(other._data)
            self._node_results.update(other._node_results)

    # ------------------------------------------------------------------
    # Dunder methods
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        with self._lock:
            return (
                f"ExecutionContext(keys={list(self._data.keys())}, "
                f"results={list(self._node_results.keys())})"
            )

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __contains__(self, key: str) -> bool:
        return self.has(key)
