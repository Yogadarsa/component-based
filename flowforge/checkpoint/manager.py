"""
FlowForge Checkpoint Manager
=============================
In-memory checkpoint system for saving and restoring workflow state.

Checkpoints capture the full state of a running workflow — node statuses,
execution context, and DAG structure — so that execution can be paused
and later resumed from exactly where it left off.
"""

from __future__ import annotations

import copy
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

from flowforge.exceptions import CheckpointError

if TYPE_CHECKING:
    from flowforge.core.dag import DAG
    from flowforge.core.context import ExecutionContext


class Checkpoint:
    """
    An immutable snapshot of a workflow's state at a point in time.

    Attributes
    ----------
    checkpoint_id : str
        Unique identifier.
    workflow_name : str
        Name of the workflow that produced this checkpoint.
    dag_state : dict
        Serialised DAG state (node statuses, results, edges).
    context_snapshot : dict
        Serialised execution context.
    created_at : datetime
        When this checkpoint was created.
    metadata : dict
        Optional user-supplied metadata.
    """

    __slots__ = (
        "checkpoint_id",
        "workflow_name",
        "dag_state",
        "context_snapshot",
        "created_at",
        "metadata",
    )

    def __init__(
        self,
        workflow_name: str,
        dag_state: dict,
        context_snapshot: dict,
        metadata: Optional[dict] = None,
    ) -> None:
        self.checkpoint_id = str(uuid.uuid4())
        self.workflow_name = workflow_name
        self.dag_state = dag_state
        self.context_snapshot = context_snapshot
        self.created_at = datetime.now()
        self.metadata = metadata or {}

    def to_dict(self) -> dict:
        """Serialise the checkpoint for storage or inspection."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "workflow_name": self.workflow_name,
            "dag_state": self.dag_state,
            "context_snapshot": self.context_snapshot,
            "created_at": self.created_at.isoformat(),
            "metadata": self.metadata,
        }

    def __repr__(self) -> str:
        return (
            f"Checkpoint(id={self.checkpoint_id[:8]}..., "
            f"workflow={self.workflow_name!r}, "
            f"created_at={self.created_at.isoformat()})"
        )


class CheckpointManager:
    """
    Manages creation, storage, and restoration of workflow checkpoints.

    Currently stores checkpoints in memory. The architecture is designed
    so that persistent backends (file, database, Redis) can be added
    by subclassing or swapping the storage strategy.

    Examples
    --------
    >>> manager = CheckpointManager()
    >>> cp_id = manager.save(dag, context, metadata={"reason": "pause"})
    >>> dag_state, ctx_snapshot = manager.restore(cp_id)
    """

    def __init__(self) -> None:
        self._checkpoints: Dict[str, Checkpoint] = {}

    def save(
        self,
        dag: "DAG",
        context: "ExecutionContext",
        metadata: Optional[dict] = None,
    ) -> str:
        """
        Save a checkpoint of the current workflow state.

        Parameters
        ----------
        dag : DAG
            The workflow DAG (node statuses and structure are serialised).
        context : ExecutionContext
            The execution context (data and node results are serialised).
        metadata : dict, optional
            Arbitrary metadata to attach to the checkpoint.

        Returns
        -------
        str
            The unique checkpoint ID.

        Raises
        ------
        CheckpointError
            If serialisation fails.
        """
        try:
            dag_state = copy.deepcopy(dag.to_dict())
            context_snapshot = copy.deepcopy(context.snapshot())
        except Exception as exc:
            raise CheckpointError(
                operation="save",
                detail=f"Failed to serialise state: {exc}",
            ) from exc

        checkpoint = Checkpoint(
            workflow_name=dag.name,
            dag_state=dag_state,
            context_snapshot=context_snapshot,
            metadata=metadata,
        )
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint
        return checkpoint.checkpoint_id

    def restore(self, checkpoint_id: str) -> Tuple[dict, dict]:
        """
        Restore a previously saved checkpoint.

        Parameters
        ----------
        checkpoint_id : str
            The ID returned by :meth:`save`.

        Returns
        -------
        tuple[dict, dict]
            ``(dag_state, context_snapshot)`` — deep copies so the
            checkpoint itself remains unchanged.

        Raises
        ------
        CheckpointError
            If the checkpoint ID is not found.
        """
        if checkpoint_id not in self._checkpoints:
            raise CheckpointError(
                operation="restore",
                detail=f"Checkpoint '{checkpoint_id}' not found",
            )

        cp = self._checkpoints[checkpoint_id]
        return copy.deepcopy(cp.dag_state), copy.deepcopy(cp.context_snapshot)

    def get(self, checkpoint_id: str) -> Checkpoint:
        """
        Retrieve a checkpoint object by ID.

        Raises
        ------
        CheckpointError
            If not found.
        """
        if checkpoint_id not in self._checkpoints:
            raise CheckpointError(
                operation="get",
                detail=f"Checkpoint '{checkpoint_id}' not found",
            )
        return self._checkpoints[checkpoint_id]

    def list_checkpoints(
        self, workflow_name: Optional[str] = None
    ) -> List[Checkpoint]:
        """
        List all checkpoints, optionally filtered by workflow name.

        Returns them sorted by creation time (newest first).
        """
        checkpoints = list(self._checkpoints.values())
        if workflow_name:
            checkpoints = [
                cp for cp in checkpoints if cp.workflow_name == workflow_name
            ]
        return sorted(checkpoints, key=lambda cp: cp.created_at, reverse=True)

    def delete(self, checkpoint_id: str) -> bool:
        """
        Delete a checkpoint. Returns ``True`` if it existed.
        """
        if checkpoint_id in self._checkpoints:
            del self._checkpoints[checkpoint_id]
            return True
        return False

    def clear(self, workflow_name: Optional[str] = None) -> int:
        """
        Delete all checkpoints, optionally filtered by workflow name.

        Returns the number of deleted checkpoints.
        """
        if workflow_name is None:
            count = len(self._checkpoints)
            self._checkpoints.clear()
            return count

        to_delete = [
            cp_id
            for cp_id, cp in self._checkpoints.items()
            if cp.workflow_name == workflow_name
        ]
        for cp_id in to_delete:
            del self._checkpoints[cp_id]
        return len(to_delete)

    def __len__(self) -> int:
        return len(self._checkpoints)

    def __repr__(self) -> str:
        return f"CheckpointManager(checkpoints={len(self._checkpoints)})"
