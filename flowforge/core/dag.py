"""
FlowForge DAG
=============
Directed Acyclic Graph container that holds workflow nodes and their
dependency edges.

Provides:
- Node and edge management
- Cycle detection (DFS-based)
- Topological sorting (Kahn's algorithm)
- Ready-node discovery for the execution engine
"""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Dict, List, Optional, Set

from flowforge.core.node import Node
from flowforge.enums import NodeStatus
from flowforge.exceptions import (
    CyclicDependencyError,
    DuplicateNodeError,
    InvalidNodeError,
    WorkflowValidationError,
)


class DAG:
    """
    Directed Acyclic Graph of workflow :class:`Node` instances.

    The DAG is the structural backbone of a workflow: it knows **what** the
    steps are and **how** they depend on one another, but it does not
    execute anything — that responsibility belongs to
    :class:`~flowforge.core.engine.WorkflowEngine`.

    Parameters
    ----------
    name : str
        Human-readable name for this workflow graph.
    description : str, optional
        Optional longer description.

    Examples
    --------
    >>> dag = DAG("etl")
    >>> dag.add_node(Node("extract", extract_fn))
    >>> dag.add_node(Node("transform", transform_fn))
    >>> dag.add_edge("extract", "transform")
    >>> dag.topological_sort()
    ['extract', 'transform']
    """

    def __init__(self, name: str, description: str = "") -> None:
        self.name = name
        self.description = description
        self._nodes: Dict[str, Node] = {}
        self._adjacency: Dict[str, List[str]] = defaultdict(list)  # parent → [children]
        self._reverse: Dict[str, List[str]] = defaultdict(list)    # child  → [parents]

    # ------------------------------------------------------------------
    # Node management
    # ------------------------------------------------------------------

    def add_node(self, node: Node) -> "DAG":
        """
        Add a node to the graph.

        Returns ``self`` to allow chaining.

        Raises
        ------
        DuplicateNodeError
            If a node with the same ``node_id`` already exists.
        """
        if node.node_id in self._nodes:
            raise DuplicateNodeError(node.node_id)
        self._nodes[node.node_id] = node
        # Ensure adjacency entries exist even for isolated nodes
        if node.node_id not in self._adjacency:
            self._adjacency[node.node_id] = []
        return self

    def get_node(self, node_id: str) -> Node:
        """
        Return the node with the given ID.

        Raises
        ------
        InvalidNodeError
            If the node does not exist.
        """
        if node_id not in self._nodes:
            raise InvalidNodeError(node_id)
        return self._nodes[node_id]

    def has_node(self, node_id: str) -> bool:
        """Check whether a node exists in the DAG."""
        return node_id in self._nodes

    def remove_node(self, node_id: str) -> None:
        """
        Remove a node and all its incident edges.

        Raises
        ------
        InvalidNodeError
            If the node does not exist.
        """
        if node_id not in self._nodes:
            raise InvalidNodeError(node_id)

        # Remove from adjacency (outgoing edges)
        for child_id in self._adjacency.get(node_id, []):
            self._reverse[child_id].remove(node_id)
            self._nodes[child_id]._dependencies.remove(node_id)
        del self._adjacency[node_id]

        # Remove from reverse (incoming edges)
        for parent_id in self._reverse.get(node_id, []):
            self._adjacency[parent_id].remove(node_id)
            self._nodes[parent_id]._dependents.remove(node_id)
        del self._reverse[node_id]

        del self._nodes[node_id]

    @property
    def nodes(self) -> Dict[str, Node]:
        """Return a read-only view of all nodes."""
        return dict(self._nodes)

    @property
    def node_ids(self) -> List[str]:
        """Return all node IDs in insertion order."""
        return list(self._nodes.keys())

    @property
    def node_count(self) -> int:
        """Return the number of nodes in the DAG."""
        return len(self._nodes)

    # ------------------------------------------------------------------
    # Edge management
    # ------------------------------------------------------------------

    def add_edge(self, from_id: str, to_id: str) -> "DAG":
        """
        Add a dependency edge: ``to_id`` depends on ``from_id``.

        Returns ``self`` for chaining.

        Raises
        ------
        InvalidNodeError
            If either node does not exist.
        CyclicDependencyError
            If the new edge would introduce a cycle.
        """
        if from_id not in self._nodes:
            raise InvalidNodeError(from_id)
        if to_id not in self._nodes:
            raise InvalidNodeError(to_id)

        # Avoid duplicate edges
        if to_id in self._adjacency[from_id]:
            return self

        # Tentatively add edge, then check for cycles
        self._adjacency[from_id].append(to_id)
        self._reverse[to_id].append(from_id)

        if self._has_cycle():
            # Roll back
            self._adjacency[from_id].remove(to_id)
            self._reverse[to_id].remove(from_id)
            raise CyclicDependencyError([from_id, to_id, from_id])

        # Book-keep on nodes themselves
        self._nodes[to_id].add_dependency(from_id)
        self._nodes[from_id].add_dependent(to_id)
        return self

    def get_dependencies(self, node_id: str) -> List[str]:
        """Return the IDs of all direct parents of ``node_id``."""
        if node_id not in self._nodes:
            raise InvalidNodeError(node_id)
        return list(self._reverse.get(node_id, []))

    def get_dependents(self, node_id: str) -> List[str]:
        """Return the IDs of all direct children of ``node_id``."""
        if node_id not in self._nodes:
            raise InvalidNodeError(node_id)
        return list(self._adjacency.get(node_id, []))

    @property
    def edge_count(self) -> int:
        """Return the total number of edges."""
        return sum(len(children) for children in self._adjacency.values())

    # ------------------------------------------------------------------
    # Graph algorithms
    # ------------------------------------------------------------------

    def topological_sort(self) -> List[str]:
        """
        Return node IDs in a valid topological order (Kahn's algorithm).

        Raises
        ------
        CyclicDependencyError
            If the graph contains a cycle.
        """
        in_degree: Dict[str, int] = {nid: 0 for nid in self._nodes}
        for nid in self._nodes:
            for child in self._adjacency.get(nid, []):
                in_degree[child] += 1

        queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
        result: List[str] = []

        while queue:
            node_id = queue.popleft()
            result.append(node_id)
            for child in self._adjacency.get(node_id, []):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        if len(result) != len(self._nodes):
            raise CyclicDependencyError()

        return result

    def get_ready_nodes(self) -> List[Node]:
        """
        Return nodes whose dependencies are all in a terminal state
        and that are themselves ``PENDING``.

        The engine calls this repeatedly to discover which nodes can
        be dispatched next.
        """
        ready = []
        for node in self._nodes.values():
            if node.status != NodeStatus.PENDING:
                continue
            parents = self._reverse.get(node.node_id, [])
            if all(self._nodes[p].is_terminal for p in parents):
                # Check that all *required* parents completed (not failed)
                # Skipped parents are considered terminal and acceptable
                all_parents_ok = all(
                    self._nodes[p].status
                    in (NodeStatus.COMPLETED, NodeStatus.SKIPPED)
                    for p in parents
                )
                if all_parents_ok:
                    ready.append(node)
        return ready

    def get_root_nodes(self) -> List[Node]:
        """Return nodes with no dependencies (entry points)."""
        return [
            node
            for node in self._nodes.values()
            if not self._reverse.get(node.node_id)
        ]

    def get_leaf_nodes(self) -> List[Node]:
        """Return nodes with no dependents (exit points)."""
        return [
            node
            for node in self._nodes.values()
            if not self._adjacency.get(node.node_id)
        ]

    # ------------------------------------------------------------------
    # Cycle detection (private)
    # ------------------------------------------------------------------

    def _has_cycle(self) -> bool:
        """DFS-based cycle detection. Returns ``True`` if a cycle exists."""
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[str, int] = {nid: WHITE for nid in self._nodes}

        def dfs(nid: str) -> bool:
            color[nid] = GRAY
            for child in self._adjacency.get(nid, []):
                if color[child] == GRAY:
                    return True
                if color[child] == WHITE and dfs(child):
                    return True
            color[nid] = BLACK
            return False

        return any(
            dfs(nid) for nid, c in color.items() if c == WHITE
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> List[str]:
        """
        Perform comprehensive validation. Returns a list of issues
        (empty list means the DAG is valid).

        Checks performed:
        - Graph is non-empty
        - No cycles
        - All edge targets exist
        - At least one root node exists
        """
        issues: List[str] = []

        if not self._nodes:
            issues.append("DAG has no nodes")
            return issues

        # Cycle check
        if self._has_cycle():
            issues.append("DAG contains a cycle")

        # Orphaned edge references
        for parent_id, children in self._adjacency.items():
            for child_id in children:
                if child_id not in self._nodes:
                    issues.append(
                        f"Edge references non-existent node '{child_id}'"
                    )

        # Root existence
        if not self.get_root_nodes():
            issues.append("DAG has no root nodes (every node has a dependency)")

        return issues

    # ------------------------------------------------------------------
    # Reset
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """Reset all nodes to ``PENDING`` status for re-execution."""
        for node in self._nodes.values():
            node.reset()

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict:
        """Serialise the DAG's structure and node states."""
        return {
            "name": self.name,
            "description": self.description,
            "nodes": {nid: n.to_dict() for nid, n in self._nodes.items()},
            "edges": {
                parent: list(children)
                for parent, children in self._adjacency.items()
            },
        }

    # ------------------------------------------------------------------
    # Dunder methods
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"DAG(name={self.name!r}, nodes={self.node_count}, "
            f"edges={self.edge_count})"
        )

    def __len__(self) -> int:
        return self.node_count

    def __contains__(self, node_id: str) -> bool:
        return self.has_node(node_id)
