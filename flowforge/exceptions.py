"""
FlowForge Exceptions
====================
Hierarchical exception classes for precise error handling throughout the engine.
"""


class FlowForgeError(Exception):
    """Base exception for all FlowForge errors."""

    def __init__(self, message: str = "", *args, **kwargs):
        self.message = message
        super().__init__(message, *args, **kwargs)


class CyclicDependencyError(FlowForgeError):
    """Raised when a cycle is detected in the workflow DAG."""

    def __init__(self, cycle_path: list = None):
        self.cycle_path = cycle_path or []
        path_str = " -> ".join(self.cycle_path) if self.cycle_path else "unknown"
        super().__init__(f"Cyclic dependency detected: {path_str}")


class DuplicateNodeError(FlowForgeError):
    """Raised when a node with the same ID already exists in the DAG."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        super().__init__(f"Node with ID '{node_id}' already exists in the workflow")


class NodeExecutionError(FlowForgeError):
    """Raised when a node's callable raises an exception during execution."""

    def __init__(self, node_id: str, original_error: Exception):
        self.node_id = node_id
        self.original_error = original_error
        super().__init__(
            f"Node '{node_id}' failed with {type(original_error).__name__}: "
            f"{original_error}"
        )


class WorkflowTimeoutError(FlowForgeError):
    """Raised when a workflow or node exceeds its configured timeout."""

    def __init__(self, node_id: str = None, timeout_seconds: float = 0):
        self.node_id = node_id
        self.timeout_seconds = timeout_seconds
        target = f"Node '{node_id}'" if node_id else "Workflow"
        super().__init__(
            f"{target} exceeded timeout of {timeout_seconds:.1f} seconds"
        )


class CheckpointError(FlowForgeError):
    """Raised when a checkpoint save or restore operation fails."""

    def __init__(self, operation: str, detail: str = ""):
        self.operation = operation
        self.detail = detail
        super().__init__(f"Checkpoint {operation} failed: {detail}")


class WorkflowValidationError(FlowForgeError):
    """Raised when workflow validation fails (e.g., no nodes, invalid edges)."""

    def __init__(self, issues: list = None):
        self.issues = issues or []
        issues_str = "; ".join(self.issues)
        super().__init__(f"Workflow validation failed: {issues_str}")


class InvalidNodeError(FlowForgeError):
    """Raised when a referenced node does not exist in the DAG."""

    def __init__(self, node_id: str):
        self.node_id = node_id
        super().__init__(f"Node '{node_id}' does not exist in the workflow")
