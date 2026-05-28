"""
FlowForge Enumerations
======================
Defines all status, state, and strategy enumerations used throughout the engine.
"""

from enum import Enum, auto


class NodeStatus(Enum):
    """Lifecycle status of a single workflow node (step)."""

    PENDING = auto()     # Node created but not yet evaluated
    READY = auto()       # All dependencies satisfied, eligible to run
    RUNNING = auto()     # Currently executing
    COMPLETED = auto()   # Finished successfully
    FAILED = auto()      # Execution raised an unrecoverable error
    RETRYING = auto()    # Failed but retrying per retry policy
    SKIPPED = auto()     # Skipped due to conditional branching


class WorkflowStatus(Enum):
    """Lifecycle status of an entire workflow execution."""

    CREATED = auto()     # Workflow defined but not started
    RUNNING = auto()     # Currently executing nodes
    COMPLETED = auto()   # All nodes finished successfully (or skipped)
    FAILED = auto()      # One or more nodes failed fatally
    PAUSED = auto()      # Execution paused (checkpoint saved)
    CANCELLED = auto()   # Execution cancelled by user


class RetryStrategy(Enum):
    """Strategy used to calculate delay between retry attempts."""

    FIXED_DELAY = auto()          # Constant delay between retries
    EXPONENTIAL_BACKOFF = auto()  # Delay doubles each attempt
    LINEAR_BACKOFF = auto()       # Delay increases linearly each attempt


class EventType(Enum):
    """Events emitted during workflow execution."""

    WORKFLOW_STARTED = auto()
    WORKFLOW_COMPLETED = auto()
    WORKFLOW_FAILED = auto()
    WORKFLOW_PAUSED = auto()
    WORKFLOW_RESUMED = auto()
    WORKFLOW_CANCELLED = auto()

    NODE_STARTED = auto()
    NODE_COMPLETED = auto()
    NODE_FAILED = auto()
    NODE_RETRYING = auto()
    NODE_SKIPPED = auto()

    CHECKPOINT_SAVED = auto()
    CHECKPOINT_RESTORED = auto()
