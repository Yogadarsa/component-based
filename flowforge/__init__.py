"""
FlowForge — Python Workflow Pipeline Engine
=============================================

FlowForge lets you define multi-step workflows as Directed Acyclic Graphs
(DAGs), execute them with parallel scheduling, retry policies, conditional
branching, checkpointing, and a rich event system.

Quick Start
-----------
>>> from flowforge import WorkflowBuilder
>>>
>>> def extract(ctx):
...     return [1, 2, 3]
>>>
>>> def transform(ctx):
...     data = ctx.get_node_result("extract")
...     return [x * 2 for x in data]
>>>
>>> def load(ctx):
...     data = ctx.get_node_result("transform")
...     print(f"Loaded {len(data)} records")
>>>
>>> wf = (WorkflowBuilder("etl")
...     .add_step("extract", extract)
...     .add_step("transform", transform, depends_on=["extract"])
...     .add_step("load", load, depends_on=["transform"])
...     .build())
>>>
>>> result = wf.run()
>>> print(result.status)
"""

__version__ = "1.0.0"
__author__ = "FlowForge Contributors"

# Core
from flowforge.core.node import Node
from flowforge.core.dag import DAG
from flowforge.core.context import ExecutionContext
from flowforge.core.engine import WorkflowEngine, WorkflowResult

# Policies
from flowforge.policies.retry import (
    RetryPolicy,
    NoRetryPolicy,
    FixedRetryPolicy,
    ExponentialBackoffPolicy,
    LinearBackoffPolicy,
)
from flowforge.policies.timeout import TimeoutPolicy

# Branching
from flowforge.branching.conditions import (
    Condition,
    LambdaCondition,
    ResultCondition,
    AlwaysTrue,
    AlwaysFalse,
)

# Checkpoint
from flowforge.checkpoint.manager import CheckpointManager

# Events
from flowforge.enums import EventType, NodeStatus, WorkflowStatus, RetryStrategy
from flowforge.events.hooks import EventBus, Event

# Builders
from flowforge.builders.workflow import (
    WorkflowBuilder,
    Workflow,
    workflow_step,
    build_from_registry,
    clear_registry,
)

# Exceptions
from flowforge.exceptions import (
    FlowForgeError,
    CyclicDependencyError,
    DuplicateNodeError,
    NodeExecutionError,
    WorkflowTimeoutError,
    CheckpointError,
    WorkflowValidationError,
    InvalidNodeError,
)

__all__ = [
    # Version
    "__version__",
    # Core
    "Node",
    "DAG",
    "ExecutionContext",
    "WorkflowEngine",
    "WorkflowResult",
    # Policies
    "RetryPolicy",
    "NoRetryPolicy",
    "FixedRetryPolicy",
    "ExponentialBackoffPolicy",
    "LinearBackoffPolicy",
    "TimeoutPolicy",
    # Branching
    "Condition",
    "LambdaCondition",
    "ResultCondition",
    "AlwaysTrue",
    "AlwaysFalse",
    # Checkpoint
    "CheckpointManager",
    # Events
    "EventType",
    "NodeStatus",
    "WorkflowStatus",
    "RetryStrategy",
    "EventBus",
    "Event",
    # Builders
    "WorkflowBuilder",
    "Workflow",
    "workflow_step",
    "build_from_registry",
    "clear_registry",
    # Exceptions
    "FlowForgeError",
    "CyclicDependencyError",
    "DuplicateNodeError",
    "NodeExecutionError",
    "WorkflowTimeoutError",
    "CheckpointError",
    "WorkflowValidationError",
    "InvalidNodeError",
]
