"""
FlowForge Branching
===================
Conditional routing logic for workflow DAG execution paths.
"""

from flowforge.branching.conditions import (
    Condition,
    LambdaCondition,
    ResultCondition,
    AlwaysTrue,
    AlwaysFalse,
)

__all__ = [
    "Condition",
    "LambdaCondition",
    "ResultCondition",
    "AlwaysTrue",
    "AlwaysFalse",
]
