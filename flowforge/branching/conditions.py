"""
FlowForge Conditions
====================
Conditional branching logic that gates whether a node should execute
or be skipped.

A ``Condition`` is evaluated at run-time: if it returns ``True`` the
node proceeds; if ``False`` the node is marked as ``SKIPPED`` and its
dependents may also be skipped (depending on their own conditions).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    from flowforge.core.context import ExecutionContext


class Condition(ABC):
    """
    Abstract base for all conditions.

    Subclasses must implement :meth:`evaluate`, which receives the
    current :class:`ExecutionContext` and returns a boolean.
    """

    @abstractmethod
    def evaluate(self, context: "ExecutionContext") -> bool:
        """Return ``True`` if the guarded node should execute."""
        ...

    def __and__(self, other: "Condition") -> "AndCondition":
        return AndCondition(self, other)

    def __or__(self, other: "Condition") -> "OrCondition":
        return OrCondition(self, other)

    def __invert__(self) -> "NotCondition":
        return NotCondition(self)


class LambdaCondition(Condition):
    """
    Wraps an arbitrary callable as a condition.

    Parameters
    ----------
    predicate : callable
        A function that accepts an ``ExecutionContext`` and returns ``bool``.
    description : str, optional
        Human-readable description used in ``repr``.

    Examples
    --------
    >>> cond = LambdaCondition(lambda ctx: ctx.get("env") == "production")
    """

    def __init__(
        self,
        predicate: Callable[["ExecutionContext"], bool],
        description: str = "custom",
    ) -> None:
        self._predicate = predicate
        self.description = description

    def evaluate(self, context: "ExecutionContext") -> bool:
        return bool(self._predicate(context))

    def __repr__(self) -> str:
        return f"LambdaCondition({self.description!r})"


class ResultCondition(Condition):
    """
    Branches based on a previous node's result.

    Parameters
    ----------
    source_node_id : str
        The node whose result to inspect.
    expected_value : Any
        The value to compare against. If the result equals this value
        the condition is ``True``.
    operator : str
        Comparison operator: ``'eq'``, ``'neq'``, ``'gt'``, ``'lt'``,
        ``'gte'``, ``'lte'``, ``'contains'``, ``'in'``.

    Examples
    --------
    >>> cond = ResultCondition("validate", expected_value=True)
    """

    _OPERATORS = {
        "eq": lambda a, b: a == b,
        "neq": lambda a, b: a != b,
        "gt": lambda a, b: a > b,
        "lt": lambda a, b: a < b,
        "gte": lambda a, b: a >= b,
        "lte": lambda a, b: a <= b,
        "contains": lambda a, b: b in a,
        "in": lambda a, b: a in b,
    }

    def __init__(
        self,
        source_node_id: str,
        expected_value: Any,
        operator: str = "eq",
    ) -> None:
        if operator not in self._OPERATORS:
            raise ValueError(
                f"Unknown operator '{operator}'. "
                f"Choose from: {list(self._OPERATORS.keys())}"
            )
        self.source_node_id = source_node_id
        self.expected_value = expected_value
        self.operator = operator
        self._op_func = self._OPERATORS[operator]

    def evaluate(self, context: "ExecutionContext") -> bool:
        result = context.get_node_result(self.source_node_id)
        if result is None:
            return False
        try:
            return bool(self._op_func(result, self.expected_value))
        except (TypeError, ValueError):
            return False

    def __repr__(self) -> str:
        return (
            f"ResultCondition(source={self.source_node_id!r}, "
            f"op={self.operator!r}, expected={self.expected_value!r})"
        )


class AlwaysTrue(Condition):
    """A condition that always evaluates to ``True``."""

    def evaluate(self, context: "ExecutionContext") -> bool:
        return True

    def __repr__(self) -> str:
        return "AlwaysTrue()"


class AlwaysFalse(Condition):
    """A condition that always evaluates to ``False``."""

    def evaluate(self, context: "ExecutionContext") -> bool:
        return False

    def __repr__(self) -> str:
        return "AlwaysFalse()"


# ------------------------------------------------------------------
# Composite conditions
# ------------------------------------------------------------------


class AndCondition(Condition):
    """Logical AND of two conditions."""

    def __init__(self, left: Condition, right: Condition) -> None:
        self.left = left
        self.right = right

    def evaluate(self, context: "ExecutionContext") -> bool:
        return self.left.evaluate(context) and self.right.evaluate(context)

    def __repr__(self) -> str:
        return f"({self.left!r} AND {self.right!r})"


class OrCondition(Condition):
    """Logical OR of two conditions."""

    def __init__(self, left: Condition, right: Condition) -> None:
        self.left = left
        self.right = right

    def evaluate(self, context: "ExecutionContext") -> bool:
        return self.left.evaluate(context) or self.right.evaluate(context)

    def __repr__(self) -> str:
        return f"({self.left!r} OR {self.right!r})"


class NotCondition(Condition):
    """Logical NOT of a condition."""

    def __init__(self, inner: Condition) -> None:
        self.inner = inner

    def evaluate(self, context: "ExecutionContext") -> bool:
        return not self.inner.evaluate(context)

    def __repr__(self) -> str:
        return f"(NOT {self.inner!r})"
