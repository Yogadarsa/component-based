"""
FlowForge Timeout Policy
=========================
Enforces a maximum execution duration on individual nodes.
"""

from __future__ import annotations

import concurrent.futures
import functools
from typing import Any, Callable, Optional

from flowforge.exceptions import WorkflowTimeoutError


class TimeoutPolicy:
    """
    Wraps node execution with a timeout.

    If the callable does not complete within ``timeout_seconds``, a
    :class:`~flowforge.exceptions.WorkflowTimeoutError` is raised.

    Parameters
    ----------
    timeout_seconds : float
        Maximum allowed execution time in seconds.

    Examples
    --------
    >>> policy = TimeoutPolicy(5.0)
    >>> result = policy.execute(my_slow_func, ctx, node_id="slow_step")
    """

    def __init__(self, timeout_seconds: float) -> None:
        self.timeout_seconds = max(0.0, timeout_seconds)

    def execute(
        self,
        func: Callable,
        *args: Any,
        node_id: Optional[str] = None,
        **kwargs: Any,
    ) -> Any:
        """
        Execute ``func`` with a timeout guard.

        Parameters
        ----------
        func : callable
            The function to execute.
        *args, **kwargs
            Passed through to ``func``.
        node_id : str, optional
            Used for error messages.

        Returns
        -------
        Any
            The return value of ``func``.

        Raises
        ------
        WorkflowTimeoutError
            If execution exceeds ``timeout_seconds``.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(func, *args, **kwargs)
            try:
                return future.result(timeout=self.timeout_seconds)
            except concurrent.futures.TimeoutError:
                future.cancel()
                raise WorkflowTimeoutError(
                    node_id=node_id,
                    timeout_seconds=self.timeout_seconds,
                )

    def __repr__(self) -> str:
        return f"TimeoutPolicy(timeout_seconds={self.timeout_seconds})"
