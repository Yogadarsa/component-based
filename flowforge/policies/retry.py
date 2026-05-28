"""
FlowForge Retry Policies
=========================
Configurable retry strategies for resilient node execution.

Each policy decides:
- How many times to retry
- How long to wait between attempts
- Which exception types warrant a retry
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Optional, Tuple, Type

from flowforge.enums import RetryStrategy


class RetryPolicy(ABC):
    """
    Abstract base class for all retry policies.

    Parameters
    ----------
    max_retries : int
        Maximum number of retry attempts (0 = no retries).
    retry_on : tuple of Exception types, optional
        Only retry if the raised exception is an instance of one of these
        types. ``None`` means retry on any ``Exception``.
    """

    def __init__(
        self,
        max_retries: int = 3,
        retry_on: Optional[Tuple[Type[Exception], ...]] = None,
    ) -> None:
        self.max_retries = max(0, max_retries)
        self.retry_on = retry_on

    def should_retry(self, attempt: int, error: Exception) -> bool:
        """
        Determine whether a retry should be attempted.

        Parameters
        ----------
        attempt : int
            The current attempt number (1-based: first failure = attempt 1).
        error : Exception
            The exception that caused the failure.

        Returns
        -------
        bool
            ``True`` if the engine should retry the node.
        """
        if attempt >= self.max_retries:
            return False
        if self.retry_on is not None:
            return isinstance(error, self.retry_on)
        return True

    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """
        Return the delay in seconds before the next retry.

        Parameters
        ----------
        attempt : int
            The current attempt number (1-based).
        """
        ...

    @property
    @abstractmethod
    def strategy(self) -> RetryStrategy:
        """Return the :class:`RetryStrategy` enum value for this policy."""
        ...

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(max_retries={self.max_retries}, "
            f"strategy={self.strategy.name})"
        )


class NoRetryPolicy(RetryPolicy):
    """No retries — the node fails immediately on the first error."""

    def __init__(self) -> None:
        super().__init__(max_retries=0)

    def get_delay(self, attempt: int) -> float:
        return 0.0

    @property
    def strategy(self) -> RetryStrategy:
        return RetryStrategy.FIXED_DELAY

    def should_retry(self, attempt: int, error: Exception) -> bool:
        return False


class FixedRetryPolicy(RetryPolicy):
    """
    Retry with a constant delay between attempts.

    Parameters
    ----------
    max_retries : int
        Maximum number of retries.
    delay : float
        Constant delay in seconds between retries.
    retry_on : tuple, optional
        Exception types that warrant retrying.
    """

    def __init__(
        self,
        max_retries: int = 3,
        delay: float = 1.0,
        retry_on: Optional[Tuple[Type[Exception], ...]] = None,
    ) -> None:
        super().__init__(max_retries=max_retries, retry_on=retry_on)
        self.delay = max(0.0, delay)

    def get_delay(self, attempt: int) -> float:
        return self.delay

    @property
    def strategy(self) -> RetryStrategy:
        return RetryStrategy.FIXED_DELAY


class ExponentialBackoffPolicy(RetryPolicy):
    """
    Retry with exponentially increasing delay.

    Delay formula: ``min(base_delay * 2^(attempt-1), max_delay)``

    Parameters
    ----------
    max_retries : int
        Maximum number of retries.
    base_delay : float
        Initial delay in seconds.
    max_delay : float
        Upper-bound cap on the delay.
    retry_on : tuple, optional
        Exception types that warrant retrying.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        retry_on: Optional[Tuple[Type[Exception], ...]] = None,
    ) -> None:
        super().__init__(max_retries=max_retries, retry_on=retry_on)
        self.base_delay = max(0.0, base_delay)
        self.max_delay = max(0.0, max_delay)

    def get_delay(self, attempt: int) -> float:
        delay = self.base_delay * (2 ** (attempt - 1))
        return min(delay, self.max_delay)

    @property
    def strategy(self) -> RetryStrategy:
        return RetryStrategy.EXPONENTIAL_BACKOFF


class LinearBackoffPolicy(RetryPolicy):
    """
    Retry with linearly increasing delay.

    Delay formula: ``min(base_delay * attempt, max_delay)``

    Parameters
    ----------
    max_retries : int
        Maximum number of retries.
    base_delay : float
        Increment per attempt in seconds.
    max_delay : float
        Upper-bound cap on the delay.
    retry_on : tuple, optional
        Exception types that warrant retrying.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        retry_on: Optional[Tuple[Type[Exception], ...]] = None,
    ) -> None:
        super().__init__(max_retries=max_retries, retry_on=retry_on)
        self.base_delay = max(0.0, base_delay)
        self.max_delay = max(0.0, max_delay)

    def get_delay(self, attempt: int) -> float:
        delay = self.base_delay * attempt
        return min(delay, self.max_delay)

    @property
    def strategy(self) -> RetryStrategy:
        return RetryStrategy.LINEAR_BACKOFF
