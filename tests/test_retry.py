"""
Tests for FlowForge Retry Policies
====================================
Covers each retry strategy, max retries, delay computation, and retry_on filtering.
"""

import pytest

from flowforge.policies.retry import (
    RetryPolicy,
    NoRetryPolicy,
    FixedRetryPolicy,
    ExponentialBackoffPolicy,
    LinearBackoffPolicy,
)
from flowforge.enums import RetryStrategy


# ── NoRetryPolicy ────────────────────────────────────────────────────

class TestNoRetryPolicy:

    def test_never_retries(self):
        policy = NoRetryPolicy()
        assert not policy.should_retry(1, ValueError("err"))

    def test_delay_is_zero(self):
        policy = NoRetryPolicy()
        assert policy.get_delay(1) == 0.0

    def test_max_retries_is_zero(self):
        assert NoRetryPolicy().max_retries == 0


# ── FixedRetryPolicy ─────────────────────────────────────────────────

class TestFixedRetryPolicy:

    def test_constant_delay(self):
        policy = FixedRetryPolicy(max_retries=3, delay=2.0)
        assert policy.get_delay(1) == 2.0
        assert policy.get_delay(2) == 2.0
        assert policy.get_delay(3) == 2.0

    def test_should_retry_within_limit(self):
        policy = FixedRetryPolicy(max_retries=3)
        assert policy.should_retry(1, ValueError())
        assert policy.should_retry(2, ValueError())
        assert not policy.should_retry(3, ValueError())

    def test_retry_on_filter(self):
        policy = FixedRetryPolicy(
            max_retries=3, retry_on=(ValueError, TypeError)
        )
        assert policy.should_retry(1, ValueError("v"))
        assert policy.should_retry(1, TypeError("t"))
        assert not policy.should_retry(1, RuntimeError("r"))

    def test_strategy(self):
        assert FixedRetryPolicy().strategy == RetryStrategy.FIXED_DELAY


# ── ExponentialBackoffPolicy ─────────────────────────────────────────

class TestExponentialBackoffPolicy:

    def test_exponential_delays(self):
        policy = ExponentialBackoffPolicy(
            max_retries=5, base_delay=1.0, max_delay=60.0
        )
        assert policy.get_delay(1) == 1.0   # 1 * 2^0
        assert policy.get_delay(2) == 2.0   # 1 * 2^1
        assert policy.get_delay(3) == 4.0   # 1 * 2^2
        assert policy.get_delay(4) == 8.0   # 1 * 2^3

    def test_max_delay_cap(self):
        policy = ExponentialBackoffPolicy(
            max_retries=10, base_delay=1.0, max_delay=5.0
        )
        assert policy.get_delay(5) == 5.0  # 16 capped to 5

    def test_strategy(self):
        assert (
            ExponentialBackoffPolicy().strategy
            == RetryStrategy.EXPONENTIAL_BACKOFF
        )


# ── LinearBackoffPolicy ─────────────────────────────────────────────

class TestLinearBackoffPolicy:

    def test_linear_delays(self):
        policy = LinearBackoffPolicy(
            max_retries=5, base_delay=2.0, max_delay=30.0
        )
        assert policy.get_delay(1) == 2.0   # 2 * 1
        assert policy.get_delay(2) == 4.0   # 2 * 2
        assert policy.get_delay(3) == 6.0   # 2 * 3

    def test_max_delay_cap(self):
        policy = LinearBackoffPolicy(
            max_retries=10, base_delay=5.0, max_delay=10.0
        )
        assert policy.get_delay(3) == 10.0  # 15 capped to 10

    def test_strategy(self):
        assert LinearBackoffPolicy().strategy == RetryStrategy.LINEAR_BACKOFF


# ── General RetryPolicy behaviour ────────────────────────────────────

class TestRetryPolicyGeneral:

    def test_negative_max_retries_clamped(self):
        policy = FixedRetryPolicy(max_retries=-5)
        assert policy.max_retries == 0

    def test_negative_delay_clamped(self):
        policy = FixedRetryPolicy(max_retries=3, delay=-1.0)
        assert policy.delay == 0.0

    def test_repr(self):
        policy = ExponentialBackoffPolicy(max_retries=5)
        r = repr(policy)
        assert "ExponentialBackoffPolicy" in r
        assert "5" in r
