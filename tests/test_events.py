"""
Tests for FlowForge EventBus
==============================
Covers event registration, emission, multiple listeners, global listeners,
history recording, and error safety.
"""

import pytest

from flowforge.events.hooks import EventBus, Event
from flowforge.enums import EventType


# ── Basic registration and emission ──────────────────────────────────

class TestEventRegistration:

    def test_on_and_emit(self):
        bus = EventBus()
        received = []
        bus.on(EventType.NODE_COMPLETED, lambda e: received.append(e))
        bus.emit(EventType.NODE_COMPLETED, node_id="step1")

        assert len(received) == 1
        assert received[0].node_id == "step1"

    def test_multiple_listeners(self):
        bus = EventBus()
        results = []
        bus.on(EventType.NODE_STARTED, lambda e: results.append("a"))
        bus.on(EventType.NODE_STARTED, lambda e: results.append("b"))
        bus.emit(EventType.NODE_STARTED)

        assert results == ["a", "b"]

    def test_different_events_are_independent(self):
        bus = EventBus()
        started = []
        completed = []
        bus.on(EventType.NODE_STARTED, lambda e: started.append(1))
        bus.on(EventType.NODE_COMPLETED, lambda e: completed.append(1))

        bus.emit(EventType.NODE_STARTED)

        assert len(started) == 1
        assert len(completed) == 0

    def test_emit_returns_event(self):
        bus = EventBus()
        event = bus.emit(EventType.WORKFLOW_STARTED, data={"name": "test"})
        assert isinstance(event, Event)
        assert event.event_type == EventType.WORKFLOW_STARTED


# ── Global listeners ─────────────────────────────────────────────────

class TestGlobalListeners:

    def test_on_any(self):
        bus = EventBus()
        all_events = []
        bus.on_any(lambda e: all_events.append(e.event_type))

        bus.emit(EventType.NODE_STARTED)
        bus.emit(EventType.NODE_COMPLETED)
        bus.emit(EventType.WORKFLOW_FAILED)

        assert len(all_events) == 3


# ── Unregistration ───────────────────────────────────────────────────

class TestUnregistration:

    def test_off(self):
        bus = EventBus()
        results = []
        cb = lambda e: results.append(1)
        bus.on(EventType.NODE_STARTED, cb)
        bus.off(EventType.NODE_STARTED, cb)
        bus.emit(EventType.NODE_STARTED)

        assert len(results) == 0

    def test_off_nonexistent_is_safe(self):
        bus = EventBus()
        bus.off(EventType.NODE_STARTED, lambda e: None)  # Should not raise

    def test_clear_specific(self):
        bus = EventBus()
        bus.on(EventType.NODE_STARTED, lambda e: None)
        bus.on(EventType.NODE_COMPLETED, lambda e: None)
        bus.clear(EventType.NODE_STARTED)

        results = []
        bus.on(EventType.NODE_STARTED, lambda e: results.append("new"))
        bus.emit(EventType.NODE_STARTED)
        assert results == ["new"]

    def test_clear_all(self):
        bus = EventBus()
        bus.on(EventType.NODE_STARTED, lambda e: None)
        bus.on_any(lambda e: None)
        bus.clear()
        # Repr should show 0 listeners
        assert "0" in repr(bus)


# ── Event data ───────────────────────────────────────────────────────

class TestEventData:

    def test_event_data(self):
        bus = EventBus()
        received = []
        bus.on(EventType.NODE_COMPLETED, lambda e: received.append(e))
        bus.emit(
            EventType.NODE_COMPLETED,
            node_id="extract",
            data={"duration": 1.5},
        )

        event = received[0]
        assert event.node_id == "extract"
        assert event.data["duration"] == 1.5

    def test_event_repr(self):
        event = Event(EventType.NODE_STARTED, node_id="x", data={"k": "v"})
        r = repr(event)
        assert "NODE_STARTED" in r
        assert "x" in r


# ── History ──────────────────────────────────────────────────────────

class TestHistory:

    def test_history_disabled_by_default(self):
        bus = EventBus()
        bus.emit(EventType.NODE_STARTED)
        assert len(bus.history) == 0

    def test_history_enabled(self):
        bus = EventBus()
        bus.enable_history()
        bus.emit(EventType.NODE_STARTED)
        bus.emit(EventType.NODE_COMPLETED)

        assert len(bus.history) == 2

    def test_clear_history(self):
        bus = EventBus()
        bus.enable_history()
        bus.emit(EventType.NODE_STARTED)
        bus.clear_history()
        assert len(bus.history) == 0

    def test_disable_history(self):
        bus = EventBus()
        bus.enable_history()
        bus.emit(EventType.NODE_STARTED)
        bus.disable_history()
        bus.emit(EventType.NODE_COMPLETED)

        assert len(bus.history) == 1  # Only the first one


# ── Error safety ─────────────────────────────────────────────────────

class TestErrorSafety:

    def test_listener_error_does_not_propagate(self):
        bus = EventBus()
        bus.on(EventType.NODE_STARTED, lambda e: 1 / 0)  # ZeroDivisionError
        # Should not raise
        bus.emit(EventType.NODE_STARTED)

    def test_other_listeners_still_fire_after_error(self):
        bus = EventBus()
        results = []
        bus.on(EventType.NODE_STARTED, lambda e: 1 / 0)
        bus.on(EventType.NODE_STARTED, lambda e: results.append("ok"))
        bus.emit(EventType.NODE_STARTED)

        assert results == ["ok"]


# ── Chaining ─────────────────────────────────────────────────────────

class TestChaining:

    def test_on_returns_self(self):
        bus = EventBus()
        result = bus.on(EventType.NODE_STARTED, lambda e: None)
        assert result is bus

    def test_chain_multiple(self):
        bus = EventBus()
        result = (
            bus.on(EventType.NODE_STARTED, lambda e: None)
               .on(EventType.NODE_COMPLETED, lambda e: None)
               .on_any(lambda e: None)
        )
        assert result is bus
