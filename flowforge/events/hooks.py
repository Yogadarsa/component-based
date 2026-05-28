"""
FlowForge Event Hooks
=====================
Publish / subscribe event system for monitoring workflow lifecycle.

The ``EventBus`` lets users register callbacks for specific lifecycle
events (e.g. ``NODE_STARTED``, ``WORKFLOW_FAILED``).  The engine emits
events at each transition; registered listeners are invoked synchronously
in the order they were registered.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional

from flowforge.enums import EventType

logger = logging.getLogger("flowforge.events")


class Event:
    """
    An immutable event payload emitted by the engine.

    Attributes
    ----------
    event_type : EventType
        The kind of event.
    node_id : str or None
        The node involved, if applicable.
    data : dict
        Arbitrary payload data.
    """

    __slots__ = ("event_type", "node_id", "data")

    def __init__(
        self,
        event_type: EventType,
        node_id: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.event_type = event_type
        self.node_id = node_id
        self.data = data or {}

    def __repr__(self) -> str:
        return (
            f"Event(type={self.event_type.name}, node={self.node_id!r}, "
            f"data_keys={list(self.data.keys())})"
        )


class EventBus:
    """
    Central pub/sub hub for workflow events.

    Listeners are simple callables that accept a single :class:`Event`
    argument.

    Examples
    --------
    >>> bus = EventBus()
    >>> bus.on(EventType.NODE_COMPLETED, lambda e: print(f"Done: {e.node_id}"))
    >>> bus.emit(EventType.NODE_COMPLETED, node_id="extract")
    Done: extract
    """

    def __init__(self) -> None:
        self._listeners: Dict[EventType, List[Callable[[Event], None]]] = (
            defaultdict(list)
        )
        self._global_listeners: List[Callable[[Event], None]] = []
        self._history: List[Event] = []
        self._record_history = False

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def on(
        self,
        event_type: EventType,
        callback: Callable[[Event], None],
    ) -> "EventBus":
        """
        Register a listener for a specific event type.

        Returns ``self`` for chaining.
        """
        self._listeners[event_type].append(callback)
        return self

    def on_any(self, callback: Callable[[Event], None]) -> "EventBus":
        """
        Register a listener that fires for **every** event.

        Returns ``self`` for chaining.
        """
        self._global_listeners.append(callback)
        return self

    def off(
        self,
        event_type: EventType,
        callback: Callable[[Event], None],
    ) -> "EventBus":
        """Remove a previously registered listener."""
        try:
            self._listeners[event_type].remove(callback)
        except ValueError:
            pass
        return self

    def clear(self, event_type: Optional[EventType] = None) -> None:
        """
        Remove all listeners.

        Parameters
        ----------
        event_type : EventType, optional
            If given, only clears listeners for that event type.
            Otherwise clears everything.
        """
        if event_type:
            self._listeners[event_type].clear()
        else:
            self._listeners.clear()
            self._global_listeners.clear()

    # ------------------------------------------------------------------
    # Emission
    # ------------------------------------------------------------------

    def emit(
        self,
        event_type: EventType,
        node_id: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Event:
        """
        Emit an event, invoking all matching listeners synchronously.

        Returns the created :class:`Event` object.
        """
        event = Event(event_type=event_type, node_id=node_id, data=data)

        if self._record_history:
            self._history.append(event)

        # Specific listeners
        for cb in self._listeners.get(event_type, []):
            try:
                cb(event)
            except Exception as exc:
                logger.warning(
                    "Event listener for %s raised %s: %s",
                    event_type.name,
                    type(exc).__name__,
                    exc,
                )

        # Global listeners
        for cb in self._global_listeners:
            try:
                cb(event)
            except Exception as exc:
                logger.warning(
                    "Global event listener raised %s: %s",
                    type(exc).__name__,
                    exc,
                )

        return event

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------

    def enable_history(self) -> None:
        """Start recording all emitted events."""
        self._record_history = True

    def disable_history(self) -> None:
        """Stop recording events (existing history is preserved)."""
        self._record_history = False

    @property
    def history(self) -> List[Event]:
        """Return a copy of the recorded event history."""
        return list(self._history)

    def clear_history(self) -> None:
        """Clear the event history buffer."""
        self._history.clear()

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        total = sum(len(v) for v in self._listeners.values())
        total += len(self._global_listeners)
        return f"EventBus(listeners={total})"
