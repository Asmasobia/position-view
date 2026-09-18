"""Fan-out: one projection callback in, many live subscribers out, with no back-pressure inward.

The single rule this package exists to enforce: **the projection's thread of control never touches a
subscriber's socket.** `SubscriptionHub.publish` is the `ChangeListener` the projector calls under
its lock, and it does nothing but drop state into per-subscriber buffers. The sending happens on
each subscriber's own task, so the cost of a slow client is paid by that client alone.

Depends on `model/` (for `Position` and `PositionKey`) and on nothing else — not even on
`projection/`, despite being wired to it, because the seam is a plain callable. That is why the hub
can be tested with hand-built positions and no projector, and why `tests/test_layering.py` permits
this package no third-party imports at all: there is nothing here a framework could help with.

The two ideas worth knowing before reading `hub.py`:

  - **Conflation, not queueing.** The buffer is keyed by position, so a newer version replaces an
    older one instead of queueing behind it. Legitimate only because the payload is versioned
    *state* (ADR-009), not events — for an event stream this would be data loss.
  - **Subscribe, then snapshot, then stream.** Registering first means no change slips through the
    gap; `Subscription.mark_delivered` then suppresses anything the snapshot already covered, so the
    client sees every version exactly once. Both orderings without that call are broken, one by a
    gap and one by duplicates.
"""

from .hub import (
    DEFAULT_MAX_PENDING,
    NOTHING_DELIVERED,
    SubscriberOverflow,
    Subscription,
    SubscriptionClosed,
    SubscriptionHub,
)

__all__ = [
    "DEFAULT_MAX_PENDING",
    "NOTHING_DELIVERED",
    "SubscriberOverflow",
    "Subscription",
    "SubscriptionClosed",
    "SubscriptionHub",
]
