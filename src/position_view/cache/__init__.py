"""The read-through cache: one store method, protected from stampedes and refreshed by version.

One class, and its shape is the argument. `CachedPositions` wraps `PositionStore.position_for` and
exposes nothing else, so it cannot be handed to a `Projector` in place of a store — which would make
the projection compute new positions from possibly-stale cached ones and write the result back as
truth. `positions.py`'s module docstring walks through that failure; `tests/test_cache.py` asserts
the public surface so the "improvement" that enables it fails a test rather than a review.

Depends on `model/` for the values and on `projection/` for the store protocol. Nothing here imports
`push/` — a cache that knew about subscribers could not be used by a batch job — and nothing here
imports a framework, which is what keeps the cache usable from a background refresh as well as from
a request.

There is no clock in this package. Invalidation comes from the projector's change listener, which
knows precisely when a position moved and carries the new value with it, so a TTL would be a second
and strictly worse mechanism bolted onto a precise one.
"""

from .positions import DEFAULT_MAX_ENTRIES, UNCACHED_VERSION, CachedPositions

__all__ = [
    "DEFAULT_MAX_ENTRIES",
    "UNCACHED_VERSION",
    "CachedPositions",
]
