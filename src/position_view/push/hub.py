"""Fan-out to live subscribers: bounded, conflating, and unable to block the projection.

WHAT THIS LAYER IS FOR, AND THE FAILURE IT EXISTS TO PREVENT
---------------------------------------------------------------------------------
The projector calls one callback when positions change (ADR-027). Something has to turn that single
callback into N live subscribers — SSE connections, in slice 5 — and the naive version of that is
three lines:

    async def publish(self, positions):
        for subscriber in self._subscribers:
            await subscriber.send(positions)      # <-- the bug

That `await` is on a network socket, and it is called *under the projector's lock* (ADR-022). One
subscriber on a slow connection, one browser tab that has been suspended by the OS, one laptop that
closed its lid without closing the TCP connection — and the projection stops applying events. Not
"slows down": stops, for as long as the TCP write buffer stays full, which can be minutes. Every
other subscriber then falls behind too, so the symptom is "the whole read model is stuck" and the
cause is one client nobody can identify.

So the rule this module is built around: **the projection's thread of control must never touch a
subscriber's I/O.** `publish` puts state into per-subscriber buffers and returns. Each subscriber's
own task does the sending, at its own pace, and the consequence of being slow is borne by that
subscriber alone.

⚠️ `publish` CONTAINS NO `await`, AND THAT IS THE PROPERTY THE WHOLE FILE RESTS ON.
   Same trick as `InMemoryPositionStore.commit`, for a different reason. There, no-suspension gives
   atomicity; here it gives two things at once. It makes `publish` unable to block the projector by
   construction rather than by inspection, and it means the subscriber list cannot be mutated
   half-way through a fan-out, so no lock is needed to iterate it. A test parses this module and
   fails if an `await` appears inside `publish`, because the day somebody adds an `await
   metrics.push()` there, every guarantee above is silently gone and every test still passes.

WHY THE BUFFER CONFLATES INSTEAD OF QUEUEING
---------------------------------------------------------------------------------
The obvious buffer is `asyncio.Queue(maxsize=N)`. It is the wrong shape here, and the reason is
worth stating precisely: **a queue is a backlog of events, and what is being published is state.**

A subscriber that is 500 updates behind on a position does not want the 500 intermediate versions.
It wants the current one. Keeping the intermediate versions costs memory to hold, bandwidth to send,
and CPU on the client to apply — and every one of them is superseded by the time it arrives. The
queue also forces an impossible choice when it fills: block the producer (forbidden, see above), or
drop. And a bounded `asyncio.Queue` drops the *newest* item, which is the worst of all the options —
the subscriber keeps a stale backlog and loses the update it actually needed.

Conflation dominates all of that. The buffer is a `dict[PositionKey, Position]`, so a second update
for a position *replaces* the first, and the buffer's size is bounded by the number of distinct
positions the subscriber watches rather than by the event rate. A subscriber watching one
counterparty cannot be made to fall behind by volume at all: however many events arrive, its buffer
holds at most one entry.

This is only legitimate because of ADR-009 and ADR-022 together. `Position` carries a `version` that
only moves when observable content changes, and the projector publishes under a lock, so
per-position publish order is version order — which means "the last one wins" is the same as "the
newest one wins". If the payload were an event stream rather than versioned state, conflation would
be data loss.

WHY OVERFLOW DISCONNECTS RATHER THAN DEGRADES
---------------------------------------------------------------------------------
A subscriber watching *every* position can still overflow: the buffer is bounded by the number of
distinct keys, and that number is unbounded if the subscriber asked for all of them. When it
happens, this module marks the subscription overflowed, discards the buffer, and makes the
next drain raise.

The alternative — keep some of the buffer and carry on — sends the client a subset of the changes
with no indication that anything was dropped, so the client's view of some positions is silently
frozen at an old version while others keep updating. That is indistinguishable from those positions
having genuinely stopped changing, which is a real and interesting state. Failing loudly means the
client re-snapshots and is correct again; degrading quietly means it is wrong and confident.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator, Iterable, Iterator
from typing import Any, Final

from ..model import Position, PositionKey

#: How many distinct positions a single subscriber may have buffered before it is cut off.
#:
#: 256 is a guess, and it is deliberately a guess with a stated shape rather than a tuned number: it
#: is "more positions than any human dashboard shows at once, far fewer than a whole book". A
#: subscriber watching specific counterparties never approaches it (its buffer is bounded by the
#: number of keys it asked for), so this only ever bites a wildcard subscriber that has stopped
#: reading — which is exactly the case that should be cut off.
DEFAULT_MAX_PENDING: Final = 256

#: The version number that means "this subscriber has been sent nothing for this position yet".
#:
#: Zero rather than `None` for the same reason `NO_CHECKPOINT` is zero: `Position.version` starts at
#: 1 after the first real change, so no real version can collide with the sentinel, and every
#: comparison stays a plain integer comparison with no null check to forget.
NOTHING_DELIVERED: Final = 0


class SubscriberOverflow(Exception):
    """This subscriber fell too far behind and its buffer was discarded.

    Raised from `drain()` rather than returned, because unlike `Skipped` (ADR-008) there is no way
    to carry on: the subscription's view of the world now has holes in it and the only correct next
    step is a fresh snapshot. A caller that wants to ignore this has to write the `except` and will
    see what it is ignoring.
    """


class SubscriptionClosed(Exception):
    """The subscription was closed while a reader was waiting on it.

    A distinct exception rather than an empty return, because "the stream has ended" and "there was
    nothing to send" are different facts and a caller that confuses them either spins or hangs.
    """


class Subscription:
    """One subscriber's conflating buffer, its key filter, and its position in the version stream.

    Constructed by `SubscriptionHub.subscribe` rather than directly — the hub has to know about it
    to publish to it, and a `Subscription` nobody publishes to is a bug that looks like a
    working object.

    THE THREE PIECES OF STATE, AND WHY EACH ONE IS HERE
      - `_pending`: the conflating buffer. One entry per position, holding the newest version.
      - `_last_delivered`: the highest version already sent for each position. What makes the
        snapshot-then-stream handover safe; see `mark_delivered`.
      - `_wakeup`: an `asyncio.Event` rather than a queue, because the buffer holds the data.
        The event carries no payload and needs no bound — it is a doorbell, and a doorbell rung
        twice before anyone answers is one visit.
    """

    def __init__(
        self,
        *,
        keys: frozenset[PositionKey] | None = None,
        max_pending: int = DEFAULT_MAX_PENDING,
    ) -> None:
        if max_pending < 1:
            raise ValueError(f"max_pending must be at least 1, got {max_pending}")

        #: None means "every position". A frozenset means exactly those keys.
        #:
        #: `None` rather than an empty set for "everything", because an empty set is what a caller
        #: passing a filtered-to-nothing list produces by accident, and silently promoting that to
        #: "send me the entire book" is the wrong default in a service that fans out money.
        self._keys = keys
        self._max_pending = max_pending
        self._pending: dict[PositionKey, Position] = {}
        self._last_delivered: dict[PositionKey, int] = {}
        self._wakeup = asyncio.Event()
        self._closed = False

        #: Set once and never cleared: an overflowed subscription is finished, not throttled.
        self.overflowed = False

        #: Counters, all of them for one question: is this subscriber keeping up?
        #:
        #: `conflated` is the interesting one. A rising `conflated` with a healthy connection means
        #: the subscriber is being *protected* by conflation and is fine. A rising `conflated` on a
        #: subscriber whose `batches_delivered` has stopped moving means it is not reading at all,
        #: and it will overflow if it watches enough keys.
        self.offered = 0
        self.conflated = 0
        self.suppressed = 0
        self.positions_delivered = 0
        self.batches_delivered = 0

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def pending_count(self) -> int:
        """How many positions are buffered. What the overflow bound is measured against."""
        return len(self._pending)

    @property
    def watches_everything(self) -> bool:
        return self._keys is None

    def watches(self, key: PositionKey) -> bool:
        """Whether this subscriber asked for this position.

        Filtering here rather than at the hub is deliberate: the hub would have to build a reverse
        index from key to subscribers to do better, and that index has to be maintained on every
        subscribe and unsubscribe. With a handful of subscribers a per-subscriber membership test is
        faster than maintaining the index, and it keeps the hub's fan-out loop honest — it visits
        every subscriber exactly once and cannot forget one because an index was stale.
        """
        return self._keys is None or key in self._keys

    def offer(self, position: Position) -> None:
        """Buffer one position, conflating with anything already buffered for that key.

        Synchronous, and it must stay that way — this is the method `publish` calls, and `publish`
        is called under the projector's lock. There is nothing here that can block, nothing that can
        raise (a `dict` assignment and an `Event.set()`), and nothing that depends on the subscriber
        being alive. A dead subscriber's buffer fills to its bound and then the subscription is
        marked overflowed; the projection never learns about it.
        """
        if self._closed or self.overflowed:
            # Nothing to do, and no error either. A publish that arrives for a subscriber which has
            # just disconnected is the normal case, not an exceptional one — the disconnect and the
            # event race by nature, and there is no ordering between them worth enforcing.
            return
        if not self.watches(position.key):
            return

        self.offered += 1
        if position.key in self._pending:
            # Conflation: the buffered version is superseded, not queued behind. Counted because it
            # is the earliest signal that a subscriber is not keeping up.
            self.conflated += 1
        self._pending[position.key] = position

        if len(self._pending) > self._max_pending:
            # Checked after the insert rather than before, so the bound means "may hold this many"
            # rather than "may hold one fewer than this many". The buffer is discarded in full: a
            # partial buffer would be delivered as though it were complete, and the subscriber would
            # have no way to know which positions it is now wrong about.
            self.overflowed = True
            self._pending.clear()

        # Set last, and set even on overflow, so a reader waiting on an empty buffer wakes up to
        # find either data or the overflow. Setting it before the mutation would let a reader that
        # is already runnable wake to an empty buffer and loop for nothing.
        self._wakeup.set()

    def mark_delivered(self, positions: Iterable[Position]) -> None:
        """Record versions this subscriber already has, so the stream does not resend them.

        ⚠️ THIS IS THE SNAPSHOT-THEN-STREAM HANDOVER, AND IT IS THE RACE EVERY PUSH API GETS WRONG.

           A client needs the current state and then every change after it. There are two obvious
           orderings and both are broken:

             snapshot, then subscribe -> every change between the two is lost, permanently, and the
                                         client's view of those positions is frozen at the snapshot
                                         with nothing to indicate it.
             subscribe, then snapshot -> nothing is lost, but the client receives updates it already
                                         has, and cannot tell a stale duplicate from a new change.

           The second ordering plus this method is the fix. Subscribing first means the buffer
           starts filling immediately, so no change can slip through the gap; calling
           `mark_delivered` with the snapshot that was just sent means every buffered version at or
           below it is dropped instead of sent. The client gets each version exactly once,
           in order, with no gap.

           This works because `version` is monotonic per position and only moves on observable
           change (ADR-009). A timestamp could not be used here: two changes within the same clock
           tick would be indistinguishable, and the second would be suppressed as a duplicate.
        """
        for position in positions:
            previous = self._last_delivered.get(position.key, NOTHING_DELIVERED)
            # `max` rather than assignment, so calling this with an older snapshot cannot move the
            # watermark backwards and cause a version to be sent twice.
            self._last_delivered[position.key] = max(previous, position.version)

    def drain_nowait(self) -> tuple[Position, ...]:
        """Take everything buffered, dropping versions this subscriber already has. Never blocks.

        Returns an empty tuple when there is nothing new, which is a normal outcome and not an
        error: it happens after a spurious wakeup, and it happens when every buffered version was
        already covered by the snapshot. Raises if the subscription is finished, because a caller
        that receives `()` from a dead subscription would poll forever.
        """
        if self.overflowed:
            raise SubscriberOverflow(
                f"subscriber fell behind on more than {self._max_pending} positions; "
                f"its buffer was discarded and it must re-snapshot"
            )
        if self._closed:
            raise SubscriptionClosed("this subscription has been closed")

        # Swap the buffer out before iterating it. Not for atomicity — there is no `await` here —
        # but so that the "clear" is unconditional and cannot be skipped by an early return.
        taken, self._pending = self._pending, {}
        self._wakeup.clear()

        fresh: list[Position] = []
        for key in sorted(taken, key=str):
            # Sorted by key so a batch has a stable order. It matters for the tests, and it matters
            # for a client rendering a table: an unordered batch makes rows jump between updates for
            # no reason a user can see.
            position = taken[key]
            if position.version <= self._last_delivered.get(key, NOTHING_DELIVERED):
                self.suppressed += 1
                continue
            self._last_delivered[key] = position.version
            fresh.append(position)

        self.positions_delivered += len(fresh)
        if fresh:
            self.batches_delivered += 1
        return tuple(fresh)

    async def drain(self) -> tuple[Position, ...]:
        """Wait until there is something new, then take it. Never returns an empty batch.

        The loop is not decoration. A wakeup can arrive with nothing to deliver — the buffered
        version was already covered by the snapshot, or two publishes rang the doorbell and the
        first drain took both — and returning `()` from a blocking call would make the caller's
        `async for` emit empty batches down a socket. Looping means "blocking" is honest.
        """
        while True:
            await self._wakeup.wait()
            batch = self.drain_nowait()
            if batch:
                return batch

    async def updates(self) -> AsyncIterator[tuple[Position, ...]]:
        """Every change from now on, as an async iterator. The shape slice 5's SSE endpoint wants.

        Ends cleanly on close and propagates `SubscriberOverflow`, which is the whole point: a
        `for`-loop over this cannot accidentally treat "you have fallen behind and lost data" as
        "the stream ended normally", because one path returns and the other raises.
        """
        while True:
            try:
                yield await self.drain()
            except SubscriptionClosed:
                return

    def close(self) -> None:
        """Idempotent, and it wakes any waiter so a closed subscription cannot leave a task parked.

        Idempotence matters because the two callers race by nature: the connection handler closes on
        disconnect, and the hub closes on shutdown. Making the second call an error would turn an
        ordinary shutdown into a logged exception.
        """
        self._closed = True
        self._pending.clear()
        self._wakeup.set()

    def as_dict(self) -> dict[str, Any]:
        """This subscriber's counters, for a `/subscribers` endpoint or a log line on disconnect."""
        return {
            "watches": "*" if self._keys is None else sorted(str(key) for key in self._keys),
            "pending": len(self._pending),
            "offered": self.offered,
            "conflated": self.conflated,
            "suppressed": self.suppressed,
            "positions_delivered": self.positions_delivered,
            "batches_delivered": self.batches_delivered,
            "overflowed": self.overflowed,
            "closed": self._closed,
        }

    def __repr__(self) -> str:
        watching = "*" if self._keys is None else f"{len(self._keys)} keys"
        return (
            f"Subscription(watching={watching}, pending={len(self._pending)}, "
            f"delivered={self.positions_delivered}, overflowed={self.overflowed})"
        )


class SubscriptionHub:
    """The fan-out point. Holds the subscribers and offers each of them every change.

    Attaches to a `Projector` by passing `on_change=hub.publish`, which is the entire integration —
    the projector knows nothing about subscribers and this module knows nothing about events. That
    seam is why the projector's tests need no hub and the hub's tests need no projector, and why the
    two things below can both be true: the projection is serialised under a lock, and a slow
    subscriber cannot slow it down.
    """

    def __init__(self, *, max_pending: int = DEFAULT_MAX_PENDING) -> None:
        self._subscriptions: list[Subscription] = []
        self._max_pending = max_pending

        #: Total positions handed to `publish`, and how many subscribers have been cut off.
        #:
        #: `overflow_disconnects` is the one to alert on. It means a client was reading too slowly
        #: to keep up with a whole book, which is either a client bug or a signal that the wildcard
        #: subscription it asked for is the wrong shape for what it is doing.
        self.published = 0
        self.overflow_disconnects = 0

    @property
    def subscribers(self) -> int:
        return len(self._subscriptions)

    def subscribe(self, *, keys: Iterable[PositionKey] | None = None) -> Subscription:
        """Register a subscriber and start buffering for it immediately.

        Synchronous, and that is a guarantee rather than an implementation detail: by the time this
        returns, the subscription is receiving. An `async` version would give the caller no way to
        know whether a change published during the await was captured, which reintroduces the exact
        gap `mark_delivered` exists to close.
        """
        subscription = Subscription(
            keys=None if keys is None else frozenset(keys),
            max_pending=self._max_pending,
        )
        self._subscriptions.append(subscription)
        return subscription

    def unsubscribe(self, subscription: Subscription) -> None:
        """Close and forget a subscription. Safe to call twice, and safe on one never registered."""
        subscription.close()
        if subscription in self._subscriptions:
            self._subscriptions.remove(subscription)

    @contextlib.contextmanager
    def subscription(self, *, keys: Iterable[PositionKey] | None = None) -> Iterator[Subscription]:
        """Subscribe for the duration of a block, unsubscribing however the block ends.

        The reason this exists rather than leaving `subscribe`/`unsubscribe` to the caller: the
        caller is a connection handler, and the way a connection handler ends is almost never by
        reaching its last line. It ends with a `CancelledError` when the client disconnects, or with
        an exception from a socket write. A `finally` is the only construct that survives both, and
        putting it here means every call site gets it instead of the one that remembered.

        A leaked subscription is not a small leak. It keeps buffering forever, so it holds
        `Position` objects alive for positions nobody is watching, and it is offered every change
        for the life of the process.
        """
        subscription = self.subscribe(keys=keys)
        try:
            yield subscription
        finally:
            self.unsubscribe(subscription)

    async def publish(self, positions: tuple[Position, ...]) -> None:
        """Offer a batch of changed positions to every subscriber. Contains no `await`, by design.

        ⚠️ THE SIGNATURE IS `async` AND THE BODY SUSPENDS NOWHERE. BOTH HALVES ARE DELIBERATE.
           `async` because `ChangeListener` is an awaitable callback, and the projector must be able
           to accept a listener that genuinely does I/O — a Kafka producer, say — without a second
           code path. No suspension because *this particular* listener is called under the
           projector's lock, so any suspension here is a suspension of the whole projection.

           The absence of an `await` also removes the need for a lock on `_subscriptions`: the list
           cannot be mutated part-way through the loop, because nothing can run between the first
           iteration and the last. `subscribe` and `unsubscribe` are synchronous for the same reason
           — they are safe against a concurrent publish because neither of them can interleave with
           one.

        A batch rather than one position at a time, so that both sides of a cross-position move
        (ADR-019) reach a subscriber in a single delivery. Split across two deliveries, a client
        renders a moment where the money exists in neither position, or in both.
        """
        self.published += len(positions)

        # Drop closed subscriptions before fanning out. Guarded by `any` rather than rebuilt
        # unconditionally, because publish is on the hot path and the common case is that nothing
        # has closed. A closed subscription would be harmless to offer to — `offer` returns
        # immediately — but it would be kept alive by this list forever: a leak with a slow fuse.
        if any(subscription.is_closed for subscription in self._subscriptions):
            self._subscriptions = [s for s in self._subscriptions if not s.is_closed]

        for subscription in self._subscriptions:
            was_overflowed = subscription.overflowed
            for position in positions:
                subscription.offer(position)
            if subscription.overflowed and not was_overflowed:
                # Counted here rather than inside `offer` so that the subscription does not need a
                # reference back to the hub. A one-way dependency means a `Subscription` can be
                # constructed and tested without a hub at all.
                self.overflow_disconnects += 1

    def close(self) -> None:
        """Close every subscription. For process shutdown, so no reader task is left parked."""
        for subscription in list(self._subscriptions):
            subscription.close()
        self._subscriptions.clear()

    def as_dict(self) -> dict[str, Any]:
        """The hub's own counters plus one row per subscriber."""
        return {
            "subscribers": len(self._subscriptions),
            "published": self.published,
            "overflow_disconnects": self.overflow_disconnects,
            "detail": [subscription.as_dict() for subscription in self._subscriptions],
        }
