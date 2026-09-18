"""The dull half of the projection: read, decide, write, tell somebody. In that order.

WHY THIS FILE IS DELIBERATELY BORING
---------------------------------------------------------------------------------
Every judgement about what an event *means* was made in `model/project.py`, by a synchronous
function of two values. What is left here is the part that cannot be pure: reading the previous
state, writing the new state as one unit, and counting what happened. There is no arithmetic in this
file and no business rule — if either appears here, it has escaped from a place where it could be
tested exhaustively into a place where it needs a store to be tested at all.

The one-line test of whether the split has been respected: this module never compares two amounts
and never mentions a `Bucket`.

WHAT THE PROJECTOR ADDS THAT THE PURE FUNCTION CANNOT
---------------------------------------------------------------------------------
Three things, and each one is a way to be wrong that no unit test of `project` would ever catch:

  1. **Atomicity.** The snapshot, the position rows and the checkpoint move together or not at all.
     `WriteBatch` makes that structural; this file's job is to build exactly one batch per event.
  2. **Serialisation.** Read-modify-write across an `await` is a lost update waiting to happen, and
     the whole design assumes a single sequential consumer. That assumption is enforced with a lock
     rather than written in a comment, because it is invisible until the day it is violated.
  3. **Observability.** Five skip reasons, a first-sighting count and an event-time watermark, all
     counted only after the write succeeded — see `ProjectionStats` for why "after" is load-bearing.

WHY THE PROJECTOR DOES NOT HANDLE TRANSPORT
---------------------------------------------------------------------------------
There is no consumer loop here, no reconnect, no acknowledgement. `ingest` takes one decoded JSON
object and returns what happened. That boundary is what lets the entire projection be tested by
calling a coroutine in a list comprehension, and it is why the replay-idempotence test is four lines
rather than a broker in a container.

The consumer that owns the socket also owns the two decisions this module refuses to make: when to
acknowledge, and what to do with a message that cannot be decoded at all. `Refused` exists to hand
that second one back explicitly.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..model import (
    Applied,
    Envelope,
    MalformedEventError,
    Outcome,
    Position,
    PositionKey,
    Skipped,
    SkipReason,
    decode_envelope,
    project,
)
from .store import NO_CHECKPOINT, PositionStore, WriteBatch

#: Called after a batch is durable, with every position whose content actually changed.
#:
#: This is the seam the fan-out hub plugs into in slice 3. Typed as a callable rather than declared
#: as a `Hub` parameter so that `projection/` does not import `push/` — the arrow runs the other
#: way, and a projector that knows about subscribers is a projector that cannot be used by a batch
#: rebuild.
ChangeListener = Callable[[tuple[Position, ...]], Awaitable[None]]


@dataclass(frozen=True)
class Refused:
    """The message could not be decoded, so nothing about it is known — not even its `event_id`.

    ⚠️ A REFUSAL DOES NOT ADVANCE THE CHECKPOINT, AND THAT IS NOT AN OVERSIGHT.
       The checkpoint is an `event_id`, and the reason this message was refused may well be that its
       `event_id` was missing or was a boolean. There is nothing to advance it *to*.

       Which leaves the caller an unpleasant choice, and the honest thing is to hand it back rather
       than pick silently: acknowledge the message and lose it, or refuse it and stall the partition
       behind it. Both are bad; which one is correct depends on whether the stream is
       carrying settlement instructions or click events, and this module does not know. A transport
       that supports dead-lettering picks a third option and should.

       The failure mode being avoided is the tidy-looking one: catch the error, log it, advance the
       checkpoint, move on. That is a silent data-loss switch with a reassuring log line next to it.
    """

    error: str


@dataclass
class ProjectionStats:
    """What the projection has done, for `/projection` and for the alerts built on it.

    ⚠️ EVERY COUNTER HERE IS INCREMENTED *AFTER* A SUCCESSFUL COMMIT, NEVER BEFORE.
       Counting first reads more naturally — the event has arrived, so count it — and it makes every
       number wrong in the same direction the moment the store starts failing. A commit that fails
       is retried by the consumer, so an event counted before the write is counted once per attempt.
       A database wobble shows up as a *rise* in applied events. The one metric that should reveal a
       stalled projection instead reports it as unusually busy.

    Mutable on purpose, and it is the only mutable state in the projection layer. A frozen dataclass
    replaced on every event would allocate one object per message to hold nine integers, and — more
    to the point — a caller holding a reference would silently be looking at a stale copy.
    `/projection` reads this live, so identity is the useful property.
    """

    #: Every message handed to the projector, including the ones it could not decode. The
    #: denominator.
    events_seen: int = 0

    #: Events that produced an `Applied` outcome, whether or not any number moved.
    applied: int = 0

    #: Events that changed at least one position's content. Strictly fewer than `applied`, and the
    #: gap is not noise: it is the `validated`/`matched` traffic, which is real work for the
    #: projection and nothing at all for a subscriber. Slice 3's conflation and slice 4's
    #: invalidation both exist because this number is smaller than `applied`, so having both is what
    #: makes that arguable rather than assumed.
    changed: int = 0

    #: Instructions seen for the first time. Expected to equal `applied` during a rebuild from empty
    #: and to be a small fraction of it in steady state. A steady trickle whose first event is not
    #: `instruction.received` means the stream's retention is shorter than the projection's lag, so
    #: positions are being built from partial history — see `Applied.first_seen`.
    first_seen: int = 0

    #: Messages that could not be decoded. Should be zero. Not a skip: a skip is this service
    #: declining an event it understands, and these are events it could not read.
    refused: int = 0

    #: Positions written. Larger than `changed` is impossible; smaller by more than a rounding
    #: amount means cross-position moves are not happening, since those write two rows for one
    #: event.
    positions_written: int = 0

    #: One counter per reason, pre-populated with every member so that `/projection` reports a
    #: stable shape. An absent key would make a dashboard panel disappear exactly when the thing it
    #: watches stops happening, which is indistinguishable from the panel being broken.
    skips: dict[SkipReason, int] = field(
        default_factory=lambda: dict.fromkeys(SkipReason, 0),
    )

    #: The highest `event_id` applied, and the `occurred_at` of the most recent event.
    #:
    #: ⚠️ `last_event_at` IS EVENT TIME, AND `now - last_event_at` IS STALENESS, NOT LAG.
    #:    The distinction is the most common mistake in read-model monitoring. On a busy stream
    #:    the two are the same number and the alert works. On a quiet stream they are not: a
    #:    projection that is perfectly caught up on a topic with no traffic reports a staleness that
    #:    grows by one second per second, and the on-call gets paged for a system that is idle.
    #:
    #:    Real lag is `producer_head_event_id - last_event_id`, and this service cannot compute it,
    #:    because it does not know the head. Only the broker does. So the honest thing is to expose
    #:    the two raw facts and let whoever *can* see both sides do the subtraction, rather than to
    #:    publish a number called "lag" that means something else.
    last_event_id: int = NO_CHECKPOINT
    last_event_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        """A JSON-shaped view, with the skip reasons flattened to their string values.

        Written here rather than in the api layer because the field names are this module's
        business, and a projection whose metric names change when somebody edits a response model is
        a projection whose dashboards break for no reason. The api layer serialises; it does not
        name.
        """
        return {
            "events_seen": self.events_seen,
            "applied": self.applied,
            "changed": self.changed,
            "first_seen": self.first_seen,
            "refused": self.refused,
            "positions_written": self.positions_written,
            "skipped": {reason.value: count for reason, count in self.skips.items()},
            "last_event_id": self.last_event_id,
            "last_event_at": None if self.last_event_at is None else self.last_event_at.isoformat(),
        }


class Projector:
    """Applies events to a store, one at a time, atomically.

    ⚠️ ONE EVENT AT A TIME IS A CORRECTNESS REQUIREMENT, NOT A PERFORMANCE CHOICE, AND THE LOCK IS
       WHAT MAKES IT TRUE RATHER THAN INTENDED.

       Applying an event is read-modify-write with two suspension points in the middle:

           snapshot = await store.snapshot_for(id)    # <- suspends
           outcome = project(snapshot, envelope)
           await store.commit(batch)                  # <- suspends

       Run two events for the same instruction concurrently and both read the same snapshot before
       either writes. The second commit overwrites the first, so one event's money is silently
       dropped and one instruction's `last_event_id` skips a value. Nothing raises. The position is
       short by one delta forever, and the counter that would have revealed it — `DUPLICATE` —
       never fires, because from each task's point of view nothing was duplicated.

       This is the concurrency bug that unit-testing a pure function cannot find, which is why
       the lock is here and why a test drives fifty concurrent `ingest` calls and then checks the
       arithmetic.

       The lock is held across the notification too. Releasing it before notifying would let a later
       event's notification overtake an earlier one, so a subscriber could see version 7 and then
       version 6 — and a client that keeps the last version it saw would ignore every subsequent
       update. Ordered delivery is cheaper to guarantee here than to repair downstream.

       The cost, stated plainly: throughput is one event at a time per projector. That is the right
       trade for a projection whose ordering guarantee is why it can be resumed at all, and the
       way to scale it is more projectors partitioned by aggregate id, not more tasks sharing one.
    """

    def __init__(self, store: PositionStore, *, on_change: ChangeListener | None = None) -> None:
        self._store = store
        self._on_change = on_change
        self._lock = asyncio.Lock()
        self.stats = ProjectionStats()

    async def checkpoint(self) -> int:
        """Where a consumer should resume from. Delegated rather than cached.

        Caching it here would create a second copy of the one number whose staleness is
        unrecoverable: if the in-memory copy and the stored copy ever disagree after a failed
        commit, the projection resumes from a position that was never durable.
        """
        return await self._store.checkpoint()

    async def ingest(self, payload: dict[str, Any]) -> Outcome | Refused:
        """Decode one message and apply it, or refuse it.

        The decode happens *outside* the lock, which is worth a sentence: decoding is pure and
        CPU-bound, it cannot touch the store, and holding a lock across it would serialise work that
        does not need serialising. Only the read-decide-write part is exclusive.
        """
        try:
            envelope = decode_envelope(payload)
        except MalformedEventError as exc:
            # `events_seen` is incremented for a refusal even though nothing was applied, because it
            # is the denominator: a refusal rate is only meaningful against everything that arrived.
            self.stats.events_seen += 1
            self.stats.refused += 1
            return Refused(str(exc))
        return await self.apply(envelope)

    async def apply(self, envelope: Envelope) -> Outcome:
        """Apply one decoded event. The whole read-decide-write cycle, under the lock."""
        async with self._lock:
            snapshot = await self._store.snapshot_for(envelope.aggregate_id)
            outcome = project(snapshot, envelope)

            if isinstance(outcome, Skipped):
                # ⚠️ A SKIP STILL COMMITS. This is the line that keeps a poison event from being
                # read forever: the checkpoint moves past an event that changed nothing, so the
                # consumer makes progress. The batch carries no snapshot and no positions, so the
                # commit is a checkpoint advance and nothing else — which is why `WriteBatch` allows
                # that shape rather than requiring a write to justify itself.
                await self._store.commit(WriteBatch(checkpoint=envelope.event_id))
                self._record_seen(envelope)
                self.stats.skips[outcome.reason] += 1
                return outcome

            changed = await self._build_changed_positions(outcome, envelope)
            batch = WriteBatch(
                checkpoint=envelope.event_id,
                snapshot=outcome.snapshot,
                positions=changed,
            )
            await self._store.commit(batch)

            # Everything below this line runs only because the commit returned. See ProjectionStats.
            self._record_seen(envelope)
            self.stats.applied += 1
            self.stats.positions_written += len(changed)
            if outcome.first_seen:
                self.stats.first_seen += 1
            if changed:
                self.stats.changed += 1

            if changed and self._on_change is not None:
                # After the commit, never before. A subscriber that is told about version 7 and then
                # finds version 6 in the store on reconnect has been lied to, and it cannot tell
                # that from a rollback it should care about. Notifying second means the worst case
                # is a missed notification, which reconnect-and-resnapshot repairs; notifying first
                # means published state that never existed, which nothing repairs.
                #
                # An exception from the listener propagates deliberately. The commit already
                # happened, so the consumer's retry re-delivers the event, `project` returns
                # DUPLICATE, and the checkpoint stays where it is — the projection is idempotent,
                # which is what makes "let it fail" a safe answer here instead of a lazy one.
                await self._on_change(changed)

            return outcome

    def _record_seen(self, envelope: Envelope) -> None:
        """The three facts recorded for every event, applied or skipped.

        `last_event_id` uses `max` rather than assignment for the same reason the store clamps its
        checkpoint: an event arriving out of order must not make the watermark go backwards, or a
        dashboard shows the projection losing ground while it is in fact catching up.
        """
        self.stats.events_seen += 1
        self.stats.last_event_id = max(self.stats.last_event_id, envelope.event_id)
        self.stats.last_event_at = envelope.occurred_at

    async def _build_changed_positions(
        self, outcome: Applied, envelope: Envelope
    ) -> tuple[Position, ...]:
        """Fold each change into its position, keeping only the ones whose content actually moved.

        ⚠️ THE `is` COMPARISON IS THE POINT OF THIS METHOD, AND IT IS WHY `with_deltas` RETURNS
        `self`.
           A `validated` event following a `received` produces deltas of `-1500 pending` and
           `+1500 pending`. They net to zero, so `with_deltas` returns the very same object, and the
           identity check drops it from the batch. The consequences of not doing that are not
           cosmetic: the row would be rewritten with an unchanged version, the change listener would
           wake every subscriber for an update containing no visible difference, and slice 4's cache
           would be invalidated for nothing — so a hot position gets evicted and reloaded several
           times per instruction.

           `is` rather than `==` because an equal-but-distinct object would pass an equality check
           while still being a different row to write and a new reference to publish. The model
           promises identity; this is the caller that depends on it.

        The instruction's *snapshot* still changes in that case, and it is still committed. That
        asymmetry — new snapshot, no position write — is the whole reason `WriteBatch.positions`
        defaults to empty.
        """
        changed: list[Position] = []
        for change in outcome.changes:
            # An explicit None check rather than `stored or _empty_position(...)`. The `or` form
            # reads better and relies on every `Position` being truthy — which is true today and
            # would stop being true the moment somebody gave `Position` a `__len__` or a `__bool__`,
            # at which point a real position with four empty buckets would be silently replaced by a
            # fresh one and its version would restart at 1. Not a bug worth leaving available.
            stored = await self._store.position_for(change.key)
            current = _empty_position(change.key) if stored is None else stored
            updated = current.with_deltas(change.deltas, at=envelope.occurred_at)
            if updated is not current:
                changed.append(updated)
        return tuple(changed)


def _empty_position(key: PositionKey) -> Position:
    """A position with four zeroed buckets, version 0 and no `updated_at`.

    A one-line function rather than an inline `Position(key=key)` so there is exactly one place that
    decides what "not seen before" looks like. The version matters: a first sighting produces
    version 1 after `with_deltas`, so version 0 is never published and a client can treat any
    version it receives as a real state. Starting at 1 here would make the first real change
    indistinguishable from the empty position it replaced.
    """
    return Position(key=key)
