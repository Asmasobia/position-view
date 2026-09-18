"""The projector: atomic apply-and-checkpoint, serialised, counted, and idempotent under replay.

WHAT THIS FILE TESTS THAT `test_project.py` CANNOT
---------------------------------------------------------------------------------
`test_project.py` exercises the pure decision — given this snapshot and this event, what does it
mean — across every combination, in milliseconds. Everything left over is in here, and it is exactly
the set of bugs that a unit test of a pure function is structurally incapable of finding:

  - the checkpoint advancing for an event that was not applied, or not advancing for one that was
  - a cross-position move landing as two units of work instead of one
  - two concurrent events reading the same snapshot and one of them being silently lost
  - a counter incremented before the write, so a failing database looks like a busy one
  - a subscriber notified about a version that is not durable yet
  - the whole stream replayed and the answer coming out different

The last one is the headline property of a read model, and it gets a section of its own at the
bottom.

WHY THERE IS NO MOCK IN THIS FILE
---------------------------------------------------------------------------------
`InMemoryPositionStore` is the real shipped implementation and it is what these tests run against,
so every assertion here is about behaviour rather than about calls. There are two doubles, and
neither observes a call — each exists to produce a condition the real store cannot be made to
produce on demand:

  - `_FailingStore`: `commit` raises. Pins what survives a failed write.
  - `_SuspendingStore`: every method yields to the event loop, as a database call does. Pins the
    lock. Necessary because awaiting the in-memory store never suspends, so a projector with the
    lock deleted passes every test that uses it — a hole the mutation matrix found and this double
    closes.

A double that exists to *observe* calls would be asserting on the projector's implementation; these
two assert on properties that only appear under a condition the shipped store cannot exhibit.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest

from builders import (
    AMOUNT,
    COUNTERPARTY,
    INSTRUCTION_ID,
    KEY,
    OCCURRED_AT,
    OTHER_KEY,
    envelope_payload,
    instruction_id,
)
from position_view.model import (
    EVENT_FAILED,
    EVENT_REVERSED,
    EVENT_SETTLED,
    EVENT_VALIDATED,
    Applied,
    Bucket,
    InstructionSnapshot,
    Position,
    PositionKey,
    Skipped,
    SkipReason,
)
from position_view.projection import (
    NO_CHECKPOINT,
    InMemoryPositionStore,
    PositionStore,
    Projector,
    Refused,
    WriteBatch,
)

# See test_store.py for why this is at module level rather than on each function.
pytestmark = pytest.mark.asyncio


class _FailingStore:
    """Reads work, `commit` always raises. Simulates the database being down mid-batch.

    Written out rather than assembled with `unittest.mock` for one reason: a `Mock` whose `commit`
    raises would also silently accept `store.save_position(...)`, `store.begin()` and every other
    method the projector might grow, so a test using it would keep passing through a refactor that
    broke the contract. A hand-written double is checked against `PositionStore` by mypy.
    """

    def __init__(self) -> None:
        self.inner = InMemoryPositionStore()
        self.commit_attempts = 0

    async def checkpoint(self) -> int:
        return await self.inner.checkpoint()

    async def snapshot_for(self, instruction_id: uuid.UUID) -> InstructionSnapshot | None:
        return await self.inner.snapshot_for(instruction_id)

    async def position_for(self, key: PositionKey) -> Position | None:
        return await self.inner.position_for(key)

    async def all_positions(self) -> tuple[Position, ...]:
        return await self.inner.all_positions()

    async def snapshot_count(self) -> int:
        return await self.inner.snapshot_count()

    async def commit(self, batch: WriteBatch) -> None:
        self.commit_attempts += 1
        raise RuntimeError("the database is down")


class _SuspendingStore:
    """A store that yields to the event loop on every call. The only double that can see the lock.

    ⚠️ THIS CLASS EXISTS BECAUSE THE MUTATION MATRIX CAUGHT THE TESTS BELOW BEING WRONG.
       Removing `async with self._lock` from `Projector.apply` left the whole suite green, including
       both concurrency tests, and the reason is a fact about asyncio that is easy to state and easy
       to forget: **awaiting a coroutine is not a suspension point.** `await` on a coroutine that
       never itself awaits anything that blocks runs it start to finish on the current task, with no
       trip through the event loop. `InMemoryPositionStore` is three dict operations and no `await`,
       so `Projector.apply` over it is effectively one synchronous block — there is no interleaving
       to lose an update in, and therefore no way for a test using it to notice the lock is gone.

       Which means the two tests below, run against the dict, were asserting that the *fold* is
       right. Useful, and not what their names claimed. They are kept (a correct fold under
       `gather` is worth pinning) and they now say what they check; the lock's guarantee is pinned
       by the two tests that use this class.

       `asyncio.sleep(0)` is the minimal faithful model of the real dependency: one yield, which is
       what a network round trip costs a task in scheduling terms. Every method suspends, because
       every method of a real store is a query. `commit` suspends *before* mutating, mirroring a
       round trip that reaches the database and then applies the whole transaction — so the batch
       stays atomic while the call as a whole is interruptible, which is exactly the shape the
       projector has to be safe against.
    """

    def __init__(self) -> None:
        self.inner = InMemoryPositionStore()

    async def checkpoint(self) -> int:
        await asyncio.sleep(0)
        return await self.inner.checkpoint()

    async def snapshot_for(self, instruction_id: uuid.UUID) -> InstructionSnapshot | None:
        await asyncio.sleep(0)
        return await self.inner.snapshot_for(instruction_id)

    async def position_for(self, key: PositionKey) -> Position | None:
        await asyncio.sleep(0)
        return await self.inner.position_for(key)

    async def all_positions(self) -> tuple[Position, ...]:
        await asyncio.sleep(0)
        return await self.inner.all_positions()

    async def snapshot_count(self) -> int:
        await asyncio.sleep(0)
        return await self.inner.snapshot_count()

    async def commit(self, batch: WriteBatch) -> None:
        await asyncio.sleep(0)
        await self.inner.commit(batch)


def _projector() -> tuple[Projector, InMemoryPositionStore]:
    """A projector over an empty store. Returned as a pair because most tests assert on both.

    The store is handed back rather than reached for through `projector._store`, because a test that
    reaches into a private attribute is a test that will break when the attribute is renamed for
    reasons that have nothing to do with what it was checking.
    """
    store = InMemoryPositionStore()
    return Projector(store), store


async def _position_at(store: InMemoryPositionStore, key: PositionKey = KEY) -> Position:
    """The position at `key`, asserted to exist. Removes the None check from twenty tests."""
    position = await store.position_for(key)
    assert position is not None, f"expected a position at {key}"
    return position


# ---------------------------------------------------------------------------
# One event
# ---------------------------------------------------------------------------


async def test_a_first_event_creates_the_position_the_snapshot_and_the_checkpoint() -> None:
    """The three writes that have to happen together, checked one at a time.

    Note `version == 1`, not 0: the empty position the fold starts from is never stored, so every
    version a client can observe describes a real state. Publishing version 0 would make "no data
    yet" and "data that has since been zeroed" the same value.
    """
    projector, store = _projector()

    outcome = await projector.ingest(envelope_payload())

    assert isinstance(outcome, Applied)
    assert outcome.first_seen is True

    position = await _position_at(store)
    assert position.pending.amount == Decimal(AMOUNT)
    assert position.pending.count == 1
    assert position.version == 1
    assert position.updated_at == datetime.fromisoformat(OCCURRED_AT)

    snapshot = await store.snapshot_for(INSTRUCTION_ID)
    assert snapshot is not None
    assert snapshot.bucket is Bucket.PENDING
    assert snapshot.last_event_id == 1

    assert await store.checkpoint() == 1


async def test_updated_at_is_event_time_and_not_wall_clock() -> None:
    """⚠️ THE PROPERTY THAT MAKES AN "AS OF" TIMESTAMP MEAN ANYTHING DURING A BACKLOG.

    The builder's `occurred_at` is a fixed literal in the past. If any layer reached for
    `datetime.now()`, this assertion would fail — and in production a projection an hour behind
    would report every position as freshly updated, which is precisely backwards from what an
    operator needs while deciding whether to trust the numbers.

    `test_layering.py` forbids the clock call in `model/`; this checks the result end to end,
    because the projector could have passed its own timestamp in and the layering test would not
    have noticed.
    """
    projector, store = _projector()

    await projector.ingest(envelope_payload(occurred_at="2020-01-01T00:00:00+00:00"))

    position = await _position_at(store)
    assert position.updated_at == datetime.fromisoformat("2020-01-01T00:00:00+00:00")


async def test_the_second_event_is_not_a_first_sighting() -> None:
    projector, store = _projector()

    await projector.ingest(envelope_payload(event_id=1))
    outcome = await projector.ingest(envelope_payload(event_id=2, event_type=EVENT_SETTLED))

    assert isinstance(outcome, Applied)
    assert outcome.first_seen is False
    assert projector.stats.first_seen == 1
    assert await store.snapshot_count() == 1


# ---------------------------------------------------------------------------
# ⚠️ The reason this repository exists, checked through the whole stack
# ---------------------------------------------------------------------------


async def test_an_amended_amount_settles_to_exactly_zero_pending() -> None:
    """⚠️ THE MOST IMPORTANT TEST IN THIS FILE. ADR-007, END TO END, THROUGH A REAL STORE.

    An instruction is received for 1500.75 and later settles for 2000.00 — it was amended in
    between, and the settled event knows nothing about what this read model previously counted.

    The undo half must use the amount in the STORED SNAPSHOT (1500.75) and the redo half the amount
    in the ENVELOPE (2000.00). Then pending returns to exactly zero.

    Use the envelope's amount for both halves — the single most natural way to write this — and
    pending becomes `1500.75 - 2000.00 = -499.25`, forever, in a bucket labelled "money still in
    flight". It is a plausible-looking number, nothing raises, no counter moves, and the only way
    anyone finds it is by reconciling against the write model months later.

    `test_project.py` pins the same rule against the pure function. This pins it against the values
    that were actually persisted and re-read, which is the part a snapshot the store forgot to save
    would break.
    """
    projector, store = _projector()

    await projector.ingest(envelope_payload(event_id=1, amount="1500.75"))
    await projector.ingest(envelope_payload(event_id=2, event_type=EVENT_SETTLED, amount="2000.00"))

    position = await _position_at(store)
    assert position.pending.amount == Decimal("0.00"), (
        "pending must return to exactly zero. -499.25 means the undo half used the event's amount "
        "instead of the stored snapshot's."
    )
    assert position.pending.count == 0
    assert position.settled.amount == Decimal("2000.00")
    assert position.settled.count == 1


async def test_a_corrected_counterparty_moves_both_positions_in_one_commit() -> None:
    """⚠️ THE TWO-ROW CASE. Both positions and the snapshot are one unit of work, or money appears.

    The instruction leaves ACME-BANK and arrives at BETA-TRUST. Commit those separately and a crash
    in between leaves 1500.75 counted at *both* counterparties — a total that is over by exactly one
    instruction, in a report somebody sets a credit limit from.

    The check that this really was one batch is the count invariant at the bottom of this file: the
    sum of bucket counts across all positions equals the number of remembered instructions, which is
    1 here. Two half-applied commits would make it 2 or 0.
    """
    projector, store = _projector()

    await projector.ingest(envelope_payload(event_id=1))
    await projector.ingest(
        envelope_payload(event_id=2, event_type=EVENT_SETTLED, counterparty="BETA-TRUST")
    )

    old = await _position_at(store)
    new = await _position_at(store, OTHER_KEY)

    assert old.pending.amount == Decimal("0.00")
    assert old.pending.count == 0
    assert new.settled.amount == Decimal(AMOUNT)
    assert new.settled.count == 1
    assert projector.stats.positions_written == 3  # one for the open, two for the move
    assert await store.snapshot_count() == 1


# ---------------------------------------------------------------------------
# The no-op transition
# ---------------------------------------------------------------------------


async def test_a_no_op_transition_advances_the_snapshot_and_writes_no_position() -> None:
    """⚠️ WHERE ADR-009's `return self` PAYS FOR ITSELF: A COMMITTED SNAPSHOT AND NO POSITION WRITE.

    `validated` following `received` puts the same money back in the same bucket. The instruction
    has genuinely moved on — its `last_event_id` must advance or the next event is misjudged — but
    no number a client can see is different.

    So the batch carries a snapshot and no positions. Writing the unchanged row anyway would be one
    line shorter and would mean: a rewritten row per workflow step, a subscriber woken for an update
    containing nothing, and slice 4's cache invalidated for nothing, so a hot position is evicted
    and reloaded several times per instruction.
    """
    projector, store = _projector()

    await projector.ingest(envelope_payload(event_id=1))
    before = await _position_at(store)

    outcome = await projector.ingest(envelope_payload(event_id=2, event_type=EVENT_VALIDATED))

    assert isinstance(outcome, Applied)
    after = await _position_at(store)
    assert after is before, "the position row must not be rewritten by a no-op transition"
    assert after.version == 1
    assert after.updated_at == before.updated_at

    snapshot = await store.snapshot_for(INSTRUCTION_ID)
    assert snapshot is not None
    assert snapshot.last_event_id == 2
    assert await store.checkpoint() == 2

    assert projector.stats.applied == 2
    assert projector.stats.changed == 1, "a no-op is applied but changes nothing"
    assert projector.stats.positions_written == 1


# ---------------------------------------------------------------------------
# Skips still commit
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"aggregate_type": "payment"}, SkipReason.NOT_OUR_AGGREGATE),
        ({"event_type": "instruction.archived"}, SkipReason.UNKNOWN_EVENT_TYPE),
    ],
    ids=["other-aggregate", "unknown-event-type"],
)
async def test_a_skipped_event_still_advances_the_checkpoint(
    overrides: dict[str, Any], reason: SkipReason
) -> None:
    """⚠️ THE LINE THAT KEEPS A SHARED TOPIC FROM STALLING THIS SERVICE FOREVER.

    Most traffic on a shared stream is somebody else's. If the checkpoint only moved for events this
    service applied, the consumer would re-read every foreign event on every restart, and a service
    subscribed to a topic it mostly ignores would never make progress at all. The symptom is lag
    growing in a straight line with a completely healthy-looking error rate.
    """
    projector, store = _projector()

    outcome = await projector.ingest(envelope_payload(event_id=11, **overrides))

    assert isinstance(outcome, Skipped)
    assert outcome.reason is reason
    assert await store.checkpoint() == 11
    assert await store.all_positions() == ()
    assert await store.snapshot_count() == 0
    assert projector.stats.skips[reason] == 1
    assert projector.stats.applied == 0


async def test_a_duplicate_delivery_changes_nothing_and_is_counted_as_normal() -> None:
    """At-least-once delivery working as designed. The position must not double.

    The counter matters even though the event is harmless: a *rising* duplicate rate means the
    consumer is dying after committing its batch and before acknowledging the message, which is a
    real problem whose only visible symptom is this number climbing.
    """
    projector, store = _projector()

    await projector.ingest(envelope_payload(event_id=1))
    outcome = await projector.ingest(envelope_payload(event_id=1))

    assert isinstance(outcome, Skipped)
    assert outcome.reason is SkipReason.DUPLICATE

    position = await _position_at(store)
    assert position.pending.count == 1
    assert position.version == 1
    assert projector.stats.skips[SkipReason.DUPLICATE] == 1


async def test_an_older_event_is_out_of_order_and_trips_the_checkpoint_counter() -> None:
    """Two independent alarms fire for one broken guarantee, and both are worth having.

    `OUT_OF_ORDER` says the *instruction* went backwards; `checkpoint_regressions` says the *stream*
    did. They usually move together, and they do not have to: a late event for an instruction nobody
    has seen is applied normally while still moving the global checkpoint backwards, which is the
    case that quietly loses data if the process dies at that moment (see DECISIONS.md).
    """
    projector, store = _projector()

    await projector.ingest(envelope_payload(event_id=9))
    outcome = await projector.ingest(envelope_payload(event_id=3, event_type=EVENT_SETTLED))

    assert isinstance(outcome, Skipped)
    assert outcome.reason is SkipReason.OUT_OF_ORDER
    assert await store.checkpoint() == 9
    assert store.checkpoint_regressions == 1

    position = await _position_at(store)
    assert position.settled.count == 0


async def test_an_illegal_transition_is_recorded_and_the_position_is_untouched() -> None:
    """A settled instruction cannot fail; it can only be reversed. The event is dropped, loudly.

    Dropped rather than raised, because the alternative is a stalled partition over a producer bug
    that a human has to look at anyway. The counter is the loud part.
    """
    projector, store = _projector()

    await projector.ingest(envelope_payload(event_id=1, event_type=EVENT_SETTLED))
    outcome = await projector.ingest(envelope_payload(event_id=2, event_type=EVENT_FAILED))

    assert isinstance(outcome, Skipped)
    assert outcome.reason is SkipReason.ILLEGAL_TRANSITION

    position = await _position_at(store)
    assert position.failed.count == 0
    assert position.settled.count == 1
    assert position.version == 1
    assert await store.checkpoint() == 2, "an illegal transition must not stall the consumer"


# ---------------------------------------------------------------------------
# Messages that cannot be decoded at all
# ---------------------------------------------------------------------------


async def test_a_malformed_message_is_refused_and_does_not_move_the_checkpoint() -> None:
    """⚠️ THE ONE PLACE THIS SERVICE DELIBERATELY REFUSES TO MAKE PROGRESS.

    Everything else advances the checkpoint. A message that could not be decoded cannot, because the
    checkpoint is an `event_id` and the reason the message was refused may be that its `event_id`
    was missing. There is nothing to advance it to.

    That leaves the caller with a genuinely unpleasant choice — acknowledge and lose the message, or
    refuse and stall the partition — and handing it back is more honest than the tidy-looking
    version: catch, log, advance, continue, which is a silent data-loss switch with a reassuring log
    line next to it.
    """
    projector, store = _projector()

    outcome = await projector.ingest(envelope_payload(event_id="not-a-number"))

    assert isinstance(outcome, Refused)
    assert "event_id" in outcome.error
    assert await store.checkpoint() == NO_CHECKPOINT
    assert projector.stats.refused == 1
    assert projector.stats.applied == 0
    assert projector.stats.events_seen == 1, "a refusal is still part of the denominator"


async def test_a_refusal_names_the_field_rather_than_saying_bad_message() -> None:
    """The first question asked about a poison message is what was wrong with it."""
    projector, _ = _projector()

    outcome = await projector.ingest(envelope_payload(currency="US Dollar"))

    assert isinstance(outcome, Refused)
    assert "currency" in outcome.error


# ---------------------------------------------------------------------------
# Counters are recorded after the write, never before
# ---------------------------------------------------------------------------


async def test_a_failed_commit_leaves_no_trace_in_the_store_or_the_counters() -> None:
    """⚠️ THE REASON EVERY COUNTER IS INCREMENTED AFTER `commit` RETURNS.

    Counting on arrival reads more naturally and makes every number wrong in the same direction the
    moment the store starts failing. The consumer retries a failed batch, so an event counted before
    the write is counted once per attempt — and a database wobble shows up as a *rise* in applied
    events. The one metric that would reveal a stalled projection instead reports it as unusually
    busy.

    Note that even `events_seen` stays at zero here, which is the deliberate asymmetry with a
    refusal: a refused message will never be retried, so counting it once is correct; a failed
    commit will be, so counting it at all is double-counting.
    """
    store = _FailingStore()
    projector = Projector(store)

    with pytest.raises(RuntimeError, match=r"database is down"):
        await projector.ingest(envelope_payload())

    assert store.commit_attempts == 1
    assert await store.checkpoint() == NO_CHECKPOINT
    assert await store.all_positions() == ()
    assert projector.stats.events_seen == 0
    assert projector.stats.applied == 0
    assert projector.stats.positions_written == 0


async def test_the_failing_store_double_satisfies_the_real_protocol() -> None:
    """Otherwise the test above proves something about a shape the projector never sees.

    A double that has drifted from the protocol is worse than no double: it keeps passing after the
    contract has changed, so the failure it was written to catch is no longer being checked. The
    annotation is what makes `mypy --strict` police that.
    """
    store: PositionStore = _FailingStore()

    assert await store.checkpoint() == NO_CHECKPOINT


# ---------------------------------------------------------------------------
# Notification
# ---------------------------------------------------------------------------


async def test_the_listener_is_called_after_the_write_is_durable() -> None:
    """⚠️ NOTIFY SECOND. A SUBSCRIBER MUST NEVER SEE A VERSION THAT IS NOT IN THE STORE.

    The listener reads the store while it is being notified, and asserts it can already see the
    position it was handed. Notifying first would be one line's difference and would publish state
    that might never become durable — and a subscriber cannot distinguish that from a rollback it
    should care about.

    The asymmetry is what makes the ordering obvious once stated: notify-second's worst case is a
    *missed* notification, which reconnect-and-resnapshot repairs. Notify-first's worst case is a
    published state that never existed, which nothing repairs.
    """
    store = InMemoryPositionStore()
    seen: list[tuple[Position, ...]] = []

    async def listener(positions: tuple[Position, ...]) -> None:
        for position in positions:
            durable = await store.position_for(position.key)
            assert durable is position, "the listener was called before the commit landed"
        seen.append(positions)

    projector = Projector(store, on_change=listener)
    await projector.ingest(envelope_payload())

    assert len(seen) == 1
    assert seen[0][0].key == KEY


async def test_the_listener_is_not_called_for_a_no_op_transition() -> None:
    """The whole point of `changed` being smaller than `applied`.

    Waking every SSE subscriber for a `validated` event means most pushes on a busy stream carry no
    information a client can act on, and a client that cannot tell an informative update from a
    workflow step it has no visibility into ends up polling anyway.
    """
    store = InMemoryPositionStore()
    calls: list[tuple[Position, ...]] = []

    async def listener(positions: tuple[Position, ...]) -> None:
        calls.append(positions)

    projector = Projector(store, on_change=listener)
    await projector.ingest(envelope_payload(event_id=1))
    await projector.ingest(envelope_payload(event_id=2, event_type=EVENT_VALIDATED))

    assert len(calls) == 1


async def test_the_listener_is_not_called_for_a_skip() -> None:
    store = InMemoryPositionStore()
    calls: list[tuple[Position, ...]] = []

    async def listener(positions: tuple[Position, ...]) -> None:
        calls.append(positions)

    projector = Projector(store, on_change=listener)
    await projector.ingest(envelope_payload(event_id=1, aggregate_type="payment"))

    assert calls == []


async def test_a_listener_failure_propagates_and_the_commit_stands() -> None:
    """⚠️ "LET IT FAIL" IS SAFE HERE *BECAUSE* THE PROJECTION IS IDEMPOTENT, NOT BECAUSE IT IS EASY.

    The commit already happened when the listener raises, so the exception reaches the consumer, the
    consumer retries the message, `project` returns DUPLICATE, and the checkpoint stays where it
    was. Nothing is applied twice and nothing is lost.

    Swallowing the exception instead would be the tempting alternative and would mean a fan-out hub
    that has stopped working produces no signal at all — every subscriber silently stale, every
    projection metric healthy.
    """
    store = InMemoryPositionStore()

    async def listener(positions: tuple[Position, ...]) -> None:
        raise RuntimeError("the hub is broken")

    projector = Projector(store, on_change=listener)

    with pytest.raises(RuntimeError, match=r"hub is broken"):
        await projector.ingest(envelope_payload(event_id=1))

    assert await store.checkpoint() == 1
    assert (await _position_at(store)).version == 1

    # And the retry is a duplicate rather than a second application: the money is not counted twice.
    projector_without_listener = Projector(store)
    outcome = await projector_without_listener.ingest(envelope_payload(event_id=1))
    assert isinstance(outcome, Skipped)
    assert outcome.reason is SkipReason.DUPLICATE
    assert (await _position_at(store)).pending.count == 1


async def test_the_multi_position_notification_carries_both_sides_of_a_move() -> None:
    """A subscriber watching only the old counterparty has to be told the money left.

    One notification per event carrying both positions, rather than one per position, because the
    two changes are one fact — and a subscriber that received them as two independent events would
    briefly hold a state in which the money exists in both places or in neither.
    """
    store = InMemoryPositionStore()
    batches: list[tuple[Position, ...]] = []

    async def listener(positions: tuple[Position, ...]) -> None:
        batches.append(positions)

    projector = Projector(store, on_change=listener)
    await projector.ingest(envelope_payload(event_id=1))
    await projector.ingest(
        envelope_payload(event_id=2, event_type=EVENT_SETTLED, counterparty="BETA-TRUST")
    )

    assert len(batches) == 2
    assert {str(position.key) for position in batches[1]} == {"ACME-BANK/USD", "BETA-TRUST/USD"}


# ---------------------------------------------------------------------------
# ⚠️ Concurrency: the lost update the lock exists to prevent
# ---------------------------------------------------------------------------


def _many_instructions_one_position() -> list[dict[str, Any]]:
    """Fifty payloads: fifty different instructions, all landing on the same position.

    Shared by the dict-backed and the suspending-store versions of the same scenario, so the two
    tests differ in exactly one thing — the store — and the comparison between them is the lesson.

    The amounts are all equal so the expected total is arithmetic a reader can check in their head:
    50 x 10.00 = 500.00. Fifty rather than two because a two-task race can be won by luck, and a
    test that passes by luck half the time is worse than no test.
    """
    return [
        envelope_payload(event_id=index, aggregate_id=str(instruction_id(index)), amount="10.00")
        for index in range(1, 51)
    ]


async def test_fifty_concurrent_events_on_one_position_all_land() -> None:
    """Fifty instructions folded into one position under `gather`, over the in-memory store.

    ⚠️ THIS TEST DOES NOT SEE THE LOCK, AND ITS FIRST VERSION CLAIMED OTHERWISE.
       The claim was that `await store.snapshot_for(...)` is a suspension point, so the tasks
       interleave and the lock is what saves them. That is wrong: awaiting a coroutine which never
       blocks does not yield to the event loop, and `InMemoryPositionStore` never blocks. Every one
       of these fifty `apply` calls therefore runs to completion before the next one starts, lock or
       no lock. The mutation matrix proved it — deleting the lock left this test green.

       What it does check, and what makes it worth keeping: the fold is correct when fifty events
       arrive through `gather` rather than in a `for` loop, i.e. nothing in the projector depends on
       being called from a single call site. `test_the_lock_is_what_makes_a_suspending_store_safe`
       is the one that checks the lock.
    """
    projector, store = _projector()

    await asyncio.gather(
        *(projector.ingest(payload) for payload in _many_instructions_one_position())
    )

    position = await _position_at(store)
    assert position.pending.count == 50
    assert position.pending.amount == Decimal("500.00")
    assert position.version == 50, "every event changed the position, so every event bumped it"
    assert await store.snapshot_count() == 50
    assert projector.stats.applied == 50


async def test_the_lock_is_what_makes_a_suspending_store_safe() -> None:
    """⚠️ THE TEST THAT FAILS THE MOMENT THE LOCK IS REMOVED. THE BUG IT DESCRIBES IS SILENT.

    The same fifty instructions, over a store that yields on every call — which is what a store
    backed by a database does. Applying an event is now read-modify-write with real suspension
    points in the middle:

        snapshot = await store.snapshot_for(id)     # yields
        position = await store.position_for(key)    # yields
        outcome  = project(snapshot, envelope)
        await store.commit(batch)                   # yields

    Without the lock, all fifty tasks read the same empty position before any of them commits, each
    computes "version 1 with my 10.00 in it", and the last commit wins. The position ends up holding
    one instruction instead of fifty. Nothing raises. No counter moves. `DUPLICATE` never fires,
    because from each task's point of view nothing was duplicated. `snapshot_count` is still 50, so
    the projection remembers all fifty instructions while the position shows one — the money is
    simply not there, and the count invariant is the only thing that would ever reveal it.

    The assertion on `snapshot_count` is deliberate: it is what distinguishes "the events were lost"
    from "the events were folded and the position was overwritten". A lost update is the second, and
    the second is the one that reconciles to a number nobody can explain.
    """
    store = _SuspendingStore()
    projector = Projector(store)

    await asyncio.gather(
        *(projector.ingest(payload) for payload in _many_instructions_one_position())
    )

    position = await store.inner.position_for(KEY)
    assert position is not None
    assert position.pending.count == 50, (
        "a lost update dropped an instruction: tasks read the same position across a suspension "
        "point and the last commit won. The lock in Projector.apply is what prevents this."
    )
    assert position.pending.amount == Decimal("500.00")
    assert await store.inner.snapshot_count() == 50


async def test_concurrent_events_on_one_instruction_never_apply_twice() -> None:
    """The same instruction, the same event, fifty times at once. Exactly one may be applied.

    Over the in-memory store, so — like the test three above — this pins the dedup arithmetic under
    `gather` rather than the lock itself. The suspending version below is the one that fails without
    serialisation.
    """
    projector, store = _projector()

    outcomes = await asyncio.gather(*(projector.ingest(envelope_payload()) for _ in range(50)))

    applied = [outcome for outcome in outcomes if isinstance(outcome, Applied)]
    assert len(applied) == 1
    assert (await _position_at(store)).pending.count == 1
    assert projector.stats.skips[SkipReason.DUPLICATE] == 49


async def test_a_duplicate_burst_over_a_suspending_store_still_applies_once() -> None:
    """The missing lock failing in the opposite direction: inflation rather than loss.

    Fifty deliveries of one event, over a store that yields. The dedup check reads the snapshot and
    the commit writes it, so without the lock many tasks read "no snapshot yet", each treats itself
    as a first sighting, and one instruction is counted dozens of times. Same absent lock, same
    silence, and this time the total is too *high* — which is the version that gets noticed, because
    somebody eventually asks why the exposure exceeds the book.
    """
    store = _SuspendingStore()
    projector = Projector(store)

    outcomes = await asyncio.gather(*(projector.ingest(envelope_payload()) for _ in range(50)))

    applied = [outcome for outcome in outcomes if isinstance(outcome, Applied)]
    assert len(applied) == 1, "each extra application is one instruction counted twice"
    position = await store.inner.position_for(KEY)
    assert position is not None
    assert position.pending.count == 1
    assert projector.stats.skips[SkipReason.DUPLICATE] == 49


async def test_the_suspending_store_double_satisfies_the_real_protocol() -> None:
    """Same reason as for `_FailingStore`: a drifted double keeps passing after the contract has
    moved, so the failure it was written to catch stops being checked."""
    store: PositionStore = _SuspendingStore()

    assert await store.checkpoint() == NO_CHECKPOINT


# ---------------------------------------------------------------------------
# Stats and the checkpoint surface
# ---------------------------------------------------------------------------


async def test_the_stats_report_every_skip_reason_even_at_zero() -> None:
    """⚠️ AN ABSENT KEY WOULD MAKE A DASHBOARD PANEL VANISH EXACTLY WHEN ITS SUBJECT STOPS HAPPENING.

    Which is indistinguishable from the panel being broken, and it is the reason `skips` is
    pre-populated with every member rather than being a `defaultdict` that grows on first use.
    """
    projector, _ = _projector()

    reported = projector.stats.as_dict()

    assert set(reported["skipped"]) == {reason.value for reason in SkipReason}
    assert all(count == 0 for count in reported["skipped"].values())


async def test_the_stats_serialise_the_event_watermark_as_a_string() -> None:
    """`last_event_at` is event time, so a client can compare it against the producer's clock."""
    projector, _ = _projector()
    await projector.ingest(envelope_payload(event_id=4))

    reported = projector.stats.as_dict()

    assert reported["last_event_id"] == 4
    assert reported["last_event_at"] == datetime.fromisoformat(OCCURRED_AT).isoformat()


async def test_the_watermark_never_moves_backwards() -> None:
    """Same reason as the store's checkpoint: a dashboard must not show progress being lost.

    An out-of-order event is skipped, but it is still *seen*, and assigning rather than taking the
    max would rewind the reported watermark to the older id — so the graph shows the projection
    going backwards at the exact moment somebody is looking at it to decide whether to fail over.
    """
    projector, _ = _projector()

    await projector.ingest(envelope_payload(event_id=9))
    await projector.ingest(envelope_payload(event_id=3, event_type=EVENT_SETTLED))

    assert projector.stats.last_event_id == 9


async def test_the_projector_reports_the_stores_checkpoint_rather_than_its_own_copy() -> None:
    """Caching it here would make a second copy of the one number whose staleness is unrecoverable.

    If an in-memory copy and the stored copy disagree after a failed commit, the projection resumes
    from a position that was never durable — and replays or skips events accordingly.
    """
    projector, store = _projector()

    await projector.ingest(envelope_payload(event_id=6))

    assert await projector.checkpoint() == await store.checkpoint() == 6


# ---------------------------------------------------------------------------
# ⚠️ Replay: the headline property of a read model
# ---------------------------------------------------------------------------


def _stream() -> list[dict[str, Any]]:
    """A small stream that exercises every path the projector has: ten events, three instructions.

    Hand-written rather than generated. A generated stream would cover more combinations and would
    make the expected end state something a test has to *compute* — at which point the test is
    asserting that two implementations of the same fold agree, which they will, including when both
    are wrong. Ten literal events mean the expected answer can be worked out with a pen.

    In order:

      1. A received, 1500.75 at ACME-BANK/USD          -> first sighting, pending
      2. A validated                                    -> no-op, snapshot advances only
      3. B received, 250.00 at BETA-TRUST/USD           -> first sighting
      4. A settled, 2000.00                             -> AMENDED: undo 1500.75, add 2000.00
      5. a payment event                                -> NOT_OUR_AGGREGATE
      6. B failed                                       -> pending to failed
      7. C received, 99.99 at ACME-BANK/USD             -> first sighting
      8. A reversed, 2000.00                            -> settled to reversed
      9. instruction.archived                           -> UNKNOWN_EVENT_TYPE
     10. C settled, 99.99, currency corrected to GBP    -> CROSS-POSITION MOVE, two rows
    """
    other = str(instruction_id(2))
    third = str(instruction_id(3))
    return [
        envelope_payload(event_id=1, amount="1500.75"),
        envelope_payload(event_id=2, event_type=EVENT_VALIDATED, amount="1500.75"),
        envelope_payload(
            event_id=3, aggregate_id=other, counterparty="BETA-TRUST", amount="250.00"
        ),
        envelope_payload(event_id=4, event_type=EVENT_SETTLED, amount="2000.00"),
        envelope_payload(event_id=5, aggregate_type="payment"),
        envelope_payload(
            event_id=6,
            aggregate_id=other,
            event_type=EVENT_FAILED,
            counterparty="BETA-TRUST",
            amount="250.00",
        ),
        envelope_payload(event_id=7, aggregate_id=third, amount="99.99"),
        envelope_payload(event_id=8, event_type=EVENT_REVERSED, amount="2000.00"),
        envelope_payload(event_id=9, event_type="instruction.archived"),
        envelope_payload(
            event_id=10,
            aggregate_id=third,
            event_type=EVENT_SETTLED,
            currency="GBP",
            amount="99.99",
        ),
    ]


async def _drain(projector: Projector, payloads: list[dict[str, Any]]) -> None:
    """Feed a stream through one payload at a time, which is what a real consumer does.

    Sequential rather than gathered on purpose: the ordering guarantee is a *precondition* of the
    design, so a test of the fold must not be simultaneously testing what happens when it is
    violated. The concurrency tests above do that, separately and deliberately.
    """
    for payload in payloads:
        await projector.ingest(payload)


async def test_the_stream_folds_to_the_state_worked_out_by_hand() -> None:
    """The expected end state, written as literals rather than computed. See `_stream`."""
    projector, store = _projector()

    await _drain(projector, _stream())

    positions = {str(position.key): position for position in await store.all_positions()}
    assert set(positions) == {"ACME-BANK/GBP", "ACME-BANK/USD", "BETA-TRUST/USD"}

    acme_usd = positions["ACME-BANK/USD"]
    assert acme_usd.pending.amount == Decimal("0.00")
    assert acme_usd.pending.count == 0
    assert acme_usd.settled.amount == Decimal("0.00")
    assert acme_usd.settled.count == 0
    assert acme_usd.reversed_.amount == Decimal("2000.00")
    assert acme_usd.reversed_.count == 1

    beta = positions["BETA-TRUST/USD"]
    assert beta.failed.amount == Decimal("250.00")
    assert beta.failed.count == 1
    assert beta.pending.count == 0

    acme_gbp = positions["ACME-BANK/GBP"]
    assert acme_gbp.settled.amount == Decimal("99.99")
    assert acme_gbp.settled.count == 1

    assert projector.stats.events_seen == 10
    assert projector.stats.applied == 8
    assert projector.stats.changed == 7, "event 2 is a no-op"
    assert projector.stats.first_seen == 3
    assert projector.stats.positions_written == 8
    assert projector.stats.skips[SkipReason.NOT_OUR_AGGREGATE] == 1
    assert projector.stats.skips[SkipReason.UNKNOWN_EVENT_TYPE] == 1
    assert await store.checkpoint() == 10


async def test_every_remembered_instruction_is_in_exactly_one_bucket() -> None:
    """⚠️ THE CHEAPEST WHOLE-SYSTEM CORRECTNESS CHECK IN THE SERVICE, AND THE REASON `count` EXISTS.

    Every instruction the projection remembers sits in exactly one bucket of exactly one position.
    So the sum of all bucket counts, across every position, must equal the number of snapshots.

    Any error in the transition arithmetic breaks this equality: a missing undo counts an
    instruction twice, a dropped half of a cross-position move counts it zero times, and a duplicate
    applied twice inflates it. It is one assertion that stands in for a reconciliation job, and it
    is only possible because the counts are stored alongside the amounts rather than derived from
    them.
    """
    projector, store = _projector()

    await _drain(projector, _stream())

    counted = sum(
        position.totals_for(bucket).count
        for position in await store.all_positions()
        for bucket in Bucket
    )

    assert counted == await store.snapshot_count() == 3


async def test_replaying_the_whole_stream_twice_changes_nothing() -> None:
    """⚠️ THE PROPERTY THE ENTIRE DESIGN EXISTS TO PROVIDE: REPLAY IS FREE.

    One store gets the stream once, another gets it twice, and the two hold identical positions.
    That is what makes a crash mid-batch survivable, a redelivery harmless, and a rebuild a matter
    of pointing the projector at an empty store.

    Compared position-by-position rather than with a single tuple equality, so a failure names
    *which* position drifted and by how much. A bare `assert a == b` on two tuples of frozen
    dataclasses produces a diff that has to be read sideways.

    The comparison includes `version`, which is stricter than it looks and is the assertion that
    would catch a re-application that happened to net to zero: the numbers would match and the
    version would not.
    """
    once, once_store = _projector()
    twice, twice_store = _projector()
    await _drain(once, _stream())
    await _drain(twice, _stream())
    await _drain(twice, _stream())

    first = await once_store.all_positions()
    second = await twice_store.all_positions()

    assert [str(position.key) for position in first] == [str(position.key) for position in second]
    for expected, actual in zip(first, second, strict=True):
        assert actual == expected, f"{expected.key} drifted on replay"

    assert await once_store.snapshot_count() == await twice_store.snapshot_count()


async def test_the_second_pass_applies_nothing_at_all() -> None:
    """The stronger half of idempotence: not merely the same answer, but no work done to get there.

    Two stores agreeing could in principle happen through compensating mistakes. Zero applications
    and zero position writes cannot.

    ⚠️ AND THE SKIP REASONS ARE NOT ALL `DUPLICATE`, WHICH IS WORTH KNOWING BEFORE ALERTING ON THEM.
       Only the *last* event for each instruction matches its stored `last_event_id`; earlier ones
       are strictly lower, so they come back as `OUT_OF_ORDER` — three instructions give three
       duplicates and five out-of-order. That is correct behaviour and it means a full replay into a
       populated store lights up the counter that is supposed to sit at zero. So a rebuild replays
       into an EMPTY store, which is why `PositionStore` has no `truncate`: you point the projector
       at a new store rather than editing the one you have.
    """
    projector, store = _projector()
    await _drain(projector, _stream())
    before = await store.all_positions()

    applied_after_first_pass = projector.stats.applied
    written_after_first_pass = projector.stats.positions_written

    await _drain(projector, _stream())

    assert projector.stats.applied == applied_after_first_pass
    assert projector.stats.positions_written == written_after_first_pass
    assert await store.all_positions() == before
    assert projector.stats.events_seen == 20

    assert projector.stats.skips[SkipReason.DUPLICATE] == 3
    assert projector.stats.skips[SkipReason.OUT_OF_ORDER] == 5
    assert projector.stats.skips[SkipReason.ILLEGAL_TRANSITION] == 0
    assert store.checkpoint_regressions == 9, "events 1 to 9 all arrive behind a checkpoint of 10"


async def test_the_order_two_unrelated_instructions_arrive_in_does_not_matter() -> None:
    """Different instructions touch disjoint snapshots, so their relative order is irrelevant.

    Worth pinning because it is the property that makes partitioning by aggregate id a valid way to
    scale this: if the answer depended on the interleaving of unrelated instructions, one projector
    per partition would produce a different total than one projector for everything.

    Note what this does *not* claim. Reordering events *within* one instruction absolutely does
    matter, and the `OUT_OF_ORDER` counter exists because it does.
    """
    forwards, forwards_store = _projector()
    backwards, backwards_store = _projector()

    other = str(instruction_id(2))
    a_events = [
        envelope_payload(event_id=1, amount="100.00"),
        envelope_payload(event_id=3, event_type=EVENT_SETTLED, amount="100.00"),
    ]
    b_events = [
        envelope_payload(event_id=2, aggregate_id=other, amount="7.50"),
        envelope_payload(event_id=4, aggregate_id=other, event_type=EVENT_FAILED, amount="7.50"),
    ]

    await _drain(forwards, [a_events[0], b_events[0], a_events[1], b_events[1]])
    await _drain(backwards, [*b_events, *a_events])

    assert await forwards_store.all_positions() == await backwards_store.all_positions()


async def test_the_counterparty_reaching_the_store_is_the_canonical_one() -> None:
    """Whitespace and case are folded at the boundary, so the position key cannot fork.

    `" ACME-BANK "` and `"acme-bank"`... only the first is exercised here, because the decoder
    upper-cases the currency but deliberately does *not* upper-case the counterparty — an identifier
    whose case is significant is a real possibility and folding it would merge two counterparties.
    Trimming is safe; case-folding is not, and the asymmetry is intentional rather than an
    oversight.
    """
    projector, store = _projector()

    await projector.ingest(envelope_payload(counterparty=f"  {COUNTERPARTY}  "))

    assert [str(position.key) for position in await store.all_positions()] == ["ACME-BANK/USD"]
