"""Tests for the fan-out hub: conflation, the handover, the bound, and the no-blocking guarantee.

WHAT THIS FILE IS ACTUALLY TRYING TO PIN
---------------------------------------------------------------------------------
Four claims, in descending order of how much damage their failure would do:

  1. **A slow subscriber cannot slow the projection.** If this breaks, one suspended browser tab
     freezes the entire read model. Pinned structurally (`publish` contains no `await`, checked by
     parsing the source) and behaviourally (fifty events land while nobody is reading).
  2. **A subscriber sees every version of a position exactly once, in order.** Pinned across the
     snapshot-to-stream handover, which is where every push API has its off-by-one race.
  3. **A subscriber's memory is bounded**, and when the bound is hit it is told rather than quietly
     given a subset.
  4. **Conflation collapses superseded state rather than queueing it**, so volume alone cannot make
     a subscriber fall behind.

WHY THESE TESTS BUILD `Position` DIRECTLY INSTEAD OF DRIVING THE PROJECTOR
---------------------------------------------------------------------------------
The hub does not care what a position *means*. It reads `key` and `version` and nothing else.
Driving a projector to produce a version-3 position would take three events and would make a hub
failure look like a projection failure in the traceback. So most tests here build positions with
`_at_version`, and one section at the end wires the real projector in — because "the seam actually
fits" is a separate claim from "the buffer conflates", and each is easier to trust when it
fails on its own.

WHY ALMOST NOTHING HERE SLEEPS
---------------------------------------------------------------------------------
`asyncio.sleep(0)` appears, `asyncio.sleep(0.1)` does not. A test that waits a tenth of a second to
decide whether a task is blocked is both slow and a liar: it passes on a fast machine and fails on a
loaded CI box, and the fix is always to raise the number. `sleep(0)` yields exactly one scheduling
round, which is a definite amount of progress rather than a hopeful one, so "still pending after a
yield" is a fact rather than a guess.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from builders import AMOUNT, KEY, OTHER_KEY, envelope_payload, instruction_id
from position_view.model import EVENT_SETTLED, Bucket, Position, PositionKey, Totals
from position_view.projection import (
    ChangeListener,
    InMemoryPositionStore,
    Projector,
)
from position_view.push import (
    DEFAULT_MAX_PENDING,
    NOTHING_DELIVERED,
    SubscriberOverflow,
    Subscription,
    SubscriptionClosed,
    SubscriptionHub,
)

pytestmark = pytest.mark.asyncio

THIRD_KEY = PositionKey(counterparty="ZULU-CORP", currency="USD")


def _hub_source() -> str:
    """The hub's own source text, for the tests that assert on structure rather than behaviour.

    Located through `inspect` rather than by a hand-written relative path, so moving the file breaks
    the import (loud) instead of the lookup (which would silently parse nothing).
    """
    path = inspect.getsourcefile(SubscriptionHub)
    assert path is not None, "SubscriptionHub has no source file"
    return Path(path).read_text(encoding="utf-8")


_HUB_SOURCE = _hub_source()


def _at_version(key: PositionKey, version: int, *, amount: str = "10.00") -> Position:
    """A position at a given version. The only two fields the hub reads, plus one for legibility.

    `pending` is filled in even though no assertion here depends on it, so that a failure message
    printing a `Position` shows something distinguishable rather than four identical zero buckets.
    """
    return Position(
        key=key,
        pending=Totals(amount=Decimal(amount), count=1),
        version=version,
    )


def _drain_all(subscription: Subscription) -> list[Position]:
    """Everything currently buffered, flattened. For tests that care about content, not batching."""
    return list(subscription.drain_nowait())


# -------------------------------------------------------------------------------------------------
# 1. The guarantee the whole design exists for: the projection is never blocked or interleaved.
# -------------------------------------------------------------------------------------------------


async def test_publish_never_suspends() -> None:
    """⚠️ THE MOST IMPORTANT TEST IN THIS FILE. `publish` runs under the projector's lock.

    An `await` here is an `await` holding the lock that serialises the entire projection, so a
    single subscriber on a slow connection stops the read model for everyone. The failure is
    invisible in normal testing: every test in this file would still pass with an `await
    subscriber.send(...)` in the loop, because in-process doubles never actually block.

    So this is checked structurally. Parsed rather than substring-searched, because the word `await`
    appears in this module's own prose — including in this docstring — and a `"await" in source`
    check would be defeated by the comment that explains it.
    """
    tree = ast.parse(_HUB_SOURCE)

    publishes = [
        node
        for klass in ast.walk(tree)
        if isinstance(klass, ast.ClassDef) and klass.name == "SubscriptionHub"
        for node in klass.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "publish"
    ]
    # Vacuity guard: a rename would otherwise leave an empty list and a test that passes by checking
    # nothing at all.
    assert len(publishes) == 1, "SubscriptionHub.publish not found — has it been renamed?"

    suspensions = [node for node in ast.walk(publishes[0]) if isinstance(node, ast.Await)]
    assert suspensions == [], (
        "SubscriptionHub.publish now suspends. It is called under the projector's lock, so this "
        "makes one slow subscriber able to stall the whole projection. Move the I/O onto the "
        "subscriber's own task, behind its buffer."
    )


async def test_offer_and_subscribe_and_unsubscribe_are_all_synchronous() -> None:
    """The three methods `publish` and a connection handler race on must not be coroutines.

    `publish` cannot suspend, so nothing it calls may be a coroutine — that is already covered
    above. The reason to state it for `subscribe` too: an `async def subscribe` would give a caller
    no way to know whether a change published during its own await was captured, which reopens the
    exact gap `mark_delivered` exists to close.
    """
    for method in (Subscription.offer, SubscriptionHub.subscribe, SubscriptionHub.unsubscribe):
        assert not inspect.iscoroutinefunction(method), (
            f"{method.__qualname__} became a coroutine; the publish path can no longer call it "
            f"without suspending"
        )


async def test_publishing_to_nobody_is_harmless_and_still_counted() -> None:
    """The common case at startup, and after the last client disconnects.

    Counted anyway, because `published` with zero `subscribers` is a useful diagnostic: it says the
    projection is alive and nobody is listening, which distinguishes "the stream is dead" from
    "the UI never connected".
    """
    hub = SubscriptionHub()

    await hub.publish((_at_version(KEY, 1),))

    assert hub.subscribers == 0
    assert hub.published == 1
    assert hub.overflow_disconnects == 0


async def test_a_subscriber_that_never_reads_does_not_stop_the_projection() -> None:
    """The whole point of the slice, stated as behaviour rather than as structure.

    Fifty events for one position, a subscriber registered and never drained. Every event must apply
    and the checkpoint must reach the end — and the subscriber's memory must be one position, not
    fifty. If the fan-out awaited the subscriber, or buffered per event instead of per position, one
    of those two numbers would be wrong.
    """
    store = InMemoryPositionStore()
    hub = SubscriptionHub()
    projector = Projector(store, on_change=hub.publish)
    subscription = hub.subscribe()

    for index in range(1, 51):
        await projector.ingest(
            envelope_payload(
                event_id=index,
                aggregate_id=str(instruction_id(index)),
                amount="10.00",
            )
        )

    assert projector.stats.applied == 50
    assert await projector.checkpoint() == 50
    assert subscription.pending_count == 1, (
        "the subscriber buffered one entry per event instead of one per position; conflation is "
        "not happening and its memory now grows with the event rate"
    )
    assert subscription.conflated == 49
    assert not subscription.overflowed


# -------------------------------------------------------------------------------------------------
# 2. Conflation: the buffer holds state, so a newer version replaces an older one.
# -------------------------------------------------------------------------------------------------


async def test_a_newer_version_replaces_a_buffered_older_one() -> None:
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 1),))
    await hub.publish((_at_version(KEY, 2),))

    assert subscription.pending_count == 1
    assert subscription.conflated == 1
    delivered = _drain_all(subscription)
    assert [position.version for position in delivered] == [2], (
        "the subscriber was sent a superseded version; the buffer is queueing, not conflating"
    )
    # A drain empties the buffer. Worth asserting separately from the content, because a drain that
    # read the buffer without clearing it would deliver the right thing once and then hold every
    # position it has ever seen for the life of the connection — a leak that no content assertion
    # notices, since the version watermark quietly suppresses the re-reads.
    assert subscription.pending_count == 0


async def test_different_positions_do_not_conflate_with_each_other() -> None:
    """The vacuity guard for the test above: conflation must be per key, not global.

    A `_pending` that was a single slot rather than a dict would pass every conflation test and lose
    every position but the last. This is the test that fails.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 1), _at_version(OTHER_KEY, 1)))

    assert subscription.pending_count == 2
    assert subscription.conflated == 0
    assert {position.key for position in _drain_all(subscription)} == {KEY, OTHER_KEY}


async def test_a_thousand_updates_to_one_position_stay_bounded_at_one_entry() -> None:
    """The bound that matters: a subscriber's memory is a function of keys watched, not event rate.

    A thousand publishes against a default `max_pending` of 256 would overflow immediately if the
    buffer counted updates. It does not overflow, because it counts positions.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    for version in range(1, 1001):
        await hub.publish((_at_version(KEY, version),))

    assert subscription.pending_count == 1
    assert subscription.offered == 1000
    assert subscription.conflated == 999
    assert not subscription.overflowed
    assert [position.version for position in _drain_all(subscription)] == [1000]


async def test_a_multi_position_change_is_delivered_as_one_batch() -> None:
    """A cross-position move (ADR-019) must not be split across two deliveries.

    Split, there is a moment where a client has applied the debit and not the credit, so the money
    exists in neither position — or, if the order is the other way, in both. Either is a screenshot
    somebody escalates. One batch means the client applies both or neither.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 4), _at_version(OTHER_KEY, 1)))

    batch = subscription.drain_nowait()
    assert len(batch) == 2, "the two halves of a move arrived in separate deliveries"
    assert subscription.batches_delivered == 1
    assert subscription.positions_delivered == 2


async def test_a_batch_is_ordered_by_key() -> None:
    """Stable order, so a client rendering a table does not have rows jump for no visible reason.

    Offered in reverse to make the assertion mean something: an implementation that simply preserved
    insertion order would return `ZULU, BETA, ACME` and fail here.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    await hub.publish(
        (_at_version(THIRD_KEY, 1), _at_version(OTHER_KEY, 1), _at_version(KEY, 1)),
    )

    delivered = subscription.drain_nowait()
    assert [str(position.key) for position in delivered] == sorted(
        str(key) for key in (KEY, OTHER_KEY, THIRD_KEY)
    )


# -------------------------------------------------------------------------------------------------
# 3. Versions are delivered once, and never go backwards.
# -------------------------------------------------------------------------------------------------


async def test_a_version_already_delivered_is_not_delivered_again() -> None:
    """A late publish for an old version must be dropped, not shown to the client as news.

    Reachable in ordinary operation: a rebuild running alongside a live stream, or a listener wired
    to two projectors. The client's rule is "apply what arrives", so a stale arrival would move its
    view backwards.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 2),))
    assert len(subscription.drain_nowait()) == 1

    await hub.publish((_at_version(KEY, 1),))

    assert subscription.drain_nowait() == ()
    assert subscription.suppressed == 1


async def test_the_same_version_twice_is_suppressed() -> None:
    """The boundary case, which is the one a `<` instead of `<=` would get wrong.

    Equal versions mean equal content (ADR-009), so a redelivery is not news. Sending it would make
    every duplicate event in an at-least-once stream produce a client-visible update.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 3),))
    subscription.drain_nowait()
    await hub.publish((_at_version(KEY, 3),))

    assert subscription.drain_nowait() == ()
    assert subscription.suppressed == 1


async def test_a_strictly_newer_version_is_delivered() -> None:
    """The vacuity guard for the two above: suppression must not be suppressing everything.

    Without this, an implementation that dropped every position after the first would pass both
    preceding tests and deliver nothing for the life of the connection.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 3),))
    subscription.drain_nowait()
    await hub.publish((_at_version(KEY, 4),))

    assert [position.version for position in subscription.drain_nowait()] == [4]
    assert subscription.suppressed == 0


async def test_the_delivered_watermark_starts_at_the_documented_sentinel() -> None:
    """A brand-new subscription must deliver version 1, so the sentinel has to be below it.

    Pinned because the sentinel and the first real version are one apart: `NOTHING_DELIVERED = 1`
    would silently swallow the first update for every position, which looks like "the first event
    after connecting is always missed" and is very hard to attribute.
    """
    assert NOTHING_DELIVERED == 0

    hub = SubscriptionHub()
    subscription = hub.subscribe()
    await hub.publish((_at_version(KEY, 1),))

    assert [position.version for position in subscription.drain_nowait()] == [1]


async def test_mark_delivered_never_moves_the_watermark_backwards() -> None:
    """`max`, not assignment. Otherwise a stale snapshot re-opens a version already sent.

    The sequence is reachable: two overlapping snapshot reads, the older one completing second.
    Assignment would lower the watermark and the next publish would resend a version the client had
    already applied.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    subscription.mark_delivered([_at_version(KEY, 5)])
    subscription.mark_delivered([_at_version(KEY, 2)])

    await hub.publish((_at_version(KEY, 3),))

    assert subscription.drain_nowait() == ()
    assert subscription.suppressed == 1


# -------------------------------------------------------------------------------------------------
# 4. The snapshot-then-stream handover: no gap, no duplicate.
# -------------------------------------------------------------------------------------------------


async def test_the_handover_has_neither_a_gap_nor_a_duplicate() -> None:
    """The full sequence a connection handler runs, in the order it must run it.

    subscribe (buffering starts) -> read the snapshot -> `mark_delivered(snapshot)` -> stream.

    The change published between the subscribe and the snapshot read is the one that a
    snapshot-then-subscribe implementation loses forever. Here it is buffered, recognised as already
    covered by the snapshot, and dropped — and the change *after* the snapshot still arrives.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe(keys=[KEY])

    # A change lands while the handler is still reading its snapshot.
    await hub.publish((_at_version(KEY, 1),))

    # The snapshot the client is sent happens to include that change, because the store is ahead of
    # the buffer — which is exactly why it must not also be streamed.
    snapshot = (_at_version(KEY, 1),)
    subscription.mark_delivered(snapshot)

    assert subscription.drain_nowait() == (), "the client was sent a version its snapshot contained"
    assert subscription.suppressed == 1
    # A drain that delivered nothing is not a delivery. The count matters because slice 5 uses it to
    # decide whether to write an SSE frame, and an empty frame per suppressed update is a heartbeat
    # the client cannot distinguish from real news.
    assert subscription.batches_delivered == 0

    # And the stream still works afterwards: this is not a subscription that has been muted.
    await hub.publish((_at_version(KEY, 2),))
    assert [position.version for position in subscription.drain_nowait()] == [2]
    assert subscription.batches_delivered == 1


async def test_without_mark_delivered_the_handover_duplicates() -> None:
    """The negative control. Proves `mark_delivered` is load-bearing rather than decorative.

    Same sequence, minus the one call. Version 1 is delivered a second time, which is precisely the
    duplicate the handover exists to prevent. Written as a test so that deleting the call from
    `hub.py` cannot pass the suite by making this file's other assertions vacuous.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe(keys=[KEY])

    await hub.publish((_at_version(KEY, 1),))

    assert [position.version for position in subscription.drain_nowait()] == [1]
    assert subscription.suppressed == 0


async def test_a_change_after_the_snapshot_is_never_lost() -> None:
    """The gap case: the snapshot is older than what the buffer holds, and the newer state wins.

    This is the failure mode of snapshot-then-subscribe. The client's view of this position would
    stay at version 1 forever with nothing to indicate it — and "nothing outstanding for this
    counterparty" is a perfectly plausible screen.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe(keys=[KEY])

    await hub.publish((_at_version(KEY, 7),))
    subscription.mark_delivered([_at_version(KEY, 6)])

    assert [position.version for position in subscription.drain_nowait()] == [7]


# -------------------------------------------------------------------------------------------------
# 5. Key filtering.
# -------------------------------------------------------------------------------------------------


async def test_a_filtered_subscriber_ignores_positions_it_did_not_ask_for() -> None:
    hub = SubscriptionHub()
    subscription = hub.subscribe(keys=[KEY])

    await hub.publish((_at_version(KEY, 1), _at_version(OTHER_KEY, 1)))

    delivered = _drain_all(subscription)
    assert [position.key for position in delivered] == [KEY]
    assert subscription.offered == 1, "a filtered-out position was counted as offered"


async def test_a_wildcard_subscriber_receives_every_position() -> None:
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 1), _at_version(OTHER_KEY, 1), _at_version(THIRD_KEY, 1)))

    assert subscription.watches_everything
    assert len(subscription.drain_nowait()) == 3


async def test_an_empty_key_set_receives_nothing_rather_than_everything() -> None:
    """⚠️ `keys=[]` means "no positions", not "all positions". The distinction is a safety property.

    An empty list is what a caller produces by *accident* — a filter that matched nothing, a query
    parameter that parsed to an empty list. Treating that as a wildcard would stream an entire book
    to a client that asked for nothing, which is both a data exposure and the one case that can
    overflow. `None` is the explicit way to ask for everything, and cannot be produced by accident.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe(keys=[])

    await hub.publish((_at_version(KEY, 1), _at_version(OTHER_KEY, 1)))

    assert not subscription.watches_everything
    assert subscription.drain_nowait() == ()
    assert subscription.offered == 0


async def test_watches_answers_for_both_kinds_of_subscriber() -> None:
    hub = SubscriptionHub()

    filtered = hub.subscribe(keys=[KEY, OTHER_KEY])
    everything = hub.subscribe()

    assert filtered.watches(KEY)
    assert filtered.watches(OTHER_KEY)
    assert not filtered.watches(THIRD_KEY)
    assert everything.watches(THIRD_KEY)


# -------------------------------------------------------------------------------------------------
# 6. The bound, and what happens when it is reached.
# -------------------------------------------------------------------------------------------------


async def test_a_subscriber_watching_a_few_keys_can_never_overflow() -> None:
    """Bounded by construction, not by luck. Worth its own test because it is the common case.

    A subscriber watching N keys with `max_pending >= N` cannot overflow however long it stops
    reading, because conflation caps its buffer at N. So the overflow path below is not something a
    normal dashboard client can ever hit — which is what makes disconnecting on it an acceptable
    response.
    """
    hub = SubscriptionHub(max_pending=2)
    subscription = hub.subscribe(keys=[KEY, OTHER_KEY])

    for version in range(1, 201):
        await hub.publish(
            (
                _at_version(KEY, version),
                _at_version(OTHER_KEY, version),
                _at_version(THIRD_KEY, version),
            )
        )

    assert not subscription.overflowed
    assert subscription.pending_count == 2


async def test_the_bound_is_inclusive() -> None:
    """Exactly `max_pending` distinct positions is fine; the check fires on the one after.

    Pinned because `>` and `>=` are one keystroke apart and the difference is a subscriber cut off
    for doing exactly what it was told it could do.
    """
    hub = SubscriptionHub(max_pending=3)
    subscription = hub.subscribe()

    await hub.publish(
        (_at_version(KEY, 1), _at_version(OTHER_KEY, 1), _at_version(THIRD_KEY, 1)),
    )

    assert not subscription.overflowed
    assert subscription.pending_count == 3


async def test_a_wildcard_subscriber_that_stops_reading_eventually_overflows() -> None:
    """The one path that can overflow, and the buffer is discarded in full when it does.

    Discarded rather than trimmed: a trimmed buffer is delivered as though it were complete, so the
    client's view of the dropped positions freezes at an old version with no indication. A client
    that knows it is wrong re-snapshots and is right again. A client that does not, does not.
    """
    hub = SubscriptionHub(max_pending=3)
    subscription = hub.subscribe()

    for index in range(1, 5):
        key = PositionKey(counterparty=f"CP-{index}", currency="USD")
        await hub.publish((_at_version(key, 1),))

    assert subscription.overflowed
    assert subscription.pending_count == 0, (
        "a partial buffer survived and would be delivered as though it were complete"
    )


async def test_an_overflowed_drain_raises_rather_than_returning_a_subset() -> None:
    hub = SubscriptionHub(max_pending=1)
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 1), _at_version(OTHER_KEY, 1)))

    with pytest.raises(SubscriberOverflow, match="re-snapshot"):
        subscription.drain_nowait()


async def test_an_overflow_is_permanent_and_counted_once() -> None:
    """Overflowed is a terminal state, not a throttle, and the hub counts the transition only once.

    Terminal because the subscription has already lost data: letting it resume would give the client
    a stream with a hole in it and no marker where the hole is. Counted once so that
    `overflow_disconnects` is a count of affected clients rather than of subsequent publishes.
    """
    hub = SubscriptionHub(max_pending=1)
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 1), _at_version(OTHER_KEY, 1)))
    assert hub.overflow_disconnects == 1

    for version in range(2, 12):
        await hub.publish((_at_version(KEY, version),))

    assert hub.overflow_disconnects == 1
    assert subscription.overflowed
    assert subscription.pending_count == 0, "an overflowed subscription started buffering again"


async def test_one_subscriber_overflowing_leaves_the_others_untouched() -> None:
    """Isolation is the reason the buffer is per subscriber rather than one shared queue.

    The wildcard subscriber here is the misbehaving one. The filtered subscriber is watching a
    single counterparty and must be entirely unaffected — same hub, same publishes, no shared state
    but the subscriber list.
    """
    hub = SubscriptionHub(max_pending=2)
    greedy = hub.subscribe()
    careful = hub.subscribe(keys=[KEY])

    for index in range(1, 6):
        key = PositionKey(counterparty=f"CP-{index}", currency="USD")
        await hub.publish((_at_version(key, 1), _at_version(KEY, index)))

    assert greedy.overflowed
    assert not careful.overflowed
    assert [position.version for position in careful.drain_nowait()] == [5]


async def test_publish_does_not_raise_when_a_subscriber_has_overflowed() -> None:
    """The projection must not learn that a client fell behind; it can do nothing useful about it.

    If `publish` raised here it would propagate into `Projector.apply` — which deliberately lets a
    listener exception through (ADR-027) — and a client's slow connection would become a projection
    failure. The client's problem stays the client's problem.
    """
    hub = SubscriptionHub(max_pending=1)
    hub.subscribe()

    await hub.publish((_at_version(KEY, 1), _at_version(OTHER_KEY, 1)))
    await hub.publish((_at_version(THIRD_KEY, 1),))

    assert hub.published == 3


async def test_a_max_pending_below_one_is_refused() -> None:
    """Zero would make a subscriber overflow on its first position, which is not a useful config.

    Refused at construction rather than clamped, because clamping would turn a typo in a config file
    into a service that quietly disconnects every client and logs nothing but overflows.
    """
    with pytest.raises(ValueError, match="at least 1"):
        Subscription(max_pending=0)
    with pytest.raises(ValueError, match="at least 1"):
        Subscription(max_pending=-5)


async def test_the_default_bound_is_the_documented_constant() -> None:
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    for index in range(DEFAULT_MAX_PENDING):
        key = PositionKey(counterparty=f"CP-{index:04d}", currency="USD")
        await hub.publish((_at_version(key, 1),))

    assert subscription.pending_count == DEFAULT_MAX_PENDING
    assert not subscription.overflowed


# -------------------------------------------------------------------------------------------------
# 7. Waiting, waking, and ending.
# -------------------------------------------------------------------------------------------------


async def test_drain_waits_until_something_arrives() -> None:
    """The blocking read: pending after a scheduling round, resolved by a publish.

    `sleep(0)` rather than a timeout, so the "still waiting" assertion is about scheduling rather
    than about wall-clock luck. One yield is enough: if `drain` were going to return immediately it
    would have done so by now.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    reader = asyncio.create_task(subscription.drain())
    await asyncio.sleep(0)
    assert not reader.done(), "drain returned with an empty buffer"

    await hub.publish((_at_version(KEY, 1),))
    batch = await reader

    assert [position.version for position in batch] == [1]


async def test_drain_never_returns_an_empty_batch() -> None:
    """A wakeup with nothing new must keep waiting, which is why `drain` loops.

    Reachable through the handover: the doorbell is rung by a publish whose version the snapshot
    already covered. Without the loop, `drain` returns `()` and the caller's `async for` writes an
    empty event down a socket — or, worse, treats it as end-of-stream.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()
    subscription.mark_delivered([_at_version(KEY, 5)])

    reader = asyncio.create_task(subscription.drain())
    await asyncio.sleep(0)

    await hub.publish((_at_version(KEY, 5),))
    await asyncio.sleep(0)
    assert not reader.done(), "drain resolved with a batch containing nothing new"
    assert subscription.suppressed == 1

    await hub.publish((_at_version(KEY, 6),))
    assert [position.version for position in await reader] == [6]


async def test_closing_wakes_a_waiting_reader() -> None:
    """Otherwise a disconnect leaves a task parked on an `Event` that will never be set again.

    Parked tasks are the kind of leak that only shows up under load: nothing errors, memory climbs,
    and the process holds a `Subscription` alive for a client that left hours ago.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    reader = asyncio.create_task(subscription.drain())
    await asyncio.sleep(0)

    hub.unsubscribe(subscription)

    with pytest.raises(SubscriptionClosed):
        await reader


async def test_closing_is_idempotent() -> None:
    """Two callers race by nature: the connection handler on disconnect, the hub on shutdown.

    Making the second call an error would turn every ordinary shutdown into a logged exception, and
    the logged exception would be in a `finally` block, where it would mask the real one.
    """
    subscription = Subscription()

    subscription.close()
    subscription.close()

    assert subscription.is_closed


async def test_a_closed_subscription_refuses_a_drain() -> None:
    """`()` would be indistinguishable from "nothing new", so a poller would spin forever."""
    subscription = Subscription()
    subscription.close()

    with pytest.raises(SubscriptionClosed):
        subscription.drain_nowait()


async def test_offering_to_a_closed_subscription_is_a_no_op_not_an_error() -> None:
    """A publish and a disconnect race, and there is no ordering between them worth enforcing.

    Raising here would mean the projection sees an exception every time a client closes a tab at an
    unlucky moment — a genuine error report for an entirely normal event.
    """
    subscription = Subscription()
    subscription.close()

    subscription.offer(_at_version(KEY, 1))

    assert subscription.pending_count == 0
    assert subscription.offered == 0


async def test_updates_yields_batches_and_ends_cleanly_on_close() -> None:
    """The shape slice 5's SSE endpoint consumes: an iterator that terminates on disconnect."""
    hub = SubscriptionHub()
    subscription = hub.subscribe()
    seen: list[int] = []

    async def consume() -> None:
        async for batch in subscription.updates():
            seen.extend(position.version for position in batch)

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)

    await hub.publish((_at_version(KEY, 1),))
    await asyncio.sleep(0)
    await hub.publish((_at_version(KEY, 2),))
    await asyncio.sleep(0)

    hub.unsubscribe(subscription)
    await consumer

    assert seen == [1, 2]
    assert consumer.exception() is None


async def test_updates_propagates_an_overflow_instead_of_ending_quietly() -> None:
    """⚠️ The distinction that keeps a data-loss event from looking like a normal disconnect.

    Both "the client left" and "the client lost data" end the iteration. If both did it by
    returning, the handler could not tell them apart, and the only correct response to the second —
    tell the client to re-snapshot — would never happen. One returns, the other raises.
    """
    hub = SubscriptionHub(max_pending=1)
    subscription = hub.subscribe()

    async def consume() -> None:
        async for _ in subscription.updates():
            pass

    consumer = asyncio.create_task(consume())
    await asyncio.sleep(0)

    await hub.publish((_at_version(KEY, 1), _at_version(OTHER_KEY, 1)))

    with pytest.raises(SubscriberOverflow):
        await consumer


# -------------------------------------------------------------------------------------------------
# 8. Hub lifecycle: registration, removal, and not leaking subscriptions.
# -------------------------------------------------------------------------------------------------


async def test_unsubscribing_stops_delivery() -> None:
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    hub.unsubscribe(subscription)
    await hub.publish((_at_version(KEY, 1),))

    assert hub.subscribers == 0
    assert subscription.offered == 0


async def test_unsubscribing_twice_or_a_stranger_is_safe() -> None:
    """Both are reachable from ordinary cleanup, and neither is an error worth raising for.

    The `finally` in `SubscriptionHub.subscription` will unsubscribe something the hub may already
    have pruned; a caller may unsubscribe a `Subscription` it built directly. Neither matters.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    hub.unsubscribe(subscription)
    hub.unsubscribe(subscription)
    hub.unsubscribe(Subscription())

    assert hub.subscribers == 0


async def test_the_context_manager_unsubscribes_on_a_normal_exit() -> None:
    """The uninteresting path, pinned anyway so the interesting ones are not the only cover.

    Without this, a `try/except Exception` in place of the `try/finally` would pass both the raising
    and the cancelled cases and leak on every connection that closed politely — which, on a
    dashboard that reloads, is most of them.
    """
    hub = SubscriptionHub()

    with hub.subscription() as subscription:
        assert hub.subscribers == 1

    assert hub.subscribers == 0
    assert subscription.is_closed


async def test_the_context_manager_unsubscribes_even_when_the_body_raises() -> None:
    """The reason it exists: a connection handler almost never ends by reaching its last line.

    It ends with a `CancelledError` on disconnect or an exception from a socket write, and a leaked
    subscription keeps buffering positions for a client that is gone, forever.
    """
    hub = SubscriptionHub()

    with pytest.raises(RuntimeError, match="the client vanished"):
        with hub.subscription(keys=[KEY]) as subscription:
            assert hub.subscribers == 1
            raise RuntimeError("the client vanished")

    assert hub.subscribers == 0
    assert subscription.is_closed


async def test_the_context_manager_unsubscribes_on_cancellation() -> None:
    """The disconnect path specifically, since `CancelledError` is not an `Exception` subclass.

    Worth its own test because a `try/except Exception` would catch the case above and miss this
    one, and this one is how every real SSE connection ends.
    """
    hub = SubscriptionHub()
    held: list[Subscription] = []

    async def handler() -> None:
        with hub.subscription() as subscription:
            held.append(subscription)
            await asyncio.Event().wait()

    task = asyncio.create_task(handler())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert hub.subscribers == 0
    assert held[0].is_closed


async def test_a_subscription_closed_behind_the_hubs_back_is_pruned() -> None:
    """A `Subscription.close()` without an `unsubscribe` still has to stop costing anything.

    Without the prune, the hub's list grows for the life of the process, holding a `Position` per
    closed subscriber's last buffer and visiting each of them on every publish.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()
    assert hub.subscribers == 1

    subscription.close()
    await hub.publish((_at_version(KEY, 1),))

    assert hub.subscribers == 0


async def test_closing_the_hub_closes_every_subscription() -> None:
    """Process shutdown, so no reader task is left parked on a doorbell nobody will ring."""
    hub = SubscriptionHub()
    subscriptions = [hub.subscribe(), hub.subscribe(keys=[KEY])]

    hub.close()

    assert hub.subscribers == 0
    assert all(subscription.is_closed for subscription in subscriptions)


async def test_the_hub_reports_its_shape_for_an_operator() -> None:
    """`as_dict` is what slice 5 serves. Pinned because a dashboard depends on the key names."""
    hub = SubscriptionHub()
    hub.subscribe(keys=[KEY])
    await hub.publish((_at_version(KEY, 1),))

    report = hub.as_dict()

    assert report["subscribers"] == 1
    assert report["published"] == 1
    assert report["overflow_disconnects"] == 0
    assert report["detail"][0]["watches"] == [str(KEY)]
    assert report["detail"][0]["pending"] == 1
    assert report["detail"][0]["overflowed"] is False


async def test_a_wildcard_subscriber_reports_a_star() -> None:
    """The two filter kinds must be distinguishable in the report: only one of them can overflow."""
    hub = SubscriptionHub()
    hub.subscribe()

    assert hub.as_dict()["detail"][0]["watches"] == "*"


async def test_the_subscriber_report_names_the_counters_an_operator_reads() -> None:
    """Conflation with delivery is health; conflation without it is a client that stopped reading.

    Both counters are needed to tell those apart, so both have to be in the report — a `conflated`
    figure on its own is unreadable: the healthy and unhealthy cases produce the same number.
    """
    hub = SubscriptionHub()
    subscription = hub.subscribe()

    await hub.publish((_at_version(KEY, 1),))
    await hub.publish((_at_version(KEY, 2),))
    subscription.drain_nowait()

    report: dict[str, Any] = subscription.as_dict()
    assert report["offered"] == 2
    assert report["conflated"] == 1
    assert report["positions_delivered"] == 1
    assert report["batches_delivered"] == 1
    assert report["suppressed"] == 0
    assert report["closed"] is False


async def test_the_repr_is_readable_in_a_failure_message() -> None:
    hub = SubscriptionHub()
    subscription = hub.subscribe(keys=[KEY, OTHER_KEY])
    await hub.publish((_at_version(KEY, 1),))

    text = repr(subscription)

    assert "2 keys" in text
    assert "pending=1" in text
    assert "overflowed=False" in text


# -------------------------------------------------------------------------------------------------
# 9. Wired to the real projector, because "the seam fits" is its own claim.
# -------------------------------------------------------------------------------------------------


async def test_the_hubs_publish_is_a_valid_change_listener() -> None:
    """Typed rather than asserted, so mypy polices the fit and the runtime cost is nil.

    The value of writing it down: if `ChangeListener` ever grows a second argument, this line fails
    the type check with a clear message, instead of the wiring failing at runtime in slice 5.
    """
    hub = SubscriptionHub()
    listener: ChangeListener = hub.publish

    await listener((_at_version(KEY, 1),))

    assert hub.published == 1


async def test_one_real_event_reaches_a_subscriber_with_the_right_money() -> None:
    """End to end through the projector: decode, project, commit, notify, buffer, drain.

    The amount is asserted, not just the arrival, because a fan-out that delivered the *previous*
    version of the position would pass an arrival-only test and show the client stale money.
    """
    store = InMemoryPositionStore()
    hub = SubscriptionHub()
    projector = Projector(store, on_change=hub.publish)
    subscription = hub.subscribe(keys=[KEY])

    await projector.ingest(envelope_payload(event_id=1))

    delivered = subscription.drain_nowait()
    assert len(delivered) == 1
    assert delivered[0].key == KEY
    assert str(delivered[0].totals_for(Bucket.PENDING).amount) == AMOUNT
    assert delivered[0].version == 1


async def test_a_duplicate_event_wakes_nobody() -> None:
    """No change, no notification (ADR-027), so a redelivery must not produce a visible update.

    Two suppression mechanisms could produce this outcome — the projector not notifying, or the
    subscription's version watermark — so the assertion is on `offered`, which is zero only if the
    projector never published.
    """
    store = InMemoryPositionStore()
    hub = SubscriptionHub()
    projector = Projector(store, on_change=hub.publish)

    await projector.ingest(envelope_payload(event_id=1))
    subscription = hub.subscribe(keys=[KEY])
    await projector.ingest(envelope_payload(event_id=1))

    assert subscription.offered == 0
    assert subscription.drain_nowait() == ()


async def test_a_real_cross_position_move_arrives_as_one_batch() -> None:
    """A later event carrying a corrected counterparty debits one position and credits another.

    Driven through the projector rather than hand-built, because the claim being pinned is that the
    projector emits both halves in a single `on_change` call and the hub keeps them together. Two
    separate publishes would produce two batches here and pass every hand-built test above.
    """
    store = InMemoryPositionStore()
    hub = SubscriptionHub()
    projector = Projector(store, on_change=hub.publish)

    await projector.ingest(envelope_payload(event_id=1))
    subscription = hub.subscribe()
    await projector.ingest(
        envelope_payload(event_id=2, event_type=EVENT_SETTLED, counterparty="BETA-TRUST")
    )

    batch = subscription.drain_nowait()
    assert len(batch) == 2, "the debit and the credit reached the subscriber separately"
    assert {position.key for position in batch} == {KEY, OTHER_KEY}
    assert subscription.batches_delivered == 1


async def test_a_projector_with_no_listener_still_works() -> None:
    """The offline-rebuild case, and the reason `on_change` is optional.

    A rebuild has no subscribers by definition, and requiring a listener would mean constructing a
    hub nobody reads from just to satisfy a signature.
    """
    store = InMemoryPositionStore()
    projector = Projector(store)

    await projector.ingest(envelope_payload(event_id=1))

    assert projector.stats.applied == 1
