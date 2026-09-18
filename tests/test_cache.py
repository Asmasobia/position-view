"""The cache: one store read, protected four different ways, and the two rules about its shape.

WHAT MAKES THESE TESTS DIFFERENT FROM THE REST OF THE SUITE
---------------------------------------------------------------------------------
Every other test file in this repository can be written against values. A cache cannot: its whole
value proposition is about what happens *between* two suspension points, and none of the four
problems it solves is observable from a single sequential call.

So the store double here is not a convenience — it is the instrument. `ReadCountingStore` counts
reads and, crucially, *suspends* on every one of them, and it can be gated so that a load is
provably still in flight while the test does something else. That is the lesson ADR-023 charged full
price for in slice 2: a double that returns without ever yielding to the event loop cannot observe
concurrency, so a suite built on one reports success for code with none of the properties it claims.

Which means the interesting tests in this file all have the same shape:

    1. gate the store so a load cannot finish
    2. drive the concurrency that the production system would drive
    3. assert on the state *while the load is in flight*
    4. release the gate and assert on the outcome

Step 3 is the one that is impossible without the gate, and it is where every real bug in this file's
subject lives.

THE TWO STRUCTURAL TESTS, AND WHY THEY ARE NOT BEHAVIOURAL
---------------------------------------------------------------------------------
Two of the cache's claims cannot be checked by calling it, so they are checked by reading it:

  - `CachedPositions` is not a `PositionStore`. Making it one is a two-line change that would let it
    be passed to `Projector`, at which point the projection computes new positions from cached ones
    and writes the result back as truth. Nothing behavioural detects that; the *surface* is what has
    to be pinned.
  - `on_change` contains no `await`. It runs inside the projector's lock, so a suspension stalls
    the projection — and in-process doubles never suspend, so the suite would stay green. Parsed,
    for the same reason `SubscriptionHub.publish` is parsed (ADR-030).
"""

from __future__ import annotations

import ast
import asyncio
import gc
import inspect
import uuid
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from builders import AMOUNT, KEY, OTHER_KEY, envelope_payload
from position_view.cache import DEFAULT_MAX_ENTRIES, UNCACHED_VERSION, CachedPositions
from position_view.model import (
    EVENT_SETTLED,
    EVENT_VALIDATED,
    Position,
    PositionKey,
    Totals,
)
from position_view.projection import (
    ChangeListener,
    InMemoryPositionStore,
    Projector,
    WriteBatch,
    combine_listeners,
)
from position_view.push import SubscriptionHub

# Applied to the module rather than to each test, for `test_store.py`'s reason: a forgotten
# `@pytest.mark.asyncio` under `asyncio_mode = "strict"` does not fail, it *skips*, so the marker is
# the kind of thing that must not be anybody's responsibility to remember.
#
# The consequence is that even the tests that need no event loop — the two surface tests, the AST
# test, the two constructor tests — are written `async def`. That is deliberate: with a module-level
# marker a synchronous test triggers a `PytestWarning` about a mark it did not ask for, and one
# warning is all it takes for the next one to be scrolled past. Consistent with `test_hub.py`, whose
# AST test is async for exactly the same reason.
pytestmark = pytest.mark.asyncio

#: A third key, so the eviction tests can push a two-entry cache over its bound. Named rather than
#: built inline because "which key got evicted" is the assertion, and a literal in the middle of an
#: assert is harder to check against the ordering above it.
THIRD_KEY = PositionKey(counterparty="ZULU-CORP", currency="USD")

#: The exact public surface of `CachedPositions`.
#:
#: Written out rather than derived, because the test using it is guarding against a name being
#: *added*. A derived expectation would pass whatever the class happened to expose, which is the one
#: thing this must not do — see `test_the_cache_is_not_a_position_store`.
CACHE_SURFACE = frozenset(
    {
        "as_dict",
        "clear",
        "entries",
        "hit_rate",
        "in_flight",
        "invalidate",
        "on_change",
        "position_for",
        "refresh",
    }
)

#: The keys `as_dict` must always contain. A stable shape is the point; see the method's docstring.
REPORT_KEYS = frozenset(
    {
        "coalesced",
        "entries",
        "evictions",
        "hit_rate",
        "hits",
        "in_flight",
        "invalidations",
        "load_failures",
        "loads",
        "loads_discarded",
        "loads_superseded",
        "max_entries",
        "misses",
        "negative_hits",
        "refreshed",
        "refreshes_dropped_cold",
        "refreshes_dropped_stale",
        "refreshes_held_for_load",
    }
)


def _cache_source() -> str:
    """The cache module's own source, for the structural test.

    Located through `inspect` rather than by building a path from `__file__`, so moving the module
    does not turn the structural test into a silent no-op — a hand-built path that no longer
    resolves would raise here — a failure — rather than reading some other file that exists.
    """
    path = inspect.getsourcefile(CachedPositions)
    assert path is not None, "CachedPositions has no source file"
    return Path(path).read_text(encoding="utf-8")


_CACHE_SOURCE = _cache_source()


class ReadCountingStore(InMemoryPositionStore):
    """The shipped in-memory store, plus a read counter, a suspension point and a gate.

    Subclassing the *concrete* store rather than implementing the protocol from scratch, which is
    the opposite of what `store.py` recommends for doubles — and deliberately so. The recommendation
    is about doubles that need to *behave differently*; this one needs to behave identically and be
    watched, so inheriting the real behaviour is what keeps the double from becoming a second,
    divergent implementation of the store's semantics. Only `position_for` is overridden, and it
    delegates.

    ⚠️ THE SUSPENSION IN `position_for` IS THE POINT OF THIS CLASS.
       `await asyncio.sleep(0)` yields to the event loop exactly once. Without it, a read completes
       without ever suspending, so two concurrent `position_for` calls on the cache cannot
       interleave at all — and a cache with no single-flight at all would pass every stampede test
       in this file. The double has to be *at least as slow as the real thing is at being fast*, or
       it measures nothing.
    """

    def __init__(self) -> None:
        super().__init__()

        #: How many times `position_for` has been called. The number single-flight is about.
        self.reads = 0

        #: When set to an `Event`, every read blocks on it. This is what holds a load in flight
        #: while the test inspects the cache, and it is why these tests need no timeouts or sleeps.
        self.gate: asyncio.Event | None = None

        #: Set as soon as a read has started, so a test can wait for "the load has reached the
        #: store" rather than guessing how many event-loop ticks that takes.
        self.reading = asyncio.Event()

        #: When set, the next and every subsequent read raises this instead of returning.
        #:
        #: Typed as `BaseException`, not `Exception`, because one test needs to raise outside the
        #: `Exception` hierarchy to check that `_load`'s except clause is narrow — a double that
        #: cannot express the failure cannot test the handling of it.
        self.fail_with: BaseException | None = None

    async def position_for(self, key: PositionKey) -> Position | None:
        self.reads += 1
        self.reading.set()
        if self.gate is None:
            await asyncio.sleep(0)
        else:
            await self.gate.wait()
        if self.fail_with is not None:
            raise self.fail_with
        return await super().position_for(key)


class Teardown(BaseException):
    """A `BaseException` that is not a `KeyboardInterrupt`, for the narrow-except-clause test.

    A real `KeyboardInterrupt` would be the honest thing to raise, and pytest reads one as a request
    to stop the session — so the test would abort the whole run rather than assert anything. This is
    the same position in the hierarchy with none of that meaning attached.
    """


def _position(key: PositionKey, version: int, *, amount: str = "10.00") -> Position:
    """A position at a chosen version. The version is what every comparison in the cache uses."""
    return Position(key=key, pending=Totals(amount=Decimal(amount), count=1), version=version)


async def _put(store: InMemoryPositionStore, position: Position, *, checkpoint: int = 1) -> None:
    """Put a position in the store the only way the store allows: one committed batch."""
    await store.commit(WriteBatch(checkpoint=checkpoint, positions=(position,)))


async def _until(predicate: Callable[[], bool], *, ticks: int = 500) -> None:
    """Yield to the event loop until `predicate` holds, or give up after `ticks` yields.

    ⚠️ BOUNDED ON PURPOSE, AND THE BOUND IS NOT A TIMEOUT.
       The natural spelling is `while not predicate(): await asyncio.sleep(0)`, and it is one bug
       away from a test that *hangs* instead of failing. A hung suite gives no failure message, no
       traceback and no indication of which test is stuck — it just stops, which during a CI run
       looks identical to an infrastructure problem. Giving up after a fixed number of ticks means
       the assertion that follows this call fails normally and says exactly what it wanted.

       Ticks rather than seconds because nothing here is slow: every wait in this file is for
       another task to be *scheduled*, not for time to pass, so a tick count is both faster and
       immune to a loaded machine.
    """
    for _ in range(ticks):
        if predicate():
            return
        await asyncio.sleep(0)


# ---------------------------------------------------------------------------------
# 1. The shape of the thing: what it is not, and what it must not do
# ---------------------------------------------------------------------------------


async def test_the_cache_is_not_a_position_store() -> None:
    """⚠️ THE MOST IMPORTANT TEST IN THIS FILE, AND IT ASSERTS ON A SET OF NAMES.

    Adding `commit`, `all_positions`, `snapshot_for`, `checkpoint` and `snapshot_count` as
    passthroughs would make `CachedPositions` satisfy `PositionStore`, and it would read like a
    tidy-up: one type instead of two, and the api layer could take either. It would also make this
    legal:

        projector = Projector(CachedPositions(store))

    at which point `_build_changed_positions` reads a *cached* position, applies the event's deltas
    to it, and commits the result. Read a version that is one change out of date and the committed
    position is short by exactly that change — then that value is the store's truth, is replayed
    identically forever, and no counter anywhere moves. The lock cannot help: it serialises access
    to the store, and the stale value did not come from the store.

    There is no behavioural test for this. The type either fits the protocol or it does not, and if
    it fits, the dangerous line compiles and passes review. So the surface is pinned as data, and
    the failure message is the argument.
    """
    public = {name for name in vars(CachedPositions) if not name.startswith("_")}
    assert public == CACHE_SURFACE, (
        f"the cache's public surface changed: {sorted(public ^ CACHE_SURFACE)}. "
        f"If a store method was added, read this test's docstring before updating the set."
    )


async def test_the_cache_offers_no_way_to_write_or_to_read_a_snapshot() -> None:
    """The same rule again, named method by method, so the failure says which one appeared.

    Deliberately redundant with the surface test above. The set comparison catches the addition but
    reports it as a set difference; this one reports "the cache grew a `commit` method", which is
    the sentence a reader needs.
    """
    for forbidden in ("commit", "checkpoint", "snapshot_for", "all_positions", "snapshot_count"):
        assert not hasattr(CachedPositions, forbidden), (
            f"CachedPositions has a '{forbidden}' method, which makes it substitutable for a "
            f"PositionStore. See test_the_cache_is_not_a_position_store."
        )


async def test_on_change_never_suspends() -> None:
    """⚠️ `on_change` RUNS UNDER THE PROJECTOR'S LOCK, SO AN `await` IN IT STALLS THE PROJECTION.

    Exactly the argument ADR-030 makes for `SubscriptionHub.publish`, and it applies here for the
    same structural reason: the projector notifies while holding the lock that serialises the whole
    fold, so anything the listener waits for is time during which no event is applied.

    Checked by parsing rather than by calling, because no behavioural test can see it. A cache that
    awaited a Redis round trip in `on_change` would pass every other test in this file — the double
    is in-process and never blocks — and would serialise event application behind a network hop in
    production only.

    Parsed rather than substring-searched: the word "await" appears throughout this module's own
    comments, including in the paragraph explaining this rule, so a text search would be defeated by
    the documentation of the thing it is searching for.
    """
    tree = ast.parse(_CACHE_SOURCE)

    # The vacuity guard. A renamed method, a moved file or a parse of the wrong source would make
    # the search below find nothing and the test pass having checked nothing at all.
    definitions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "on_change"
    ]
    assert len(definitions) == 1, f"expected exactly one on_change, found {len(definitions)}"
    assert any(isinstance(node, ast.Await) for node in ast.walk(tree)), (
        "the parsed source contains no await at all, so this file is almost certainly not the cache"
    )

    suspensions = [node for node in ast.walk(definitions[0]) if isinstance(node, ast.Await)]
    assert not suspensions, (
        "CachedPositions.on_change contains an await. It runs inside the projector's lock, so "
        "whatever it waits for is time during which the projection applies nothing."
    )


async def test_on_change_is_a_valid_change_listener() -> None:
    """The cache can be handed straight to the projector, and mypy is what proves the fit.

    The annotation is the test. `ChangeListener` is a `Callable[[tuple[Position, ...]], Awaitable[
    None]]`, so assigning `cache.on_change` to it fails type-checking if the signature drifts — a
    check that runs in `mypy --strict` rather than at runtime, which is where a signature mismatch
    belongs. The `await` below then confirms it is callable in the shape the projector calls it.
    """
    cache = CachedPositions(ReadCountingStore())
    listener: ChangeListener = cache.on_change
    await listener(())
    assert cache.refreshed == 0


# ---------------------------------------------------------------------------------
# 2. Read-through: the ordinary path
# ---------------------------------------------------------------------------------


async def test_a_cold_read_goes_to_the_store() -> None:
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    cache = CachedPositions(store)

    result = await cache.position_for(KEY)

    assert result is not None
    assert result.version == 1
    assert store.reads == 1
    assert cache.misses == 1
    assert cache.loads == 1
    assert cache.hits == 0
    assert cache.entries == 1


async def test_a_second_read_is_served_from_the_cache() -> None:
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    cache = CachedPositions(store)

    first = await cache.position_for(KEY)
    second = await cache.position_for(KEY)

    assert store.reads == 1, "the second read reached the store, so nothing is being cached"
    assert cache.hits == 1
    assert first is second, "positions are frozen, so a cache hit should return the same object"


async def test_the_cached_value_is_the_stores_value() -> None:
    """A cache that returns something *equal* would be a cache that copies, and copying is a bug.

    `Position` is frozen, so there is nothing to defend against by copying, and a copy would break
    the identity comparison the projector relies on (`with_deltas` returns `self` when nothing
    changed) as well as costing an allocation per hit.
    """
    store = ReadCountingStore()
    stored = _position(KEY, 3, amount="777.25")
    await _put(store, stored)
    cache = CachedPositions(store)

    assert await cache.position_for(KEY) is stored


async def test_two_keys_are_cached_independently() -> None:
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1), checkpoint=1)
    await _put(store, _position(OTHER_KEY, 1), checkpoint=2)
    cache = CachedPositions(store)

    assert await cache.position_for(KEY) is not None
    assert await cache.position_for(OTHER_KEY) is not None
    assert store.reads == 2
    assert cache.entries == 2


async def test_the_in_flight_register_is_empty_once_a_load_finishes() -> None:
    """A leaked in-flight entry is a permanently broken key, so its absence is worth asserting.

    The register is what makes single-flight work, and an entry that outlives its load makes every
    subsequent request for that key wait on a task that has already finished — which returns the
    *original* value forever, or raises the original error forever. Silent, permanent, and specific
    to one key, which is the hardest kind of bug to be handed.
    """
    store = ReadCountingStore()
    cache = CachedPositions(store)

    await cache.position_for(KEY)

    assert cache.in_flight == 0


async def test_the_read_counters_reconcile() -> None:
    """`misses - coalesced == loads`, which is the cache's own arithmetic stated as an assertion.

    Cheap, and it catches the class of bug where a counter is incremented on the wrong branch —
    which matters because these are the numbers an operator uses to decide whether the cache is
    working, and a hit rate computed from a miscounted miss is worse than no hit rate at all.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    cache = CachedPositions(store)

    await cache.position_for(KEY)
    await cache.position_for(KEY)
    await cache.position_for(OTHER_KEY)

    assert cache.misses - cache.coalesced == cache.loads == store.reads


# ---------------------------------------------------------------------------------
# 3. Negative caching: an absence is a value
# ---------------------------------------------------------------------------------


async def test_an_absent_position_is_cached_as_an_absence() -> None:
    """⚠️ THE BUG THIS PREVENTS IS A FREE LOAD GENERATOR.

    `position_for` returns None for a counterparty nothing has contributed to, and slice 5 turns
    that into a 404. A cache that stores only hits therefore sends every 404 to the store, so the
    cheapest way to put load on this service is to ask about counterparties that do not exist — no
    authentication needed beyond whatever the read endpoint needs, and no cache in the way.
    """
    store = ReadCountingStore()
    cache = CachedPositions(store)

    assert await cache.position_for(KEY) is None
    assert await cache.position_for(KEY) is None

    assert store.reads == 1, "the absence was not cached, so every 404 reaches the store"
    assert cache.negative_hits == 1


async def test_a_negative_hit_is_counted_separately_from_a_hit() -> None:
    """Two numbers rather than one, because they mean different things about the caller.

    A high `hits` is a cache doing its job. A high `negative_hits` is either a client polling for
    something that has not happened yet or somebody enumerating counterparty names, and both are
    worth seeing. Summed into a single "hits" figure, neither is visible at all.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    cache = CachedPositions(store)

    await cache.position_for(KEY)
    await cache.position_for(KEY)
    await cache.position_for(OTHER_KEY)
    await cache.position_for(OTHER_KEY)

    assert cache.hits == 1
    assert cache.negative_hits == 1


async def test_a_cached_absence_occupies_an_entry() -> None:
    """Which is precisely why the cache has to be bounded — see the eviction section.

    Caching absences changes the key space from "positions that exist" to "strings a caller can
    send". This assertion is the one-line statement of that: an entry the caller chose the name of.
    """
    store = ReadCountingStore()
    cache = CachedPositions(store)

    assert await cache.position_for(KEY) is None

    assert cache.entries == 1


async def test_a_cached_absence_is_upgraded_when_the_position_first_appears() -> None:
    """The first event for a counterparty must dislodge a cached 404, and version 0 is why it does.

    A cached absence has no version, so it is compared as `UNCACHED_VERSION`. That works because a
    position is only ever published after `with_deltas` has bumped it past zero — so every real
    change is strictly newer than an absence, and the upgrade needs no special case.
    """
    store = ReadCountingStore()
    cache = CachedPositions(store)
    assert await cache.position_for(KEY) is None

    cache.refresh([_position(KEY, 1, amount="42.00")])

    upgraded = await cache.position_for(KEY)
    assert upgraded is not None
    assert upgraded.version == 1
    assert store.reads == 1, "the upgrade should not have required a reload"
    assert cache.refreshed == 1


async def test_the_absence_sentinel_is_lower_than_any_published_version() -> None:
    """Pins the reasoning above rather than the number: version 0 is never published.

    `_empty_position` starts at 0 and `with_deltas` bumps before anything is written, so 0 is
    unreachable as a cached value and safe as a floor. If that ever changed, an absence would
    compare equal to a real version and the upgrade above would silently stop happening.
    """
    assert UNCACHED_VERSION == 0
    assert Position(key=KEY).version == UNCACHED_VERSION


# ---------------------------------------------------------------------------------
# 4. Single-flight: the stampede
# ---------------------------------------------------------------------------------


async def test_fifty_concurrent_misses_produce_one_store_read() -> None:
    """⚠️ THE TEST THE GATE EXISTS FOR, AND THE ONE A NON-SUSPENDING DOUBLE CANNOT WRITE.

    Fifty tasks ask for the same cold key. Without the gate, the first read completes before the
    second task is scheduled, so the second is a *hit* — and a cache with no single-flight
    whatsoever would report `reads == 1` and pass. The measurement is of the double, not the cache.

    With the gate, all fifty are provably in flight at the same moment: the assertion on
    `cache.in_flight` runs while nothing can have completed. That is the state a stampede actually
    is, and one store read is the property being claimed.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    waiters = [asyncio.create_task(cache.position_for(KEY)) for _ in range(50)]
    # Both conditions, because they are two different facts. `coalesced == 49` says every waiter
    # joined the one load; `store.reading` says that load has actually reached the store rather than
    # merely being scheduled — `create_task` queues a coroutine, it does not start it. Asserting on
    # `store.reads` without the second condition would be asserting that a task the loop has not run
    # yet has already done its work.
    await _until(lambda: cache.coalesced == 49 and store.reading.is_set())

    assert cache.coalesced == 49, "not every waiter joined the load in flight"
    assert cache.in_flight == 1, "more than one load was started for one key"
    assert store.reads == 1

    store.gate.set()
    results = await asyncio.gather(*waiters)

    assert store.reads == 1, "the stampede reached the store"
    assert cache.loads == 1
    assert all(result is not None and result.version == 1 for result in results)


async def test_every_waiter_on_one_load_gets_the_same_object() -> None:
    """Not just an equal value: the same object, so one allocation serves fifty waiters."""
    store = ReadCountingStore()
    stored = _position(KEY, 1)
    await _put(store, stored)
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    waiters = [asyncio.create_task(cache.position_for(KEY)) for _ in range(5)]
    await _until(lambda: cache.coalesced == 4)
    store.gate.set()

    assert all(result is stored for result in await asyncio.gather(*waiters))


async def test_concurrent_misses_for_different_keys_are_not_coalesced() -> None:
    """Single-flight is per key. Collapsing across keys answers one question with another's data.

    Worth an explicit test because the bug is easy to write — a single in-flight future instead of a
    dict of them — and its symptom is the worst kind: the wrong counterparty's exposure, returned
    with every appearance of being correct.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1), checkpoint=1)
    await _put(store, _position(OTHER_KEY, 7), checkpoint=2)
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    first = asyncio.create_task(cache.position_for(KEY))
    second = asyncio.create_task(cache.position_for(OTHER_KEY))
    await _until(lambda: cache.in_flight == 2)

    assert cache.in_flight == 2
    assert cache.coalesced == 0

    store.gate.set()
    mine, theirs = await asyncio.gather(first, second)

    assert mine is not None and mine.key == KEY
    assert theirs is not None and theirs.key == OTHER_KEY and theirs.version == 7
    assert store.reads == 2


async def test_the_wave_after_a_stampede_is_all_hits() -> None:
    """The stampede is absorbed once and then the cache does its ordinary job."""
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    waiters = [asyncio.create_task(cache.position_for(KEY)) for _ in range(10)]
    await _until(lambda: cache.coalesced == 9)
    store.gate.set()
    await asyncio.gather(*waiters)
    store.gate = None

    for _ in range(10):
        assert await cache.position_for(KEY) is not None

    assert store.reads == 1
    assert cache.hits == 10


# ---------------------------------------------------------------------------------
# 5. Failure: what a broken store must not be allowed to teach the cache
# ---------------------------------------------------------------------------------


async def test_a_store_failure_reaches_the_caller() -> None:
    store = ReadCountingStore()
    store.fail_with = RuntimeError("the database is unreachable")
    cache = CachedPositions(store)

    with pytest.raises(RuntimeError, match="unreachable"):
        await cache.position_for(KEY)

    assert cache.load_failures == 1


async def test_a_store_failure_is_not_cached_as_an_absence() -> None:
    """⚠️ THE MOST TEMPTING WRONG ANSWER IN THE WHOLE FILE IS `except: return None`.

    It is one line, it makes the endpoint stop throwing 500s, and it converts a five-second database
    wobble into a 404 that lasts as long as the cache entry. A client asking about a counterparty
    with real exposure is told — with an ordinary, unalarming status code — that it does not exist.
    The monitoring sees a drop in errors.

    Failing the request is worse for one caller and honest for every caller, and the retry lands on
    a cache that has learnt nothing false. This test is the negative control for that: after the
    store recovers, the very next read produces the real value.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.fail_with = RuntimeError("transient")
    cache = CachedPositions(store)

    with pytest.raises(RuntimeError):
        await cache.position_for(KEY)

    assert cache.entries == 0, "a failed read was cached, so the failure is now the answer"

    store.fail_with = None
    recovered = await cache.position_for(KEY)

    assert recovered is not None
    assert recovered.version == 1
    assert store.reads == 2


async def test_a_store_failure_leaves_no_in_flight_entry() -> None:
    """Without the `finally`, one transient error poisons a key permanently.

    The in-flight register would keep a finished, failed task, and every later request for that key
    would await it and be handed the same exception — forever, for that one key, with the store
    perfectly healthy. The `finally` in `_load` is what makes the failure as transient as its cause.
    """
    store = ReadCountingStore()
    store.fail_with = RuntimeError("transient")
    cache = CachedPositions(store)

    with pytest.raises(RuntimeError):
        await cache.position_for(KEY)

    assert cache.in_flight == 0


async def test_every_waiter_on_a_failed_load_sees_the_error() -> None:
    """A coalesced request must not be quietly resolved with `None` when the load it joined failed.

    This is single-flight's obligation in the other direction: the waiters gave up their own store
    read on the promise of somebody else's result, so they are entitled to that result *including*
    its failure. Handing them `None` instead would turn one store error into forty silent 404s.
    """
    store = ReadCountingStore()
    store.gate = asyncio.Event()
    store.fail_with = RuntimeError("transient")
    cache = CachedPositions(store)

    waiters = [asyncio.create_task(cache.position_for(KEY)) for _ in range(5)]
    await _until(lambda: cache.coalesced == 4)
    store.gate.set()

    outcomes = await asyncio.gather(*waiters, return_exceptions=True)

    assert len(outcomes) == 5
    assert all(isinstance(outcome, RuntimeError) for outcome in outcomes)
    assert store.reads == 1
    assert cache.load_failures == 1


async def test_a_failed_load_does_not_prevent_the_next_one() -> None:
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    cache = CachedPositions(store)

    store.fail_with = RuntimeError("transient")
    with pytest.raises(RuntimeError):
        await cache.position_for(KEY)
    store.fail_with = None

    assert await cache.position_for(KEY) is not None
    assert cache.loads == 1
    assert cache.load_failures == 1


async def test_a_base_exception_is_not_counted_as_a_store_failure() -> None:
    """`except Exception`, not `except BaseException`, and `load_failures` is where it shows.

    A `KeyboardInterrupt` or a `CancelledError` during a store read is the process being torn down,
    not the store being broken. Widening the clause to `BaseException` would make every shutdown add
    to the failure count — so an orderly deploy would look, on the dashboard, exactly like a
    database outage. The bookkeeping still has to be clean either way, which the in-flight
    assertion checks.
    """
    store = ReadCountingStore()
    store.fail_with = Teardown()
    cache = CachedPositions(store)

    with pytest.raises(Teardown):
        await cache.position_for(KEY)

    assert cache.load_failures == 0, "a teardown was counted as a store failure"
    assert cache.in_flight == 0
    assert cache.entries == 0


async def test_an_abandoned_failed_load_does_not_spam_the_event_loops_logger() -> None:
    """⚠️ THE ONE TEST HERE THAT IS ABOUT A LOG LINE, AND IT IS WORTH ITS COMPLEXITY.

    When a load fails and every waiter has already gone away, nobody is left to retrieve the
    exception — so asyncio reports "Task exception was never retrieved" when the task is collected.
    That log line is noise: the failure was counted, and the caller that cared has already had it
    re-raised. But it is *alarming* noise, arriving at an unpredictable moment (garbage collection,
    not failure) with a traceback and no request context, and the cost of noise like that is paid
    the day it hides something real.

    Hence the `add_done_callback(_retrieve_exception)` in `position_for`, and hence this test, which
    has to construct the exact situation: a failed load with no surviving waiter, and then a
    collection. `gc.collect()` is what turns "eventually" into "now", which is the only way the
    assertion can be about a specific moment.

    The loop's exception handler is replaced rather than the logger captured, because that is the
    hook asyncio actually calls. pytest-asyncio gives each test its own loop, so nothing leaks.
    """
    logged: list[dict[str, Any]] = []
    asyncio.get_running_loop().set_exception_handler(lambda _loop, context: logged.append(context))

    store = ReadCountingStore()
    store.gate = asyncio.Event()
    store.fail_with = RuntimeError("transient")
    cache = CachedPositions(store)

    waiter = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.in_flight == 1)

    # The one caller gives up *before* the load fails, so by the time it does there is nobody
    # holding the task and nobody who will ever ask it for its exception.
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    del waiter

    store.gate.set()
    await _until(lambda: cache.load_failures == 1)
    assert cache.load_failures == 1

    gc.collect()
    await asyncio.sleep(0)

    assert logged == [], f"the event loop logged an abandoned failure: {logged}"


# ---------------------------------------------------------------------------------
# 6. Cancellation: one client hanging up must not fail everybody else's request
# ---------------------------------------------------------------------------------


async def test_cancelling_one_waiter_does_not_cancel_the_shared_load() -> None:
    """⚠️ THE REASON `position_for` AWAITS A SHIELD AND NOT THE TASK ITSELF.

    Cancelling a task cancels the future it is waiting on. With a bare `await load`, one HTTP client
    disconnecting — which cancels its request task, and is the single most common event in a web
    service — would cancel the *shared* load, and every other waiter on that key would receive a
    `CancelledError` for a request they had nothing to do with.

    The symptom in production is the worst possible combination: intermittent, load-dependent,
    concentrated on the most popular keys, and completely absent from any sequential test. One
    impatient browser tab failing forty unrelated requests.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    leaving = asyncio.create_task(cache.position_for(KEY))
    staying = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.coalesced == 1)

    leaving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leaving

    store.gate.set()
    survived = await staying

    assert survived is not None, "the departing caller took the shared load down with it"
    assert survived.version == 1
    assert store.reads == 1


async def test_a_cancelled_first_caller_still_leaves_the_cache_warm() -> None:
    """The work the departed caller paid for is not thrown away.

    A consequence of the same design rather than a separate feature: because the load runs as its
    own task and nobody's cancellation reaches it, it finishes and populates the cache. The next
    caller gets a hit. The alternative — tying the load to whichever request happened to arrive
    first — makes the cache's warmth depend on client patience.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    leaving = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.in_flight == 1)

    leaving.cancel()
    with pytest.raises(asyncio.CancelledError):
        await leaving

    store.gate.set()
    await _until(lambda: cache.entries == 1)

    assert cache.entries == 1
    assert await cache.position_for(KEY) is not None
    assert store.reads == 1
    assert cache.load_failures == 0, "a cancellation is not a store failure"


# ---------------------------------------------------------------------------------
# 7. The bound, and what "least recently used" is allowed to mean
# ---------------------------------------------------------------------------------


async def test_the_bound_is_inclusive() -> None:
    """`max_entries=2` means two entries are fine. Off by one here evicts a live working set."""
    store = ReadCountingStore()
    cache = CachedPositions(store, max_entries=2)

    await cache.position_for(KEY)
    await cache.position_for(OTHER_KEY)

    assert cache.entries == 2
    assert cache.evictions == 0


async def test_the_coldest_entry_is_evicted_when_the_bound_is_passed() -> None:
    store = ReadCountingStore()
    cache = CachedPositions(store, max_entries=2)

    await cache.position_for(KEY)
    await cache.position_for(OTHER_KEY)
    await cache.position_for(THIRD_KEY)

    assert cache.entries == 2
    assert cache.evictions == 1

    # KEY was the coldest, so it should be the one that has to be read again.
    await cache.position_for(KEY)
    assert store.reads == 4


async def test_a_read_promotes_an_entry_out_of_the_eviction_position() -> None:
    """`Least recently used` has to mean *used*, or the cache evicts its own hot keys."""
    store = ReadCountingStore()
    cache = CachedPositions(store, max_entries=2)

    await cache.position_for(KEY)
    await cache.position_for(OTHER_KEY)
    await cache.position_for(KEY)  # a hit, and it moves KEY to the warm end
    await cache.position_for(THIRD_KEY)  # evicts OTHER_KEY, not KEY

    assert cache.evictions == 1

    await cache.position_for(KEY)
    assert store.reads == 3, "KEY was evicted despite being the most recently used"

    await cache.position_for(OTHER_KEY)
    assert store.reads == 4


async def test_a_refresh_does_not_promote_an_entry() -> None:
    """⚠️ RECENCY MEANS "RECENTLY REQUESTED", NOT "RECENTLY CHANGED", AND THE DIFFERENCE MATTERS.

    Promoting on refresh would hand the event stream control of the eviction order. A position that
    churns constantly and is read once would stay resident forever, while a position read every
    second but rarely changed would be evicted for being calm — so the cache's hit rate would
    degrade as the *stream* got busier rather than as the *traffic* changed, which is the opposite
    of the signal an operator would use to size it.

    Here KEY is refreshed and then evicted anyway, because a refresh is not a use.
    """
    store = ReadCountingStore()
    cache = CachedPositions(store, max_entries=2)

    await cache.position_for(KEY)
    await cache.position_for(OTHER_KEY)
    cache.refresh([_position(KEY, 5)])
    assert cache.refreshed == 1

    await cache.position_for(THIRD_KEY)
    assert cache.evictions == 1

    await cache.position_for(KEY)
    assert store.reads == 4, "the refresh promoted KEY, so the stream is choosing what is cached"


async def test_negative_entries_count_against_the_bound() -> None:
    """The bound exists *because* absences are cached, so exempting them would defeat it."""
    store = ReadCountingStore()
    cache = CachedPositions(store, max_entries=2)

    assert await cache.position_for(KEY) is None
    assert await cache.position_for(OTHER_KEY) is None
    assert await cache.position_for(THIRD_KEY) is None

    assert cache.entries == 2
    assert cache.evictions == 1


async def test_evicting_repeatedly_keeps_the_cache_at_its_bound() -> None:
    """Two hundred keys through a ten-entry cache: the bound is a property, not a moment."""
    store = ReadCountingStore()
    cache = CachedPositions(store, max_entries=10)

    for index in range(200):
        await cache.position_for(PositionKey(counterparty=f"CP-{index:04d}", currency="USD"))
        assert cache.entries <= 10

    assert cache.entries == 10
    assert cache.evictions == 190


async def test_a_max_entries_below_one_is_refused() -> None:
    """A zero-entry cache misses every time and still pays the single-flight bookkeeping.

    Refused rather than accepted-and-degraded, because "caching disabled" is expressed by not
    constructing one — and a configuration typo that silently disables the cache is a performance
    incident whose cause is invisible.
    """
    with pytest.raises(ValueError, match="at least 1"):
        CachedPositions(ReadCountingStore(), max_entries=0)

    with pytest.raises(ValueError, match="at least 1"):
        CachedPositions(ReadCountingStore(), max_entries=-1)


async def test_the_default_bound_is_the_documented_constant() -> None:
    """Asserted through the signature rather than by filling the cache 4096 times.

    A behavioural version of this test would load four thousand positions to observe one eviction,
    and it would take longer than the entire rest of the suite to assert something the signature
    states directly. The fact being pinned is that the default *is* the exported constant, so a
    hardcoded number here cannot drift from the one the documentation names.
    """
    default = inspect.signature(CachedPositions.__init__).parameters["max_entries"].default
    assert default is DEFAULT_MAX_ENTRIES

    # And a floor on the constant itself, because the assertion above is satisfied by *any* value —
    # including one small enough to evict an ordinary dashboard's working set on every refresh,
    # which would show up as a mysteriously useless cache rather than as a failure. The number is a
    # judgement call; "at least a thousand distinct counterparties fit" is the judgement.
    assert DEFAULT_MAX_ENTRIES >= 1024


# ---------------------------------------------------------------------------------
# 8. Refresh: invalidation by version, carrying the new value
# ---------------------------------------------------------------------------------


async def test_a_change_updates_a_cached_entry_in_place() -> None:
    """No eviction, no reload — the notification already carries the value.

    Dropping the entry instead would create a miss at the exact moment the position is being written
    to, which is when it is hottest, so a busy position would stampede on every change and
    single-flight would spend its life absorbing traffic the cache created for itself.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    cache = CachedPositions(store)
    await cache.position_for(KEY)

    cache.refresh([_position(KEY, 2, amount="500.00")])
    updated = await cache.position_for(KEY)

    assert updated is not None
    assert updated.version == 2
    assert updated.pending.amount == Decimal("500.00")
    assert store.reads == 1, "the refresh caused a reload instead of updating in place"
    assert cache.refreshed == 1


async def test_a_change_for_an_uncached_key_is_dropped_rather_than_inserted() -> None:
    """⚠️ A CACHE IS POPULATED BY READS AND UPDATED BY WRITES. NEVER POPULATED BY WRITES.

    Inserting would look like free warming — the value is right there, and the alternative is a miss
    later. It also hands the event stream the decision about what the cache holds: an hour of
    traffic on positions nobody queries would evict the working set the API is actually serving.
    Since a live stream touches every position and the cache holds only what was read, the *large
    majority* of notifications land here, and that is correct.
    """
    store = ReadCountingStore()
    cache = CachedPositions(store)

    cache.refresh([_position(KEY, 1)])

    assert cache.entries == 0
    assert cache.refreshed == 0
    assert cache.refreshes_dropped_cold == 1


async def test_a_change_older_than_the_cached_version_is_refused() -> None:
    """Should be unreachable, and checked anyway, because the cache cannot see the guarantee.

    Per-key publish order is version order only because the projector notifies under a lock
    (ADR-022) — a fact this layer has no way to verify from where it sits. A cache that trusts an
    invariant it cannot check is a cache that starts going backwards silently on the day the
    invariant moves, and `refreshes_dropped_stale` is the alarm for exactly that day.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 5))
    cache = CachedPositions(store)
    await cache.position_for(KEY)

    cache.refresh([_position(KEY, 4, amount="0.01")])

    current = await cache.position_for(KEY)
    assert current is not None
    assert current.version == 5
    assert cache.refreshes_dropped_stale == 1
    assert cache.refreshed == 0


async def test_a_change_at_the_cached_version_is_refused() -> None:
    """The comparison is strict, so a redelivered notification is not a write.

    Equal versions mean equal content (ADR-009), so applying one would be harmless — and refusing it
    keeps the counters honest, which is the actual point: `refreshed` should mean "the cache learnt
    something", not "a message arrived".
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 3))
    cache = CachedPositions(store)
    await cache.position_for(KEY)

    cache.refresh([_position(KEY, 3)])

    assert cache.refreshed == 0
    assert cache.refreshes_dropped_stale == 1


async def test_one_batch_refreshes_every_cached_position_it_touches() -> None:
    """A cross-position move publishes two positions in one notification; both must land."""
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1), checkpoint=1)
    await _put(store, _position(OTHER_KEY, 1), checkpoint=2)
    cache = CachedPositions(store)
    await cache.position_for(KEY)
    await cache.position_for(OTHER_KEY)

    cache.refresh([_position(KEY, 2), _position(OTHER_KEY, 2)])

    mine = await cache.position_for(KEY)
    theirs = await cache.position_for(OTHER_KEY)
    assert mine is not None and mine.version == 2
    assert theirs is not None and theirs.version == 2
    assert cache.refreshed == 2
    assert store.reads == 2


# ---------------------------------------------------------------------------------
# 9. The read-through / invalidate race — problem 4
# ---------------------------------------------------------------------------------


async def test_a_change_arriving_during_a_load_is_not_lost() -> None:
    """⚠️ THE CLASSIC BUG IN THIS PATTERN, AND IT IS INVISIBLE WITHOUT A GATED STORE.

    The interleaving: a load reads version 1; a change to version 2 arrives while the read is still
    in flight; the refresh finds nothing cached, so it correctly declines to insert; and *then* the
    load stores version 1. The cache now serves version 1 with no signal that it is wrong, and it
    will keep doing so until the next change to that position — for a quiet position, indefinitely.

    The fix is version comparison at the end of the load rather than trust in arrival order: the
    change is held while the load is in flight, and whichever value is newer wins. Note that this is
    not "the held change always wins" — see the next test.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    reader = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.in_flight == 1)

    cache.refresh([_position(KEY, 2, amount="900.00")])

    assert cache.refreshes_held_for_load == 1
    assert cache.entries == 0, "the refresh inserted an entry for a key nobody has read"
    assert cache.refreshed == 0
    # Held, and *only* held: falling through to the cold-drop branch as well would count the same
    # notification twice and make the two counters unusable for working out where changes went.
    assert cache.refreshes_dropped_cold == 0

    store.gate.set()
    result = await reader

    assert result is not None
    assert result.version == 2, "the caller was served the stale value the store returned"
    assert cache.loads_superseded == 1

    cached = await cache.position_for(KEY)
    assert cached is not None
    assert cached.version == 2, "a stale value was cached, and is served until the next change"
    assert store.reads == 1


async def test_a_held_change_older_than_the_load_does_not_win() -> None:
    """The held change is compared, not preferred. The store may already have seen the newer value.

    Both orderings are legitimate: the commit may land before the read or after it. If before, the
    read returns version 3 and the notification for version 3 is a duplicate — so a rule of "the
    held change wins" would push the cache *backwards* on an ordinary interleaving.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 3))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    reader = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.in_flight == 1)

    cache.refresh([_position(KEY, 2, amount="0.01")])
    store.gate.set()
    result = await reader

    assert result is not None
    assert result.version == 3
    assert cache.loads_superseded == 0


async def test_the_newest_of_several_held_changes_is_the_one_kept() -> None:
    """One slot per key, so two changes during one load have to be resolved as they arrive.

    A busy position can move twice while a single read is outstanding, and the held slot is not a
    queue — there is no point keeping version 2 once version 3 exists, because the cache only ever
    serves the latest. So `refresh` compares against whatever is already held and keeps the newer,
    which also means the arithmetic does not depend on the order the notifications arrive in.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    reader = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.in_flight == 1)

    cache.refresh([_position(KEY, 3, amount="300.00")])
    cache.refresh([_position(KEY, 2, amount="200.00")])  # out of order on purpose

    assert cache.refreshes_held_for_load == 2

    store.gate.set()
    result = await reader

    assert result is not None
    assert result.version == 3, "an older held change displaced a newer one"
    assert result.pending.amount == Decimal("300.00")


async def test_a_held_change_leaves_nothing_behind_once_the_load_finishes() -> None:
    """A held change that outlived its load would be applied to some unrelated later load.

    Both bookkeeping maps are cleared in `_load`'s `finally`, so the state is bounded by the number
    of loads in flight — which is the only bound available, since the keys are caller-chosen.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    reader = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.in_flight == 1)
    cache.refresh([_position(KEY, 2)])
    store.gate.set()
    await reader

    assert cache.in_flight == 0
    assert cache.as_dict()["refreshes_held_for_load"] == 1

    # A second load of the same key must see the store's value, not the change held for the first.
    cache.invalidate(KEY)
    store.gate = None
    again = await cache.position_for(KEY)
    assert again is not None
    assert again.version == 1, "a held change survived its load and was applied to the next one"


async def test_a_held_change_is_discarded_when_the_load_fails() -> None:
    """A failed load caches nothing — including the change that arrived while it was running.

    Applying the held change would be a way of caching a value the store was never successfully
    asked for, on a key that has no entry. It happens to be a real value, which is what makes it
    tempting; it is still an entry created by a failure, and the rule is that a failure teaches the
    cache nothing.
    """
    store = ReadCountingStore()
    store.gate = asyncio.Event()
    store.fail_with = RuntimeError("transient")
    cache = CachedPositions(store)

    reader = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.in_flight == 1)
    cache.refresh([_position(KEY, 2)])
    store.gate.set()

    with pytest.raises(RuntimeError):
        await reader

    assert cache.entries == 0
    assert cache.in_flight == 0
    assert cache.refreshes_held_for_load == 1

    # And it is *gone*, not merely unused: the next load of the same key must see the store's
    # answer. A held change that survived its failed load would be applied to whatever load came
    # next, so a value nobody asked for would appear in the cache on a key with no entry — an entry
    # created by a failure, which is exactly what the rule above forbids.
    store.fail_with = None
    store.gate = None
    assert await cache.position_for(KEY) is None, "a held change outlived the load that failed"
    assert cache.loads_superseded == 0


# ---------------------------------------------------------------------------------
# 10. Explicit invalidation, and the generation that gives it teeth
# ---------------------------------------------------------------------------------


async def test_invalidate_drops_an_entry_so_the_next_read_reloads() -> None:
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    cache = CachedPositions(store)
    await cache.position_for(KEY)

    cache.invalidate(KEY)

    assert cache.entries == 0
    assert cache.invalidations == 1
    assert await cache.position_for(KEY) is not None
    assert store.reads == 2


async def test_invalidating_an_uncached_key_is_harmless() -> None:
    """Idempotent, because the callers are operational scripts and retries are how those are run."""
    cache = CachedPositions(ReadCountingStore())

    cache.invalidate(KEY)
    cache.invalidate(KEY)

    assert cache.entries == 0
    assert cache.invalidations == 0


async def test_clear_drops_everything_and_counts_what_it_dropped() -> None:
    """Counted so that an unexplained hit-rate collapse has an explanation in the same report."""
    store = ReadCountingStore()
    cache = CachedPositions(store)
    await cache.position_for(KEY)
    await cache.position_for(OTHER_KEY)

    cache.clear()

    assert cache.entries == 0
    assert cache.invalidations == 2


async def test_a_load_in_flight_when_the_cache_is_cleared_does_not_refill_it() -> None:
    """⚠️ WITHOUT THE GENERATION CHECK, `clear()` IS A SUGGESTION.

    The sequence: a load is reading the store, an operator clears the cache because the store was
    restored from a backup, and the load then writes its pre-restore answer into the empty cache.
    The clear appeared to work — the entry count went to zero — and one stale value came straight
    back, which is the single hardest kind of cache bug to reproduce.

    The load still returns its value to the caller, and that is correct: the caller asked before the
    clear, the read really happened, and failing the request would be inventing an error. It just
    does not get to teach the cache anything.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    reader = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.in_flight == 1)

    cache.clear()
    store.gate.set()
    result = await reader

    assert result is not None, "the caller's own read was thrown away"
    assert result.version == 1
    assert cache.entries == 0, "the cleared cache was refilled by a load already in flight"
    assert cache.loads_discarded == 1


async def test_a_load_in_flight_when_its_key_is_invalidated_does_not_refill_it() -> None:
    """`invalidate` bumps the same generation as `clear`, for the same reason.

    Bumping a single global generation means a one-key invalidation also discards other keys' loads
    in flight. That is wasteful — they will be re-read on the next request — and never wrong, and it
    is the right trade here because invalidation is an operational rescue path, not a hot one. A
    per-key generation would be more precise and would add a second map to keep bounded for no
    benefit on the path that actually gets traffic.
    """
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    store.gate = asyncio.Event()
    cache = CachedPositions(store)

    reader = asyncio.create_task(cache.position_for(KEY))
    await _until(lambda: cache.in_flight == 1)

    cache.invalidate(KEY)
    store.gate.set()
    await reader

    assert cache.entries == 0
    assert cache.loads_discarded == 1


# ---------------------------------------------------------------------------------
# 11. The report
# ---------------------------------------------------------------------------------


async def test_the_hit_rate_is_none_before_any_traffic() -> None:
    """`None` and not `0.0`, for ADR-028's reason in miniature.

    A cache with no traffic and a cache that is failing to hit both report 0% if the default is
    zero, and the second is an incident. A null renders as a gap on a dashboard, which is what "no
    data" should look like — the same refusal to publish a number that means something else.
    """
    cache = CachedPositions(ReadCountingStore())

    assert cache.hit_rate is None
    assert cache.as_dict()["hit_rate"] is None
    # And `__repr__` has to survive it. Formatting `None` as a percentage raises `TypeError`, so a
    # repr without the null branch would blow up inside whatever was already going wrong — a fresh
    # cache appearing in a log line or an assertion message is the most likely place for that.
    assert "hit_rate=-" in repr(cache)


async def test_the_hit_rate_counts_a_negative_hit_as_a_hit() -> None:
    """A served 404 is a store read avoided, which is exactly what the hit rate measures."""
    store = ReadCountingStore()
    cache = CachedPositions(store)

    await cache.position_for(KEY)  # miss
    await cache.position_for(KEY)  # negative hit
    await cache.position_for(KEY)  # negative hit

    assert cache.hit_rate == pytest.approx(2 / 3)


async def test_the_report_has_a_stable_set_of_keys() -> None:
    """Every counter every time, including the zeros.

    An absent key makes a dashboard panel disappear exactly when the thing it watches stops
    happening, which looks identical to the panel being broken — the same argument that
    pre-populates `ProjectionStats.skips` with every skip reason.
    """
    cache = CachedPositions(ReadCountingStore())

    assert set(cache.as_dict()) == REPORT_KEYS

    await cache.position_for(KEY)
    assert set(cache.as_dict()) == REPORT_KEYS


async def test_the_report_reflects_what_happened() -> None:
    store = ReadCountingStore()
    await _put(store, _position(KEY, 1))
    cache = CachedPositions(store, max_entries=8)
    await cache.position_for(KEY)
    await cache.position_for(KEY)

    report = cache.as_dict()

    assert report["entries"] == 1
    assert report["max_entries"] == 8
    assert report["hits"] == 1
    assert report["misses"] == 1
    assert report["loads"] == 1
    assert report["in_flight"] == 0
    assert report["hit_rate"] == pytest.approx(0.5)


async def test_the_repr_names_the_numbers_worth_seeing() -> None:
    """A cache appears in assertion failures, where the default repr is no help at all."""
    store = ReadCountingStore()
    cache = CachedPositions(store, max_entries=16)
    await cache.position_for(KEY)

    text = repr(cache)

    assert "entries=1/16" in text
    assert "loads=1" in text
    assert "in_flight=0" in text


# ---------------------------------------------------------------------------------
# 12. Composing listeners, and the whole thing wired to a real projector
# ---------------------------------------------------------------------------------


async def test_combine_listeners_notifies_every_listener_in_the_order_given() -> None:
    calls: list[str] = []

    async def first(positions: tuple[Position, ...]) -> None:
        calls.append("first")

    async def second(positions: tuple[Position, ...]) -> None:
        calls.append("second")

    await combine_listeners(first, second)((_position(KEY, 1),))

    assert calls == ["first", "second"]


async def test_combine_listeners_with_no_listeners_is_a_no_op() -> None:
    """So a service configured with neither a cache nor a hub needs no branch at the wiring site."""
    await combine_listeners()((_position(KEY, 1),))


async def test_combine_listeners_runs_the_rest_when_one_raises() -> None:
    """⚠️ THE BARE LOOP IS THE BUG, AND ITS COST IS PERMANENT.

    Stopping at the first failure means: the cache refresh raises, `hub.publish` never runs, no
    subscriber hears about the change. The projector propagates, the consumer retries the event, and
    `project` returns `DUPLICATE` — correctly, because the commit already happened — so the
    notification is never regenerated. One transient failure in the cache costs every live
    subscriber that update, forever.
    """
    reached: list[str] = []

    async def broken(positions: tuple[Position, ...]) -> None:
        raise RuntimeError("the cache is unhappy")

    async def after(positions: tuple[Position, ...]) -> None:
        reached.append("after")

    with pytest.raises(RuntimeError, match="unhappy"):
        await combine_listeners(broken, after)((_position(KEY, 1),))

    assert reached == ["after"], "a failure in the first listener silenced the second"


async def test_combine_listeners_reraises_a_single_failure_unwrapped() -> None:
    """One failure stays catchable by its own type, so a fan-out does not change error handling."""

    async def broken(positions: tuple[Position, ...]) -> None:
        raise ValueError("just the one")

    with pytest.raises(ValueError, match="just the one"):
        await combine_listeners(broken)((_position(KEY, 1),))


async def test_combine_listeners_groups_several_failures() -> None:
    """Two failures cannot both be raised, and neither should be silently dropped."""

    async def broken_first(positions: tuple[Position, ...]) -> None:
        raise RuntimeError("first")

    async def broken_second(positions: tuple[Position, ...]) -> None:
        raise ValueError("second")

    with pytest.raises(ExceptionGroup) as caught:
        await combine_listeners(broken_first, broken_second)((_position(KEY, 1),))

    assert [type(error) for error in caught.value.exceptions] == [RuntimeError, ValueError]


async def test_combine_listeners_propagates_cancellation_immediately() -> None:
    """A cancelled task should stop, not finish the fan-out first.

    Which is why the `except` catches `Exception` and not `BaseException`: "collect the error and
    carry on" is the right answer for a listener that failed and exactly the wrong answer for a task
    that has been told to stop.
    """
    reached: list[str] = []

    async def cancelled(positions: tuple[Position, ...]) -> None:
        raise asyncio.CancelledError

    async def after(positions: tuple[Position, ...]) -> None:
        reached.append("after")

    with pytest.raises(asyncio.CancelledError):
        await combine_listeners(cancelled, after)((_position(KEY, 1),))

    assert reached == []


async def test_a_real_event_refreshes_a_cached_position_with_no_reload() -> None:
    """The whole slice, end to end: ingest, cache, ingest again, and the cache is already right.

    Nothing is stubbed here. A real event goes through the real decoder into the real projector,
    which commits to a real store and notifies the cache, and the second read is a hit holding the
    new version. That is the property the slice claims, and it is only checkable with all four
    layers present.
    """
    store = ReadCountingStore()
    cache = CachedPositions(store)
    projector = Projector(store, on_change=cache.on_change)

    await projector.ingest(envelope_payload(event_id=1))
    first = await cache.position_for(KEY)
    assert first is not None
    assert first.version == 1
    assert first.pending.amount == Decimal(AMOUNT)

    await projector.ingest(envelope_payload(event_id=2, event_type=EVENT_SETTLED))
    reads_before = store.reads

    second = await cache.position_for(KEY)

    assert second is not None
    assert second.version == 2
    assert second.settled.amount == Decimal(AMOUNT)
    assert second.pending.amount == Decimal("0.00")
    assert store.reads == reads_before, "the cache reloaded a position the notification carried"
    assert cache.refreshed == 1


async def test_the_cache_and_the_hub_share_one_projector() -> None:
    """Both consumers of the change stream, wired the way slice 5 will wire them."""
    store = ReadCountingStore()
    cache = CachedPositions(store)
    hub = SubscriptionHub()
    projector = Projector(store, on_change=combine_listeners(cache.on_change, hub.publish))

    await projector.ingest(envelope_payload(event_id=1))
    cached = await cache.position_for(KEY)
    assert cached is not None
    assert cached.version == 1

    subscription = hub.subscribe(keys=[KEY])
    await projector.ingest(envelope_payload(event_id=2, event_type=EVENT_SETTLED))

    assert cache.refreshed == 1
    assert subscription.pending_count == 1

    batch = subscription.drain_nowait()
    assert [position.version for position in batch] == [2]

    reads_before = store.reads
    served = await cache.position_for(KEY)
    assert served is not None
    assert served.version == 2, "a subscriber was told about a version the cache would not serve"
    assert store.reads == reads_before


async def test_a_no_op_transition_neither_refreshes_the_cache_nor_wakes_a_subscriber() -> None:
    """The `is` check in `_build_changed_positions`, observed from two layers away.

    A `validated` event after a `received` produces deltas of `-1500.75 pending` and
    `+1500.75 pending`, which net to nothing, so `with_deltas` returns the same object and the
    projector publishes no change at all. This is the test that shows what that identity check is
    worth: without it the cache would be rewritten and every subscriber woken, once per instruction,
    for an update containing no visible difference.
    """
    store = ReadCountingStore()
    cache = CachedPositions(store)
    hub = SubscriptionHub()
    projector = Projector(store, on_change=combine_listeners(cache.on_change, hub.publish))

    await projector.ingest(envelope_payload(event_id=1))
    await cache.position_for(KEY)
    subscription = hub.subscribe()

    await projector.ingest(envelope_payload(event_id=2, event_type=EVENT_VALIDATED))

    assert projector.stats.applied == 2
    assert projector.stats.changed == 1, "the no-op transition was counted as a change"
    assert cache.refreshed == 0
    assert cache.refreshes_dropped_stale == 0
    assert subscription.pending_count == 0


async def test_the_cache_serves_a_projection_it_never_writes_to() -> None:
    """A last structural sanity check: the store's own state is unaffected by cache activity.

    Reading through the cache a thousand times must leave the projection's numbers exactly where the
    events left them. Obvious, and worth one assertion, because the failure it would catch — a cache
    that writes back — is the one this file's first two tests exist to prevent.
    """
    store = ReadCountingStore()
    cache = CachedPositions(store)
    projector = Projector(store, on_change=cache.on_change)
    await projector.ingest(envelope_payload(event_id=1))

    before = await store.all_positions()
    snapshots_before = await store.snapshot_count()
    for _ in range(1000):
        await cache.position_for(KEY)
        await cache.position_for(OTHER_KEY)

    assert await store.all_positions() == before
    assert await store.snapshot_count() == snapshots_before
    assert await store.checkpoint() == 1


async def test_the_cache_survives_an_unknown_instruction_id_being_asked_for() -> None:
    """A guard against the cache reaching for anything other than `position_for`.

    `uuid` is imported here for one reason: to assert that a cache which has never been given a
    snapshot id cannot be asked for one. If `snapshot_for` ever appears on the surface, this test's
    sibling above fails first — this one just makes the intent explicit at the point of use.
    """
    cache: Any = CachedPositions(ReadCountingStore())

    with pytest.raises(AttributeError):
        await cache.snapshot_for(uuid.UUID("11111111-1111-4111-8111-111111111111"))
