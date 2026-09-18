"""The composition root, and the gate that makes read-your-writes possible.

WHY A `Service` OBJECT RATHER THAN MODULE-LEVEL GLOBALS
---------------------------------------------------------------------------------
The four collaborators — store, projector, cache, hub — have to be wired together in exactly one
way, and the wiring is the part with a bug in it if anything is. Module-level singletons would make
that wiring implicit and, much worse, would make it *shared*: two tests in the same process would
observe each other's positions, so a test would pass or fail depending on the file it lives in.

Everything here is constructed per `Service`, and `create_app` takes one. So a test builds a
service, drives it, and throws it away; production builds exactly one in the lifespan hook. Same
code path, no "if testing" branch anywhere.

WHY THE WIRING LIVES IN THE API LAYER
---------------------------------------------------------------------------------
Because the api layer is the only layer allowed to know about all four, and because composition is a
decision about *this deployment* rather than about any of the parts. `projection/` deliberately does
not know that a cache exists (ADR-038), `cache/` does not know that a hub exists, and `push/` knows
nothing about either. The single line that joins them —

    on_change=combine_listeners(self.cache.on_change, self.hub.publish)

— is the whole integration, and it is here because here is where the reader looks for it.
"""

from __future__ import annotations

import asyncio
from typing import Any, Final

from position_view.cache import DEFAULT_MAX_ENTRIES, CachedPositions
from position_view.model import Outcome
from position_view.projection import (
    NO_CHECKPOINT,
    InMemoryPositionStore,
    PositionStore,
    Projector,
    Refused,
    combine_listeners,
)
from position_view.push import DEFAULT_MAX_PENDING, SubscriptionHub

#: How long a read-your-writes request waits by default, and the most it may ask for.
#:
#: A default rather than "wait as long as it takes", because a request that waits indefinitely holds
#: a connection, a worker slot and a client's patience while the thing it is waiting for may never
#: arrive — the event it names might not exist yet, or at all. A ceiling as well as a default,
#: because the timeout arrives in the query string: without a maximum, `?timeout_ms=600000` is a way
#: for any caller to pin ten minutes of server resources per request, which is a denial of service
#: with a perfectly innocent-looking URL.
DEFAULT_AWAIT_TIMEOUT_MS: Final = 1000
MAX_AWAIT_TIMEOUT_MS: Final = 5000


class CheckpointGate:
    """Lets a reader wait until the projection has caught up to a particular `event_id`.

    ⚠️ THIS IS WHAT MAKES A CQRS READ MODEL USABLE BY THE CLIENT THAT JUST WROTE.
       The write side accepts a settlement instruction and returns; the read model finds out
       eventually. So the client's very next read — the page refresh right after the button click —
       is the one read most likely to show stale data, and "eventually consistent" is exactly no
       comfort to the person looking at a screen that has not changed. This is the standard
       complaint about CQRS, and it is not inherent: it is what happens when the read side offers no
       way to say *how* fresh an answer has to be.

       The write side hands the client the `event_id` it produced. The client passes it back as
       `?await_event_id=`, and this gate turns that into "block until the projection has passed
       that point, then answer". Elsewhere in the same API, a caller that does not care pays
       nothing: no waiting, no coordination, no slower path.

    WHY A FUTURE PER WAITER AND NOT A POLLING LOOP
    ---------------------------------------------------------------------------------
    The obvious implementation is `while await store.checkpoint() < event_id: await sleep(0.005)`.
    It works, and it has three costs that get worse exactly when the service is busy: a store read
    per waiter per tick, a latency floor of half a tick even when the data is already there, and a
    `sleep` in the request path — which puts a clock into every test that exercises freshness, so
    the suite either becomes slow or becomes flaky.

    A future per waiter has none of those. `advance` is called once per ingested event by the code
    that ingested it, so nothing polls, and a waiter whose event has already arrived returns without
    suspending at all.

    WHY THE COUNTER IS MONOTONIC (`max`, NOT ASSIGNMENT)
    ---------------------------------------------------------------------------------
    Same reason the store's checkpoint clamps: `advance` is called with whatever the store reports
    after an event, and two concurrent ingests can return those reports out of order. A plain
    assignment would let the gate go backwards, and a reader already released by the higher value
    would then be re-blocked by the lower one — a request that hangs for no reason a log line could
    explain.
    """

    def __init__(self) -> None:
        #: The highest checkpoint the projection is known to have reached.
        self._reached = NO_CHECKPOINT

        #: One entry per waiting request. A list rather than a heap or a sorted structure: the
        #: expected length is "the number of requests currently waiting for freshness", which is
        #: small, and a list keeps `advance` — the hot path, called once per event — a single pass
        #: with no bookkeeping to get wrong.
        self._waiters: list[tuple[int, asyncio.Future[None]]] = []

        #: Traffic, for `/projection`. `already_satisfied` is the interesting one: a high ratio
        #: means clients are asking for freshness they already had, so the parameter is costing them
        #: nothing and can stay. A high `timed_out` means either the projection is behind or callers
        #: are naming event ids the stream has not carried, and those need different fixes.
        self.waits = 0
        self.waits_already_satisfied = 0
        self.waits_timed_out = 0

    @property
    def reached(self) -> int:
        return self._reached

    @property
    def waiting(self) -> int:
        return len(self._waiters)

    def advance(self, checkpoint: int) -> None:
        """Record progress and release every waiter it satisfies. Synchronous, and never suspends.

        Synchronous because it is called from the ingest path immediately after a commit, and a
        suspension there would let a second event interleave with the first one's bookkeeping.
        `Future.set_result` does not run the waiting coroutine inline — it schedules it — so no
        waiter's code can run inside this loop, which is what makes iterating the list safe without
        a lock.
        """
        self._reached = max(self._reached, checkpoint)

        # The early return is not just an optimisation. The common case is that nothing is waiting,
        # and rebuilding the list on every event would allocate once per message for nothing.
        if not self._waiters:
            return

        still_waiting: list[tuple[int, asyncio.Future[None]]] = []
        for event_id, waiter in self._waiters:
            if waiter.done():
                # Cancelled by a disconnecting client, or already resolved. `wait_for`'s own
                # `finally` removes it too, but this loop must tolerate finding it: the removal and
                # this iteration are not ordered with respect to each other.
                continue
            if event_id <= self._reached:
                waiter.set_result(None)
            else:
                still_waiting.append((event_id, waiter))
        self._waiters = still_waiting

    async def wait_for(self, event_id: int, *, timeout_ms: int) -> bool:
        """True if the projection reached `event_id`, False if the wait timed out.

        A returned bool rather than a raised exception, because "not yet" is not an error at this
        level — it is an answer the caller has to turn into an HTTP status, and which status that is
        belongs to the route, not to the gate.
        """
        self.waits += 1

        # The fast path, and it is the path that matters: a client that reads a moment after writing
        # usually finds the projection already past its event. Returning here means the freshness
        # parameter costs nothing at all in the common case — not even a trip through the event
        # loop, so it cannot reorder anything either.
        if event_id <= self._reached:
            self.waits_already_satisfied += 1
            return True

        waiter: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        entry = (event_id, waiter)
        self._waiters.append(entry)
        try:
            await asyncio.wait_for(waiter, timeout=timeout_ms / 1000)
        except TimeoutError:
            self.waits_timed_out += 1
            return False
        finally:
            # ⚠️ THE `finally` IS WHAT KEEPS THIS FROM LEAKING ONE FUTURE PER ABANDONED REQUEST.
            #    A timeout and a client disconnect both leave the entry in the list, and the entries
            #    that are never removed are exactly the ones for event ids the stream never carries
            #    — so the leak grows fastest under the condition that produced it. Removed here
            #    rather than trusted to `advance`, because `advance` only ever visits waiters it can
            #    resolve.
            if entry in self._waiters:
                self._waiters.remove(entry)
        return True

    def as_dict(self) -> dict[str, Any]:
        """The freshness numbers, folded into `/projection` by the route that serves it."""
        return {
            "reached": self._reached,
            "waiting": len(self._waiters),
            "waits": self.waits,
            "waits_already_satisfied": self.waits_already_satisfied,
            "waits_timed_out": self.waits_timed_out,
        }

    def __repr__(self) -> str:
        return f"CheckpointGate(reached={self._reached}, waiting={len(self._waiters)})"


class Service:
    """Everything the HTTP layer serves from, wired together once.

    The store is injectable and defaults to the in-memory one, which is the whole reason this repo
    runs end to end with nothing installed. A real deployment passes a Postgres-backed
    `PositionStore` and changes nothing else — the projector, the cache and the hub are all written
    against the protocol, and `test_store.py` is the contract they were written against.
    """

    def __init__(
        self,
        store: PositionStore | None = None,
        *,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        max_pending: int = DEFAULT_MAX_PENDING,
    ) -> None:
        self.store: PositionStore = InMemoryPositionStore() if store is None else store
        self.cache = CachedPositions(self.store, max_entries=max_entries)
        self.hub = SubscriptionHub(max_pending=max_pending)
        self.gate = CheckpointGate()

        # ⚠️ THE ORDER OF THESE TWO LISTENERS IS A CONVENTION, AND ADR-047 EXPLAINS WHY IT IS NOT
        #    CURRENTLY LOAD-BEARING: neither of them suspends, so nothing can observe the moment
        #    between them. It is written cache-first anyway, so that the day one of them grows an
        #    `await` the order is already the one where a reader cannot be told about a version the
        #    cache has not yet accepted.
        self.projector = Projector(
            self.store,
            on_change=combine_listeners(self.cache.on_change, self.hub.publish),
        )

    async def start(self) -> None:
        """Seed the gate from the store. Called by the lifespan hook before the first request.

        Without this, a process that restarts against a store already holding a checkpoint of 900
        would treat `?await_event_id=800` as "not reached yet" and block until the timeout, on data
        it already had. The gate's whole job is to describe the store, so it has to start by asking.
        """
        self.gate.advance(await self.store.checkpoint())

    async def ingest(self, payload: dict[str, Any]) -> Outcome | Refused:
        """Apply one event, then let the freshness gate through.

        ⚠️ THE GATE IS ADVANCED *AFTER* `projector.ingest` RETURNS, AND THAT ORDERING IS THE WHOLE
           CORRECTNESS ARGUMENT FOR READ-YOUR-WRITES.
           By the time `ingest` returns, the commit has happened *and* the listeners have run — so
           the cache already holds the new version and every subscriber has been offered it. Only
           then is the waiter released. Advance the gate any earlier and a released reader could
           observe the cache mid-update: it would be told the projection had passed event 7 and then
           be handed version 6, which is precisely the lie the parameter exists to prevent, and it
           would be intermittent enough to be blamed on the client.
        """
        outcome = await self.projector.ingest(payload)
        self.gate.advance(await self.store.checkpoint())
        return outcome

    def close(self) -> None:
        """Close every subscription so no reader task is left parked. Called on shutdown.

        The cache is deliberately not cleared: it holds no resources, and clearing it would only
        matter if something were going to read it afterwards, which is exactly what shutdown means
        is not going to happen.
        """
        self.hub.close()

    def __repr__(self) -> str:
        return (
            f"Service(store={type(self.store).__name__}, subscribers={self.hub.subscribers}, "
            f"cached={self.cache.entries}, checkpoint_reached={self.gate.reached})"
        )
