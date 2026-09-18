"""A read-through cache in front of one store read, and the four ways that goes wrong.

WHAT THIS CACHES, AND WHY IT IS EXACTLY ONE METHOD
---------------------------------------------------------------------------------
`CachedPositions` caches `PositionStore.position_for(key)` and nothing else. Not
`all_positions`, not `snapshot_for`, not `checkpoint`. Each omission is a decision:

  - `all_positions` is invalidated by *any* change to *any* position, so a cache of it on a live
    stream would spend its life being thrown away. A cache with a near-100% miss rate is a memory
    cost with a coherence bug attached.
  - `snapshot_for` is read by exactly one caller — the projector — and that caller must never read
    through a cache. See the warning below; this is the most dangerous thing in the file.
  - `checkpoint` is a single integer read from the store on demand, and slice 5's read-your-writes
    endpoint compares against it. Caching the number that answers "are you up to date yet?" is a
    contradiction in terms.

⚠️ THIS CLASS IS DELIBERATELY *NOT* A `PositionStore`, AND A TEST KEEPS IT THAT WAY.
   Making it satisfy the protocol would be a two-line change — add `commit`, `all_positions`,
   `snapshot_for`, `checkpoint`, `snapshot_count` as passthroughs — and it would read like an
   improvement, because then the api layer could take a single `PositionStore` and be handed either
   one. It would also arm the worst bug this repository can contain.

   The moment `CachedPositions` is substitutable for a store, somebody writes:

       projector = Projector(CachedPositions(store))

   and the projector's read-modify-write cycle now reads *cached* positions.
   `_build_changed_positions` reads a position, applies deltas, and commits the result. Read a stale
   version and the deltas are applied to the wrong base, so the committed position is short by
   whatever it missed — and then that wrong value is written to the store, becomes the new truth,
   and is replayed identically forever. `asyncio.Lock` cannot help: the lock serialises access to
   the store, and the stale value came from somewhere the lock does not cover.

   That is a silent money bug produced by a refactor that looks like tidying up. So the surface is
   narrow on purpose, `tests/test_cache.py` asserts the exact set of public names, and adding
   `commit` here means having an argument with a test rather than getting a green build.

THE FOUR PROBLEMS, IN THE ORDER THEY BITE
---------------------------------------------------------------------------------
1. **The stampede.** A hot position expires or is evicted, fifty requests miss at once, and fifty
   identical reads hit the store. The load that was supposed to be protected by the cache arrives
   all at once, at the worst possible moment — which is exactly when the position is popular. Fixed
   by single-flight: the first miss starts one load, every other miss waits on that same load.

2. **The negative gap.** `position_for` returns `None` for a counterparty nothing has ever
   contributed to, and the api layer turns that into a 404. A cache that only stores *hits* sends
   every 404 to the store, so the cheapest way to load this service is to ask for counterparties
   that do not exist. So `None` is cached as a value — which means `dict.get(key)` cannot be used to
   detect a miss, because it returns `None` for both "cached absence" and "not cached". Membership
   is tested explicitly, everywhere.

3. **The unbounded map.** Caching absences makes the key space not "positions that exist" but
   "strings a caller can send". Unbounded, that is a memory leak with a remote trigger. So the cache
   is bounded and evicts least-recently-*requested* first.

4. **The invalidate race.** A read-through cache and an invalidation signal can interleave: a load
   reads version 6, a change to version 7 arrives while the load is still in flight, the
   invalidation finds nothing cached and does nothing, and *then* the load stores version 6. The
   cache is now stale until the next change to that position, which for a quiet position is
   indefinitely. That is the classic bug in this pattern, and `_changed_while_loading` handles it.

WHY INVALIDATION IS BY VERSION AND NOT BY A CLOCK
---------------------------------------------------------------------------------
There is no TTL in this file and no call to a clock. A TTL is what you use when you cannot know when
the underlying value changed — and this service knows exactly when, because the projector already
publishes every changed position to a listener (`ChangeListener`, slice 2). Given a precise signal,
a TTL is a choice to serve a known-wrong answer for `ttl` seconds.

Better than invalidating, the signal carries the *new value*, so the cache updates in place rather
than dropping the entry. Dropping it would create a miss at the exact moment the position is being
written to — which is when it is hottest — so a busy position would stampede on every change, and
single-flight would be doing all its work on traffic the cache created itself.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Iterable
from typing import Any, Final

from ..model import Position, PositionKey
from ..projection import PositionStore

#: How many positions the cache will hold before evicting the coldest.
#:
#: A guess, and a guess with a shape rather than a tuned number: comfortably more than the working
#: set of a dashboard or a handful of API consumers, comfortably less than a whole book. The bound
#: exists because of negative caching (problem 3 above), so the number that matters is "how many
#: distinct keys can a caller invent before this hurts", and any value that is not "unlimited"
#: answers that.
DEFAULT_MAX_ENTRIES: Final = 4096

#: The version a *cached absence* is treated as having, for the version comparison in `refresh`.
#:
#: Zero is safe as a sentinel for the same reason `_empty_position` starts there: a position is
#: published only after `with_deltas` has bumped it, so version 0 never reaches a cache. Any real
#: change is therefore strictly newer than a cached `None`, which is what makes "this counterparty
#: does not exist" upgrade cleanly to "here it is" the first time money lands on it.
UNCACHED_VERSION: Final = 0


class CachedPositions:
    """Read-through, single-flight, bounded, refreshed from the change stream.

    The state is four collections, and the two smaller ones are the interesting ones:

      - `_entries` — the cache proper. `Position | None`, LRU-ordered, bounded.
      - `_in_flight` — one task per key currently being loaded. This is the single-flight register.
      - `_changed_while_loading` — changes that arrived for a key while its load was in flight.
      - `_generation` — bumped by `clear` and `invalidate`, so a load in flight when the cache was
        emptied does not quietly refill it.

    ⚠️ NOTHING IN THIS CLASS AWAITS WHILE HOLDING THE CACHE IN AN INCONSISTENT STATE, AND THAT IS
       WHAT REPLACES A LOCK.
       There is no `asyncio.Lock` here. Every mutation of `_entries`, `_in_flight` and
       `_changed_while_loading` happens in a run of statements with no `await` between them, so no
       other task can observe a half-updated cache — the same argument
       `InMemoryPositionStore.commit` makes, and it holds for the same reason: a coroutine yields
       control only at an `await`.

       The one `await` in the class is the store read inside `_load`, and everything that makes that
       safe — the in-flight register, the generation check, the held-change map — exists because
       that `await` is a window during which the world can change. Adding an `await` anywhere else
       in this file re-opens a window that nothing here is written to survive.
    """

    def __init__(self, store: PositionStore, *, max_entries: int = DEFAULT_MAX_ENTRIES) -> None:
        if max_entries < 1:
            # A zero-entry cache is not a disabled cache — it is a cache that misses every time and
            # still pays for the single-flight bookkeeping. If the intent is "no caching", the right
            # move is to not construct one, and refusing here says so at the call site.
            raise ValueError(f"max_entries must be at least 1, got {max_entries}")

        self._store = store
        self._max_entries = max_entries

        #: An `OrderedDict` rather than a plain dict, purely for `move_to_end` and
        #: `popitem(last=False)`. A plain dict preserves insertion order but offers no way to
        #: promote a key on access or to pop the oldest, and reimplementing either with
        #: `del`/reinsert is two lines of the same thing with a subtle hole in it.
        self._entries: OrderedDict[PositionKey, Position | None] = OrderedDict()

        #: The single-flight register, and also the strong reference that keeps a load alive. A task
        #: nothing holds a reference to can be garbage-collected mid-flight, which produces a load
        #: that silently never finishes — so this dict is load-bearing twice over.
        self._in_flight: dict[PositionKey, asyncio.Task[Position | None]] = {}

        #: Changes that arrived while a load was in flight. Bounded by the number of concurrent
        #: loads, because `_load` clears its own key in a `finally` whether it succeeded or not.
        self._changed_while_loading: dict[PositionKey, Position] = {}

        #: Bumped by `clear` and `invalidate`. A load captures it on entry and compares on exit.
        self._generation = 0

        #: Requests served from a cached position.
        self.hits = 0

        #: Requests served from a cached *absence*. Counted separately because the two say different
        #: things about the caller: a high `hits` is a working cache, and a high `negative_hits` is
        #: either a client polling for something that has not happened yet or somebody enumerating
        #: counterparty names. Summed into one number, neither is visible.
        self.negative_hits = 0

        #: Requests that found no entry, so a load was started or joined.
        self.misses = 0

        #: Misses that joined a load already in flight instead of starting one. This is the number
        #: that says whether single-flight is doing anything: `coalesced` near zero means either
        #: there is no concurrency or the cache is not being missed concurrently, and `coalesced`
        #: much larger than `loads` means the store is being protected from a real stampede.
        self.coalesced = 0

        #: Store reads performed. `misses - coalesced` should equal this, and a test asserts it.
        self.loads = 0

        #: Store reads that raised. See `_load` for why a failure is never cached.
        self.load_failures = 0

        #: Loads whose result was replaced by a newer version that arrived mid-flight — problem 4.
        #: Expected to be small and non-zero on a busy stream; permanently zero on a busy stream
        #: means the interleaving is not happening, not that it cannot.
        self.loads_superseded = 0

        #: Loads whose result was thrown away because the cache was cleared or invalidated while
        #: they were in flight.
        self.loads_discarded = 0

        #: Entries updated in place from the change stream.
        self.refreshed = 0

        #: Changes for keys nobody has asked for. Expected to be the large majority: the stream
        #: touches every position, and the cache holds only the ones somebody read.
        self.refreshes_dropped_cold = 0

        #: Changes not newer than what is already cached. Should be zero, because the projector
        #: publishes under a lock and versions only increase — a non-zero value means that
        #: assumption has broken upstream, which is worth knowing rather than worth crashing over.
        self.refreshes_dropped_stale = 0

        #: Changes recorded against an in-flight load rather than applied — problem 4's bookkeeping.
        self.refreshes_held_for_load = 0

        #: Entries dropped by an explicit `invalidate` or `clear`.
        self.invalidations = 0

        #: Entries dropped because the cache was full.
        self.evictions = 0

    # ---------------------------------------------------------------------------------
    # reading
    # ---------------------------------------------------------------------------------

    async def position_for(self, key: PositionKey) -> Position | None:
        """The cached position, the cached absence, or whatever the store says — once.

        The three branches, in the order they are checked:

          1. **Cached.** Return it, and promote the key so eviction sees it as recently used.
          2. **Being loaded.** Wait on the load somebody else started. No second store read.
          3. **Cold.** Start the load, register it, and wait on it like everybody else.

        ⚠️ `await asyncio.shield(load)` RATHER THAN `await load`, AND THE DIFFERENCE IS A REAL BUG.
           Cancelling a task cancels the future it is waiting on. With `await load`, one HTTP client
           disconnecting — which cancels its request task, and is the single most common event in a
           web service — would cancel the *shared* load, and every other waiter on that key would
           get a `CancelledError` for a request they had no part in. One impatient browser tab
           failing forty other requests, intermittently, under load only.

           `shield` gives each waiter its own cancellable wrapper around a load that is not
           cancelled by it. The load still completes and still populates the cache, so the work the
           departed caller paid for is not wasted either.
        """
        if key in self._entries:
            # Membership rather than `.get(key) is not None`, because `None` is a cached *value*
            # here. `.get` would turn every negative hit into a miss, and negative caching would
            # quietly do nothing while the counter said it was working.
            entry = self._entries[key]
            self._entries.move_to_end(key)
            if entry is None:
                self.negative_hits += 1
            else:
                self.hits += 1
            return entry

        self.misses += 1

        load = self._in_flight.get(key)
        if load is None:
            # ⚠️ THE GENERATION IS READ HERE, NOT INSIDE `_load`, AND THE DIFFERENCE IS A REAL
            # WINDOW.    `create_task` schedules a coroutine; it does not start it. So a
            # `_generation` read on    `_load`'s first line happens some number of event-loop ticks
            # *later* — and a `clear()`    landing in that gap would be captured as the load's own
            # starting generation, so the    load would consider itself current and refill the cache
            # it was supposed to be    discarded by. Reading it here makes the capture simultaneous
            # with the decision to    load, which is the moment the generation is meant to be about.
            load = asyncio.create_task(self._load(key, self._generation))
            self._in_flight[key] = load
            # If the load fails and every waiter has gone away, nobody retrieves the exception and
            # asyncio logs "Task exception was never retrieved" at collection time. That log line is
            # noise here — the failure is already counted, and the caller that cared has already
            # seen it re-raised through its shield — so the callback retrieves it deliberately.
            load.add_done_callback(_retrieve_exception)
        else:
            self.coalesced += 1

        return await asyncio.shield(load)

    async def _load(self, key: PositionKey, generation: int) -> Position | None:
        """One store read, then decide whether the answer is still worth keeping.

        `generation` is passed in rather than read here, and `position_for` explains why: this
        coroutine does not begin running at the moment it is created, so reading the counter on the
        first line would miss a `clear()` that landed in between.

        Three things can have happened during the `await`, and each has a branch:

          - The store raised. The key is *not* cached, positively or negatively, and the error is
            re-raised to every waiter.
          - A newer version arrived. It wins, and the caller gets it rather than the older value the
            store returned.
          - The cache was cleared or the key invalidated. The result is returned to the caller but
            not stored, because storing it would undo the clear.

        ⚠️ A FAILED READ IS NEVER CACHED, AND THAT IS THE MOST IMPORTANT LINE IN THIS METHOD.
           The tempting shortcut is to treat any unusable answer as an absence — `except: return
           None` — and it converts a five-second database wobble into a 404 that lasts as long as
           the entry does. A client asking about a counterparty with real exposure is told, with a
           perfectly ordinary status code, that it does not exist. Failing the request instead is
           worse for one caller and honest for all of them, and the retry lands on a cache that has
           learnt nothing false.
        """
        try:
            position = await self._store.position_for(key)
            self.loads += 1

            # Everything from here to the `return` runs with no `await` in it, which is what makes
            # the read-then-write sequence below safe without a lock.
            held = self._changed_while_loading.pop(key, None)
            if held is not None and (position is None or held.version > position.version):
                # Problem 4, resolved: the store read is older than a change that landed while it
                # was in flight. Compared by version rather than assumed by arrival order, because
                # the read may equally have seen the newer value already — in which case the held
                # change is a duplicate and this branch correctly does nothing.
                position = held
                self.loads_superseded += 1

            if generation == self._generation:
                self._remember(key, position)
            else:
                self.loads_discarded += 1

            return position
        except Exception:
            # `Exception` and not `BaseException`: a `CancelledError` is not a store failure, it is
            # this task being torn down, and counting it as one would make an ordinary shutdown look
            # like a database outage.
            self.load_failures += 1
            raise
        finally:
            # Both maps are cleared here rather than in the success path, so a raised load leaves no
            # trace: the key can be loaded again immediately, and a held change cannot linger to be
            # applied to some unrelated later load. Without this, one transient failure would leave
            # a permanently poisoned in-flight entry and every subsequent request for that key would
            # wait on a task that has already finished.
            self._in_flight.pop(key, None)
            self._changed_while_loading.pop(key, None)

    def _remember(self, key: PositionKey, position: Position | None) -> None:
        """Store an entry as the most recently used, evicting the coldest while over the bound."""
        # A new key lands at the warm end of an `OrderedDict` on its own, and there is no
        # `move_to_end` here because this method is only ever reached for a key that is *absent*:
        # `position_for` returns early on a hit, and single-flight means one load per key. So the
        # "promote an existing entry" case cannot arise, and a `move_to_end` guarding against it
        # would be a line no test could distinguish from its own deletion — which is a line that
        # quietly stops meaning anything. The promotion that does matter is on the hit path, in
        # `position_for`; the deliberate *lack* of one is in `refresh`.
        self._entries[key] = position

        # `while` rather than `if`: the bound is a property the cache should hold no matter how the
        # entries got there, and a lowered `max_entries` or a future bulk insert would leave an `if`
        # trimming one entry and calling it done.
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
            self.evictions += 1

    # ---------------------------------------------------------------------------------
    # invalidation
    # ---------------------------------------------------------------------------------

    def refresh(self, positions: Iterable[Position]) -> None:
        """Update cached entries from the change stream. Synchronous, and it inserts nothing.

        ⚠️ A CHANGE FOR A KEY THAT IS NOT CACHED IS DROPPED, NOT INSERTED.
           Inserting would look like free warming — the value is right there — and it would hand the
           event stream control of what the cache holds. A quiet hour of traffic on positions nobody
           queries would evict the working set that the API is actually serving, so the cache's hit
           rate would fall as the *stream* got busier rather than as the *traffic* changed. A cache
           is populated by reads and updated by writes; it is never populated by writes.

        The refreshed entry is also *not* promoted in the LRU order, for the same reason. Recency
        here means "recently requested", not "recently changed" — otherwise a position that churns
        constantly and is read once stays resident forever while a position read every second gets
        evicted for being calm.
        """
        for position in positions:
            key = position.key

            if key in self._in_flight:
                # The load has not finished, so there is no entry to update and nothing cached to
                # compare against. Hold the value: `_load` will compare versions and take the newer.
                held = self._changed_while_loading.get(key)
                if held is None or position.version > held.version:
                    self._changed_while_loading[key] = position
                self.refreshes_held_for_load += 1
                continue

            if key not in self._entries:
                self.refreshes_dropped_cold += 1
                continue

            cached = self._entries[key]
            cached_version = UNCACHED_VERSION if cached is None else cached.version
            if position.version <= cached_version:
                # Should be unreachable given ADR-022 — the projector publishes under a lock, so
                # per-key publish order is version order. Checked anyway, because the cache cannot
                # verify that guarantee from where it sits, and a cache that trusts an invariant it
                # cannot see is a cache that goes backwards silently on the day the invariant moves.
                self.refreshes_dropped_stale += 1
                continue

            self._entries[key] = position
            self.refreshed += 1

    async def on_change(self, positions: tuple[Position, ...]) -> None:
        """The `ChangeListener` adapter. `async` for the signature, and it suspends nowhere.

        ⚠️ NO `await` IN HERE, FOR EXACTLY THE REASON `SubscriptionHub.publish` HAS NONE (ADR-030).
           This runs inside the projector's lock. An `await` here — a metrics push, a distributed
           cache round trip, a log flush — stops the projection until it returns. A test parses this
           method and fails if an `ast.Await` appears in it, because no behavioural test can see the
           difference: the suite would pass, and the projection would stall in production only.

        The signature takes a `tuple` rather than an `Iterable` to match `ChangeListener` exactly,
        so mypy checks the fit at the point of use rather than accepting a wider adapter that
        happens to work. `refresh` is the wider one, for callers that have a list.
        """
        self.refresh(positions)

    def invalidate(self, key: PositionKey) -> None:
        """Drop one entry, and make sure a load in flight for it does not put it straight back.

        For the cases the change stream cannot cover: the store was restored from a backup, the
        projection was rebuilt underneath the service, an operator repaired a row. Not part of the
        normal path — normal invalidation is `refresh`, which carries the new value.
        """
        # The generation is bumped whether or not there was an entry, because the entry may be
        # mid-load: the point of the bump is to tell that load its answer is no longer wanted.
        self._generation += 1
        if key in self._entries:
            del self._entries[key]
            self.invalidations += 1

    def clear(self) -> None:
        """Drop everything. Counted, so an unexplained hit-rate collapse has an explanation."""
        self._generation += 1
        self.invalidations += len(self._entries)
        self._entries.clear()

    # ---------------------------------------------------------------------------------
    # reporting
    # ---------------------------------------------------------------------------------

    @property
    def entries(self) -> int:
        """How many keys are cached, absences included."""
        return len(self._entries)

    @property
    def in_flight(self) -> int:
        """How many distinct keys are being loaded right now.

        Worth exposing rather than keeping private: this is the number that distinguishes "the store
        is slow" from "the cache is cold". A steady non-zero value with a low `coalesced` means
        requests are arriving spread out over a slow store; a spike with a high `coalesced` is a
        stampede being absorbed.
        """
        return len(self._in_flight)

    @property
    def hit_rate(self) -> float | None:
        """Hits over requests, or `None` when there have been no requests.

        `None` rather than `0.0`, for ADR-028's reason in miniature: a fresh or idle cache reporting
        a 0% hit rate is indistinguishable on a dashboard from a cache that is failing to hit, and
        the second is an incident. A null renders as a gap, which is what "no data" looks like.
        """
        requests = self.hits + self.negative_hits + self.misses
        if requests == 0:
            return None
        return (self.hits + self.negative_hits) / requests

    def as_dict(self) -> dict[str, Any]:
        """A JSON-shaped view for slice 5's `/cache`, with a stable set of keys.

        Every counter appears every time, including the ones sitting at zero. An absent key makes a
        dashboard panel vanish exactly when the thing it watches stops happening, which looks
        identical to the panel being broken — the same argument that pre-populates
        `ProjectionStats.skips` with every reason.
        """
        return {
            "entries": self.entries,
            "max_entries": self._max_entries,
            "in_flight": self.in_flight,
            "hits": self.hits,
            "negative_hits": self.negative_hits,
            "misses": self.misses,
            "hit_rate": self.hit_rate,
            "coalesced": self.coalesced,
            "loads": self.loads,
            "load_failures": self.load_failures,
            "loads_superseded": self.loads_superseded,
            "loads_discarded": self.loads_discarded,
            "refreshed": self.refreshed,
            "refreshes_dropped_cold": self.refreshes_dropped_cold,
            "refreshes_dropped_stale": self.refreshes_dropped_stale,
            "refreshes_held_for_load": self.refreshes_held_for_load,
            "invalidations": self.invalidations,
            "evictions": self.evictions,
        }

    def __repr__(self) -> str:
        """The four numbers worth seeing in an assertion failure, named."""
        rate = "-" if self.hit_rate is None else f"{self.hit_rate:.0%}"
        return (
            f"CachedPositions(entries={self.entries}/{self._max_entries}, "
            f"hit_rate={rate}, loads={self.loads}, in_flight={self.in_flight})"
        )


def _retrieve_exception(load: asyncio.Task[Position | None]) -> None:
    """Mark a failed load's exception as retrieved, so asyncio does not log it as unhandled.

    A module-level function rather than a lambda or a closure, because a done-callback that holds a
    reference to the cache would keep the cache alive for as long as the task, and because a named
    function is what shows up in a traceback if this ever raises.

    The `cancelled()` guard is required: calling `.exception()` on a cancelled task raises
    `CancelledError` rather than returning it, which inside a done-callback becomes an exception in
    the event loop's callback handler — a noisy way to fail while suppressing noise.
    """
    if not load.cancelled():
        load.exception()
