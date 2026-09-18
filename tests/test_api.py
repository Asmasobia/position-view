"""The HTTP surface: status codes, the wire shapes, read-your-writes, and Server-Sent Events.

THREE TEST STRATEGIES, AND WHY THIS FILE NEEDS ALL THREE
---------------------------------------------------------------------------------
1. **In-process, through the real ASGI app** (`httpx.ASGITransport`). This is the default and it
   covers every request/response route. It exercises FastAPI's routing, its query validation, the
   `HTTPException` handler and the response models — the actual stack — without a port, a process,
   or a millisecond of network. There is no mocking anywhere in this file.

2. **The SSE generator directly** (`position_stream(...)` as an async generator). Not a shortcut:
   see the next section. This is where frame ordering, the heartbeat, the overflow report and the
   subscription's cleanup are pinned, because the generator *is* where those decisions live.

3. **One real server on a real socket** (`uvicorn`), for the single property neither of the others
   can see: that the bytes are *flushed* as they are produced rather than at the end.

⚠️ WHY THE SSE ENDPOINT IS NOT TESTED THROUGH `ASGITransport`, WHICH IS THE OBVIOUS THING TO DO
   `httpx.ASGITransport` buffers. Its response stream is `ASGIResponseStream(body: list[bytes])`,
   built after the app has finished: it collects every `http.response.body` message, waits for the
   application to complete, and only then hands the joined bytes to `httpx`. That is invisible for
   an ordinary route and fatal for this one — an event stream never completes, so
   `client.stream("GET", "/stream/positions")` would not return a response object at all. The test
   would not fail; it would hang until the suite was killed, which is the single worst failure
   mode a test can have.

   So the split is not a preference. Frames are asserted against the generator, and the one thing
   the generator cannot prove — flushing — is asserted against a real server. ADR-056 records it.

WHY THE LIFESPAN IS DRIVEN EXPLICITLY, AND WHY THAT MATTERS MORE THAN IT LOOKS
---------------------------------------------------------------------------------
`ASGITransport` does not send `lifespan` events; it only ever sends `http` scopes. So without
`app.router.lifespan_context(app)`, `Service.start()` never runs and `Service.close()` never runs.
The failure would be quiet and it would be *in the tests' favour*: an unseeded gate reports
`reached == 0`, so a freshness test that expected a wait would get one, and a test asserting the
startup seed would pass on a service that had never started. Every test below goes through the
lifespan for that reason, and `test_startup_seeds_the_gate_from_the_store` fails if it does not.

WHY EVERY STREAM READ IS WRAPPED IN `asyncio.wait_for`
---------------------------------------------------------------------------------
The bugs available in this file are hangs, not wrong answers: a heartbeat that never fires, a
`drain` that loses its wakeup, a generator whose `finally` never runs. A bounded read turns each of
those into a failing test with a stack trace pointing at the line that waited. An unbounded one
turns them into a suite that stops producing output, and the difference is between a five-minute
fix and a CI job somebody eventually cancels.

The bounds are generous on purpose (`STREAM_TIMEOUT` is seconds, not milliseconds). They exist to
turn a hang into a failure, not to measure anything, so a slow machine must never trip them.

⚠️ ONE ASYNCIO DETAIL SHAPES THREE TESTS BELOW, SO IT IS WORTH STATING ONCE.
   `asyncio.wait_for(coro, timeout=0)` takes a documented fast path in 3.12: it wraps the coroutine
   in a task, finds it not done — a task that has not been scheduled has not run at all — cancels
   it, and raises `TimeoutError`. The coroutine's body never executes. So `heartbeat_seconds=0` is
   an exact, clock-free way to assert the timeout *branch*, and it can never deliver a batch. Where
   a test needs the drain genuinely started and then cancelled, it uses a small positive interval
   instead and says so.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from decimal import Decimal
from typing import Any, cast

import httpx
import pytest
import uvicorn
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from builders import COUNTERPARTY, CURRENCY, KEY, OCCURRED_AT, OMIT, OTHER_KEY, envelope_payload
from position_view.api import (
    DEFAULT_AWAIT_TIMEOUT_MS,
    MAX_AWAIT_TIMEOUT_MS,
    SSE_MEDIA_TYPE,
    CheckpointGate,
    PositionPayload,
    Service,
    TotalsPayload,
    create_app,
    parse_position_key,
    position_stream,
)
from position_view.api.payloads import problem
from position_view.api.stream import RETRY_MS, sse_comment, sse_frame, stream_headers
from position_view.model import (
    EVENT_SETTLED,
    InstructionSnapshot,
    Position,
    PositionKey,
    Totals,
)
from position_view.projection import (
    NO_CHECKPOINT,
    InMemoryPositionStore,
    PositionStore,
    StoreError,
    WriteBatch,
)
from position_view.push import Subscription

#: The host name in the URLs. Never resolved — `ASGITransport` calls the app directly — but it has
#: to be a valid absolute URL for `httpx` to build a request, and a name that could not exist is a
#: clearer signal than `localhost` that nothing here touches a network.
BASE_URL = "http://position-view.test"

#: The ceiling on any single read from a stream. See the module docstring: this exists to convert a
#: hang into a failure, so it is far larger than anything a correct implementation needs.
STREAM_TIMEOUT = 5.0

#: A heartbeat short enough to observe and long enough to be a real timer rather than the
#: `timeout=0` fast path. Used only where a test needs the drain genuinely started and cancelled.
SHORT_HEARTBEAT = 0.01

#: How long a real server is given to bind its socket, as a number of polls and an interval. Polled
#: rather than slept, so the usual case costs one poll instead of a fixed wait.
STARTUP_POLLS = 500
STARTUP_POLL_SECONDS = 0.01

#: The path the single-position route lives at for the builders' default key, written once so a
#: typo in a test is a typo in one place.
POSITION_PATH = f"/positions/{COUNTERPARTY}/{CURRENCY}"


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------
@asynccontextmanager
async def running(
    service: Service | None = None,
    *,
    raise_app_exceptions: bool = True,
) -> AsyncIterator[tuple[AsyncClient, Service]]:
    """An app with its lifespan run and a client wired to it, yielded with the service it wraps.

    An async context manager rather than a pytest fixture, for the reason `builders.py` gives about
    its own builders: an ordinary call is traceable. `async with running() as (http, service)` says
    on the test's own screen that there is a client *and* a service, and that both stop existing at
    the end of the block. A fixture would put the setup in another file and the teardown nowhere
    visible, and the teardown here is the part that matters — it is what asserts the lifespan's
    shutdown half runs at all.

    The service is yielded alongside the client because most assertions worth making are about both
    sides: the response body *and* what the cache counted, the status code *and* whether the gate
    still holds a waiter.
    """
    state = service if service is not None else Service()
    app = create_app(state)
    transport = ASGITransport(app=app, raise_app_exceptions=raise_app_exceptions)
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=transport, base_url=BASE_URL) as http:
            yield http, state


async def next_frame(lines: AsyncIterator[str]) -> list[str]:
    """The next complete SSE frame from a line iterator, as its non-empty lines.

    A frame ends at a blank line, so this reads until one arrives and discards the leading blank of
    an already-terminated previous frame. Returning the lines rather than a parsed object keeps the
    protocol visible in the assertions: a test that says `== ["retry: 2000"]` is checking the bytes
    on the wire, which is the contract, rather than checking this helper's idea of them.

    The bound is `STREAM_TIMEOUT` and it is deliberately not a parameter. Nothing here is measuring
    anything, so no test has a reason to want its own value, and one bound in one place is one thing
    to raise if a CI runner ever turns out to be slower than five seconds' worth of slow. Naming it
    `timeout` would also be the one shape `flake8-async` warns about (ASYNC109): a callee that owns
    a deadline does not compose with a caller that owns one.
    """
    collected: list[str] = []
    while True:
        line = await asyncio.wait_for(anext(lines), timeout=STREAM_TIMEOUT)
        if line:
            collected.append(line)
        elif collected:
            return collected


async def next_chunk(stream: AsyncIterator[bytes]) -> str:
    """The next thing the generator yields, decoded.

    Bounded by `STREAM_TIMEOUT`, and for the reason `next_frame` gives: the failure being hunted in
    this file is a hang rather than a wrong answer, and a bound turns one into the other.
    """
    return (await asyncio.wait_for(anext(stream), timeout=STREAM_TIMEOUT)).decode()


def frame_data(chunk: str) -> dict[str, Any]:
    """The JSON payload of a single-frame chunk, asserting on the way that there is one data line.

    The one-`data:`-line rule is a real invariant (`sse_frame` explains why a multi-line payload
    would need one prefix per line), so it is checked here rather than assumed — every test that
    reads a payload also checks the framing for free.
    """
    lines = [line for line in chunk.split("\n") if line]
    data_lines = [line for line in lines if line.startswith("data: ")]
    assert len(data_lines) == 1, f"expected exactly one data line, got {lines}"
    parsed = json.loads(data_lines[0].removeprefix("data: "))
    assert isinstance(parsed, dict)
    return parsed


async def yield_until(predicate: Callable[[], bool], *, description: str) -> None:
    """Hand control back to the event loop until `predicate()` holds, or fail with `description`.

    ⚠️ THIS IS NOT A SLEEP AND IT IS NOT A CLOCK. `asyncio.sleep(0)` yields to the loop exactly
       once without scheduling a timer, so this advances *other tasks* rather than time. That is
       what a test needs when it has to say "let the request get as far as waiting on the gate, then
       post the event": the condition is about progress, not about duration, and writing it as
       `sleep(0.05)` would be a guess that is simultaneously too slow on a fast machine and too fast
       on a loaded one.

    The iteration bound is what keeps a broken implementation from hanging the suite; it is not a
    timeout in any meaningful sense, since a correct one satisfies the predicate in a few turns.
    """
    for _ in range(1000):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError(f"never became true: {description}")


def error_detail(error: HTTPException) -> dict[str, Any]:
    """An `HTTPException`'s body as what it is: a JSON object, not the `str` starlette declares.

    Starlette annotates `HTTPException.detail` as `str`, because its own errors are strings. This
    API's errors never are — `problem()` puts the one error envelope there on purpose — so a test
    reading `detail["error"]` is indexing something mypy believes is a string, and under `--strict`
    that is an error rather than a warning.

    A `cast` and not a `# type: ignore`: the ignore would switch off *every* future check on that
    line, including a genuinely wrong key type, whereas the cast states the one fact that is known
    and leaves the indexing checked. It is also in one place, so if the envelope ever gets a real
    `TypedDict` there is one line to change.
    """
    return cast(dict[str, Any], error.detail)


def is_closed(subscription: Subscription) -> bool:
    """Whether a subscription has been closed, read through a call rather than as an attribute.

    ⚠️ THIS EXISTS FOR A TYPE-CHECKER REASON THAT LOOKS LIKE A BUG IN THE TEST AND IS NOT.
       mypy narrows attribute expressions, and it does not widen them again at the end of a `with`
       block — so `assert not subscription.is_closed` inside the block narrows `is_closed` to
       `False` for the rest of the function, and the `assert subscription.is_closed` *after* the
       block is then a comparison mypy believes can never hold. With `warn_unreachable` on, the
       following line is reported as dead code.

       The narrowing is not wrong in general; it is wrong here, because closing happens as a side
       effect of leaving the block. Reading the flag through a function call is the honest fix: a
       call result is not narrowable, so both assertions mean what they say.
    """
    return subscription.is_closed


class _UnreachableStore:
    """A `PositionStore` that answers the checkpoint and fails every read.

    It answers `checkpoint()` because otherwise the lifespan could not start, and a test about a
    failing *read* would fail during setup instead — the wrong failure, reported in the wrong place.
    Everything else raises `StoreError`, which is what a real store does when the database is gone.

    Written as a class conforming to the protocol rather than as a `unittest.mock` of one: mypy
    checks a class against `PositionStore` structurally, so a method renamed in the protocol breaks
    this file at type-check time. A mock would keep passing while testing a store shape that no
    longer exists.
    """

    def __init__(self, checkpoint: int = NO_CHECKPOINT) -> None:
        self._checkpoint = checkpoint
        self.reads_attempted = 0

    async def checkpoint(self) -> int:
        return self._checkpoint

    async def snapshot_for(self, instruction_id: uuid.UUID) -> InstructionSnapshot | None:
        raise self._failure()

    async def position_for(self, key: PositionKey) -> Position | None:
        raise self._failure()

    async def all_positions(self) -> tuple[Position, ...]:
        raise self._failure()

    async def snapshot_count(self) -> int:
        raise self._failure()

    async def commit(self, batch: WriteBatch) -> None:
        raise self._failure()

    def _failure(self) -> StoreError:
        self.reads_attempted += 1
        return StoreError("the store is unreachable")


class _ObservingStore:
    """An in-memory store that records, on every `checkpoint()` call, how many refreshes had run.

    This exists for one test: the claim that `Service.ingest` advances the freshness gate *after*
    the change listeners have run. That claim cannot be observed from outside — by the time `ingest`
    returns, both have happened — so the observation has to be taken from inside, and `checkpoint()`
    is the call `Service.ingest` makes immediately before advancing the gate.

    Delegation rather than inheritance, because `InMemoryPositionStore` is a concrete class and the
    thing being conformed to is the `PositionStore` protocol. Subclassing would inherit any method
    added to the store later without this file noticing; delegating means a new protocol method is a
    type error here.
    """

    def __init__(self) -> None:
        self._inner = InMemoryPositionStore()
        self._service: Service | None = None
        self.refreshes_seen: list[int] = []

    def watch(self, service: Service) -> None:
        """Point the store at the service whose cache it observes. Called after construction."""
        self._service = service

    async def checkpoint(self) -> int:
        if self._service is not None:
            self.refreshes_seen.append(self._service.cache.refreshed)
        return await self._inner.checkpoint()

    async def snapshot_for(self, instruction_id: uuid.UUID) -> InstructionSnapshot | None:
        return await self._inner.snapshot_for(instruction_id)

    async def position_for(self, key: PositionKey) -> Position | None:
        return await self._inner.position_for(key)

    async def all_positions(self) -> tuple[Position, ...]:
        return await self._inner.all_positions()

    async def snapshot_count(self) -> int:
        return await self._inner.snapshot_count()

    async def commit(self, batch: WriteBatch) -> None:
        await self._inner.commit(batch)


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
class TestReadingOnePosition:
    """`GET /positions/{counterparty}/{currency}` — the hot path, and the only cached route."""

    @pytest.mark.asyncio
    async def test_a_position_is_readable_after_the_event_that_created_it(self) -> None:
        """The end-to-end path, in one test: HTTP in, projection, store, cache, HTTP out.

        Every field is asserted rather than a subset, because this is the response a consumer builds
        against and a silently renamed key is a broken client. The values are the builder's
        defaults, which are chosen to be unremarkable *and* to have real cents in them.
        """
        async with running() as (http, _):
            assert (await http.post("/events", json=envelope_payload())).status_code == 200

            response = await http.get(POSITION_PATH)

            assert response.status_code == 200
            assert response.json() == {
                "counterparty": COUNTERPARTY,
                "currency": CURRENCY,
                "version": 1,
                "updated_at": OCCURRED_AT,
                "pending": {"amount": "1500.75", "count": 1},
                "settled": {"amount": "0.00", "count": 0},
                "failed": {"amount": "0.00", "count": 0},
                "reversed": {"amount": "0.00", "count": 0},
            }

    @pytest.mark.asyncio
    async def test_money_crosses_the_wire_as_a_string_and_never_as_a_number(self) -> None:
        """⚠️ THE ONE ASSERTION HERE ABOUT CORRECTNESS OF MONEY RATHER THAN ABOUT HTTP.

        Asserted against the raw response *text*, not against the parsed body, and that is the whole
        point. `response.json()` would give `"1500.75"` either way on this value — Python's parser
        happens to round-trip it — so a body serialised as a JSON number would pass a parsed
        assertion and fail in a browser, where `JSON.parse` produces a double. Checking the bytes is
        the only way to check the property.

        `1500.75` is exactly representable in binary floating point, so it is not itself the value
        that would break. That is precisely why the test looks at the encoding rather than at a
        value: the failure would appear on somebody else's number, months later.
        """
        async with running() as (http, _):
            await http.post("/events", json=envelope_payload())

            response = await http.get(POSITION_PATH)
            compact = response.text.replace(" ", "")

            assert '"amount":"1500.75"' in compact
            assert '"amount":1500.75' not in compact
            assert isinstance(response.json()["pending"]["amount"], str)

    @pytest.mark.asyncio
    async def test_an_unknown_position_is_a_404_in_the_one_error_envelope(self) -> None:
        """404 rather than a zeroed body, and the error shape every failure in this API uses.

        The envelope is asserted key by key because it is a contract: a client branching on
        `detail.error` must be able to rely on the slug being there and on it being stable, while
        `detail.message` is prose it should print and never parse.
        """
        async with running() as (http, _):
            response = await http.get(POSITION_PATH)

            assert response.status_code == 404
            detail = response.json()["detail"]
            assert detail["error"] == "no_position"
            assert COUNTERPARTY in detail["message"]

    @pytest.mark.asyncio
    async def test_the_read_is_served_through_the_cache(self) -> None:
        """The second read of the same key must not reach the store, and the counters say so.

        Asserted through `/cache` rather than by reaching into the object, so the test exercises the
        same numbers an operator would look at. A cache whose diagnostics disagreed with its
        behaviour would be worse than no cache at all.
        """
        async with running() as (http, _):
            await http.post("/events", json=envelope_payload())

            await http.get(POSITION_PATH)
            await http.get(POSITION_PATH)

            cache = (await http.get("/cache")).json()
            assert cache["misses"] == 1
            assert cache["hits"] == 1
            assert cache["hit_rate"] == 0.5

    @pytest.mark.asyncio
    async def test_the_currency_in_the_url_is_upper_cased_like_the_event_was(self) -> None:
        """`/positions/ACME-BANK/usd` finds the position stored under `USD`.

        The decoder upper-cases the currency on the way in, so a URL that did not would 404 on data
        that is present — and the client's mistake (a lower-case code) is one every HTTP client
        makes eventually.
        """
        async with running() as (http, _):
            await http.post("/events", json=envelope_payload())

            response = await http.get(f"/positions/{COUNTERPARTY}/usd")

            assert response.status_code == 200
            assert response.json()["currency"] == CURRENCY

    @pytest.mark.asyncio
    async def test_the_counterparty_in_the_url_is_not_upper_cased(self) -> None:
        """⚠️ THE ASYMMETRY IS THE POINT, AND IT IS THE EASIEST THING HERE TO "FIX" WRONGLY.

        The decoder strips the counterparty and does *not* change its case, so the api layer must
        not either. A helpful `.upper()` on both halves of the key would look tidier and would 404
        every position belonging to a counterparty stored as `AcmeBank` — presenting as "the data is
        missing" rather than as "the URL was transformed".

        Both directions are asserted: the exact case is found, and the upper-cased spelling is not.
        One without the other would pass on an implementation that upper-cased the *stored* value
        instead.
        """
        mixed = "AcmeBank"
        async with running() as (http, _):
            await http.post("/events", json=envelope_payload(counterparty=mixed))

            assert (await http.get(f"/positions/{mixed}/{CURRENCY}")).status_code == 200
            assert (await http.get(f"/positions/ACMEBANK/{CURRENCY}")).status_code == 404

    @pytest.mark.asyncio
    async def test_a_currency_that_is_not_three_letters_is_a_400_not_a_404(self) -> None:
        """A malformed key is the caller's error, and saying so is more useful than "not found".

        400 and 404 are both plausible and they mean different things to whoever is debugging:
        "your URL is wrong" sends them to their code, "no such position" sends them to the data.
        """
        async with running() as (http, _):
            response = await http.get(f"/positions/{COUNTERPARTY}/US")

            assert response.status_code == 400
            assert response.json()["detail"]["error"] == "malformed_currency"

    @pytest.mark.asyncio
    async def test_a_store_failure_is_not_reported_as_a_missing_position(self) -> None:
        """⚠️ A DATABASE OUTAGE MUST NOT LOOK LIKE "THIS COUNTERPARTY HAS NO EXPOSURE".

        The 404 path is one `except` away from swallowing an infrastructure failure, and the result
        would be the worst kind of wrong answer: a screen that confidently shows nothing outstanding
        while the store is unreachable. The read is allowed to fail with a 500 — a 500 is honest,
        and it is what alerting is for.

        `raise_app_exceptions=False` is what lets the test see the response the client would get
        rather than the exception the app raised, which is the whole reason `httpx` offers the flag.
        """
        store = _UnreachableStore()
        async with running(Service(store), raise_app_exceptions=False) as (http, _):
            response = await http.get(POSITION_PATH)

            assert response.status_code == 500
            assert store.reads_attempted == 1


class TestReadingACounterparty:
    """`GET /positions/{counterparty}` — the uncached list, and the deliberate absence of a 404."""

    @pytest.mark.asyncio
    async def test_every_currency_for_the_counterparty_is_listed(self) -> None:
        """Two events in two currencies produce two rows, wrapped in an object not an array."""
        async with running() as (http, _):
            await http.post("/events", json=envelope_payload())
            await http.post("/events", json=envelope_payload(event_id=2, currency="EUR"))

            body = (await http.get(f"/positions/{COUNTERPARTY}")).json()

            assert body["counterparty"] == COUNTERPARTY
            assert {row["currency"] for row in body["positions"]} == {"USD", "EUR"}

    @pytest.mark.asyncio
    async def test_another_counterpartys_positions_are_not_included(self) -> None:
        """The filter is the entire behaviour of this route, so it gets a test of its own.

        Without it, `test_every_currency_for_the_counterparty_is_listed` would still pass on an
        implementation that returned the whole book — the set comparison only looks at currencies.
        """
        async with running() as (http, _):
            await http.post("/events", json=envelope_payload())
            await http.post(
                "/events",
                json=envelope_payload(event_id=2, counterparty=OTHER_KEY.counterparty),
            )

            body = (await http.get(f"/positions/{COUNTERPARTY}")).json()

            assert [row["counterparty"] for row in body["positions"]] == [COUNTERPARTY]

    @pytest.mark.asyncio
    async def test_an_unknown_counterparty_is_an_empty_list_and_not_a_404(self) -> None:
        """A collection that exists and is empty is not a missing resource.

        A client rendering a table wants zero rows; the 404 belongs on the single-position route,
        where the caller named one specific thing.
        """
        async with running() as (http, _):
            response = await http.get("/positions/NOBODY")

            assert response.status_code == 200
            assert response.json() == {"counterparty": "NOBODY", "positions": []}

    @pytest.mark.asyncio
    async def test_the_list_route_does_not_touch_the_cache(self) -> None:
        """ADR-039, asserted rather than commented: the whole-book read stays off the cache.

        A cached `all_positions` is invalidated by a change to *any* position, so on a live stream
        it would be wrong more often than right. This test is what stops a later "optimisation" from
        adding it — the counters would move and this would fail.
        """
        async with running() as (http, _):
            await http.post("/events", json=envelope_payload())

            await http.get(f"/positions/{COUNTERPARTY}")

            cache = (await http.get("/cache")).json()
            assert (cache["hits"], cache["misses"], cache["entries"]) == (0, 0, 0)
            assert cache["hit_rate"] is None


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------
class TestReadYourWrites:
    """`?await_event_id=` — the parameter that makes an eventually-consistent read model usable."""

    @pytest.mark.asyncio
    async def test_a_checkpoint_already_reached_costs_nothing(self) -> None:
        """The common case: the client's own write is already visible, so nothing waits.

        `timeout_ms=0` is what makes this test prove something. With any positive timeout it would
        pass on an implementation that waited and then succeeded; with zero, a single suspension is
        a failure. And the counter confirms *why* it passed — the fast path, not luck.
        """
        async with running() as (http, service):
            posted = (await http.post("/events", json=envelope_payload())).json()

            response = await http.get(
                POSITION_PATH,
                params={"await_event_id": posted["checkpoint"], "timeout_ms": 0},
            )

            assert response.status_code == 200
            assert service.gate.as_dict()["waits_already_satisfied"] == 1

    @pytest.mark.asyncio
    async def test_a_checkpoint_that_never_arrives_is_a_503_with_retry_after(self) -> None:
        """⚠️ A STALE 200 IS NOT AN OPTION, WHICH IS WHY THIS ASSERTS A FAILURE STATUS.

        The tempting alternative — answer anyway, add a "may be behind" header — cannot be detected
        by a client that is rendering the body, and the person watching sees the number they just
        changed unchanged. 503 is retryable, loggable and alertable.

        `retry-after` is asserted because it is the difference between a client that backs off and a
        client that hammers, and because it lives in the *headers*: `problem()` keeps `headers`
        separate from the body fields precisely so a typo cannot bury it in the JSON.
        """
        async with running() as (http, service):
            response = await http.get(
                POSITION_PATH,
                params={"await_event_id": 99, "timeout_ms": 0},
            )

            assert response.status_code == 503
            assert response.headers["retry-after"] == "1"
            detail = response.json()["detail"]
            assert detail["error"] == "not_caught_up"
            assert detail["awaited_event_id"] == 99
            assert detail["checkpoint"] == NO_CHECKPOINT
            assert "retry-after" not in detail
            assert service.gate.as_dict()["waits_timed_out"] == 1

    @pytest.mark.asyncio
    async def test_a_timed_out_read_leaves_no_waiter_behind(self) -> None:
        """The leak that would grow fastest under exactly the condition that caused it.

        Every abandoned request that left its future in the gate's list would be one for an event id
        the stream never carried — so a client polling for a bad id would grow the list without
        bound. `wait_for`'s `finally` is what prevents it, and this is the assertion that keeps the
        `finally` there.
        """
        async with running() as (http, service):
            for event_id in (98, 99):
                await http.get(POSITION_PATH, params={"await_event_id": event_id, "timeout_ms": 0})

            assert service.gate.waiting == 0

    @pytest.mark.asyncio
    async def test_a_waiting_read_is_released_by_the_event_arriving(self) -> None:
        """The whole feature, concurrently: a read blocks, an event lands, and the read answers.

        The read is started as a task and the test waits until it is genuinely parked on the gate
        before posting — `yield_until` on `gate.waiting`, which is progress rather than time.
        Posting first would test the fast path again and prove nothing about the release.

        The 404 is the right expectation and it is not a weakness: the event posted is for a
        *different* counterparty, so what is being asserted is that the request came back at all
        rather than timing out. Using the same key would leave a passing test if the gate released
        early, because the position would be there either way.
        """
        async with running() as (http, service):
            reader = asyncio.create_task(
                http.get(
                    "/positions/NOBODY/USD",
                    params={"await_event_id": 1, "timeout_ms": MAX_AWAIT_TIMEOUT_MS},
                )
            )
            try:
                await yield_until(lambda: service.gate.waiting == 1, description="the read parked")

                await http.post("/events", json=envelope_payload())

                response = await asyncio.wait_for(reader, timeout=STREAM_TIMEOUT)
            finally:
                reader.cancel()

            assert response.status_code == 404
            assert service.gate.as_dict()["waits_timed_out"] == 0

    @pytest.mark.asyncio
    async def test_the_list_route_honours_freshness_too(self) -> None:
        """Both read routes take the parameter, because a client cannot know which it will need.

        A freshness guarantee available on one of two reads is a guarantee nobody can rely on.
        """
        async with running() as (http, _):
            response = await http.get(
                f"/positions/{COUNTERPARTY}",
                params={"await_event_id": 1, "timeout_ms": 0},
            )

            assert response.status_code == 503
            assert response.json()["detail"]["error"] == "not_caught_up"

    @pytest.mark.asyncio
    async def test_startup_seeds_the_gate_from_the_store(self) -> None:
        """⚠️ WITHOUT THIS, A RESTARTED PROCESS WOULD BLOCK ON DATA IT ALREADY HAD.

        A store holding a checkpoint of 1 and a gate that starts at 0 would treat
        `?await_event_id=1` as "not reached yet" and wait for the timeout — on a position sitting in
        the store. The lifespan's `start()` is what closes that gap.

        `timeout_ms=0` is what makes the test meaningful, and it is also the test that catches the
        harness bug the module docstring warns about: if the lifespan were not being run, `start()`
        would never fire and this would 503.
        """
        store = InMemoryPositionStore()
        seeded = Service(store)
        await seeded.start()
        await seeded.ingest(envelope_payload())

        restarted = Service(store)
        assert restarted.gate.reached == NO_CHECKPOINT

        async with running(restarted) as (http, _):
            response = await http.get(POSITION_PATH, params={"await_event_id": 1, "timeout_ms": 0})

            assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_a_timeout_above_the_ceiling_is_refused_by_the_framework(self) -> None:
        """The ceiling is a denial-of-service control, not a nicety.

        Without it, `?timeout_ms=600000` is a way for any caller to pin ten minutes of server
        resources per request from a URL that looks entirely innocent. 422 is FastAPI's own
        validation shape and that is correct here: a query parameter is HTTP's business, which is
        exactly the division of labour `POST /events` sits on the other side of.
        """
        async with running() as (http, _):
            response = await http.get(
                POSITION_PATH,
                params={"await_event_id": 1, "timeout_ms": MAX_AWAIT_TIMEOUT_MS + 1},
            )

            assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_event_id_zero_is_refused(self) -> None:
        """`NO_CHECKPOINT` is 0, so awaiting event 0 is meaningless or a bug in the caller.

        Refusing it turns "my client sent an uninitialised variable" into a 422 at the boundary
        rather than a read that appears to work and guarantees nothing.
        """
        async with running() as (http, _):
            response = await http.get(POSITION_PATH, params={"await_event_id": 0})

            assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_the_default_timeout_applies_when_none_is_given(self) -> None:
        """A caller that asks for freshness and not for a timeout gets the documented default.

        Asserted through the failure path with a deliberately unreachable event id: the request must
        come back on its own rather than hang, and the recorded wait must be the one that timed out.
        The default is a second, so this test costs a second — the honest price of asserting that a
        default exists at all.
        """
        async with running() as (http, service):
            response = await asyncio.wait_for(
                http.get(POSITION_PATH, params={"await_event_id": 99}),
                timeout=STREAM_TIMEOUT + DEFAULT_AWAIT_TIMEOUT_MS / 1000,
            )

            assert response.status_code == 503
            assert f"{DEFAULT_AWAIT_TIMEOUT_MS}ms" in response.json()["detail"]["message"]
            assert service.gate.as_dict()["waits_timed_out"] == 1


class TestCheckpointGate:
    """The gate on its own: no HTTP, no store, no clock. Every edge lives here."""

    @pytest.mark.asyncio
    async def test_advance_is_monotonic(self) -> None:
        """⚠️ A GATE THAT WENT BACKWARDS WOULD RE-BLOCK A READER IT HAD ALREADY RELEASED.

        `advance` is called with whatever the store reports after an event, and two concurrent
        ingests can produce those reports out of order. `max` is what makes the out-of-order call
        harmless; a plain assignment would produce a request that hangs for a reason no log line
        could explain.
        """
        gate = CheckpointGate()

        gate.advance(5)
        gate.advance(3)

        assert gate.reached == 5

    @pytest.mark.asyncio
    async def test_a_waiter_is_released_when_its_event_is_reached(self) -> None:
        """The mechanism, with the waiter and the advance in one test and no polling anywhere."""
        gate = CheckpointGate()
        waiter = asyncio.create_task(gate.wait_for(7, timeout_ms=MAX_AWAIT_TIMEOUT_MS))
        await yield_until(lambda: gate.waiting == 1, description="the waiter registered")

        gate.advance(7)

        assert await asyncio.wait_for(waiter, timeout=STREAM_TIMEOUT) is True
        assert gate.waiting == 0

    @pytest.mark.asyncio
    async def test_a_waiter_is_released_by_a_checkpoint_past_its_own(self) -> None:
        """`<=`, not `==`: a batch jumping the checkpoint 3 to 9 must release the waiter for 7.

        An equality test would be an easy thing to write and would deadlock on any store that
        commits more than one event per checkpoint advance — which is exactly what a batched or
        replaying consumer does.
        """
        gate = CheckpointGate()
        waiter = asyncio.create_task(gate.wait_for(7, timeout_ms=MAX_AWAIT_TIMEOUT_MS))
        await yield_until(lambda: gate.waiting == 1, description="the waiter registered")

        gate.advance(9)

        assert await asyncio.wait_for(waiter, timeout=STREAM_TIMEOUT) is True

    @pytest.mark.asyncio
    async def test_an_unsatisfied_waiter_stays_registered(self) -> None:
        """An advance that falls short must leave the waiter in the list rather than dropping it.

        The rebuild in `advance` is where a waiter could silently be lost, and a lost waiter is a
        request that waits out its whole timeout on data that has since arrived.
        """
        gate = CheckpointGate()
        waiter = asyncio.create_task(gate.wait_for(7, timeout_ms=MAX_AWAIT_TIMEOUT_MS))
        await yield_until(lambda: gate.waiting == 1, description="the waiter registered")

        gate.advance(6)
        assert gate.waiting == 1

        gate.advance(7)
        assert await asyncio.wait_for(waiter, timeout=STREAM_TIMEOUT) is True

    @pytest.mark.asyncio
    async def test_a_timeout_returns_false_rather_than_raising(self) -> None:
        """A "not yet" is an answer, not an exception: the status code is the route's decision.

        A gate that raised would have to raise something HTTP-shaped to be useful, and then the
        freshness mechanism could not be used by anything that is not a request.
        """
        gate = CheckpointGate()

        assert await gate.wait_for(7, timeout_ms=0) is False
        assert gate.as_dict()["waits_timed_out"] == 1
        assert gate.waiting == 0

    @pytest.mark.asyncio
    async def test_advance_tolerates_a_cancelled_waiter(self) -> None:
        """A client that disconnects mid-wait leaves a cancelled future, and `advance` must survive.

        The removal in `wait_for`'s `finally` and the iteration in `advance` are not ordered with
        respect to each other, so `advance` has to expect to find a done future. Without the
        `waiter.done()` check this raises `InvalidStateError` — inside the ingest path, which would
        turn one disconnected client into a failed event.
        """
        gate = CheckpointGate()
        waiter = asyncio.create_task(gate.wait_for(7, timeout_ms=MAX_AWAIT_TIMEOUT_MS))
        await yield_until(lambda: gate.waiting == 1, description="the waiter registered")

        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter

        gate.advance(7)

        assert gate.waiting == 0

    @pytest.mark.asyncio
    async def test_many_waiters_are_released_in_one_advance(self) -> None:
        """One advance, four waiters, one pass over the list — and the short one stays put."""
        gate = CheckpointGate()
        released = [
            asyncio.create_task(gate.wait_for(event_id, timeout_ms=MAX_AWAIT_TIMEOUT_MS))
            for event_id in (1, 2, 3)
        ]
        pending = asyncio.create_task(gate.wait_for(9, timeout_ms=MAX_AWAIT_TIMEOUT_MS))
        await yield_until(lambda: gate.waiting == 4, description="four waiters registered")

        gate.advance(3)

        results = await asyncio.wait_for(asyncio.gather(*released), timeout=STREAM_TIMEOUT)
        assert results == [True, True, True]
        assert gate.waiting == 1
        pending.cancel()

    @pytest.mark.asyncio
    async def test_the_fast_path_does_not_suspend(self) -> None:
        """An already-satisfied wait must not even reach the event loop.

        Proven by driving the coroutine by hand: `send(None)` on a coroutine that returns without
        suspending raises `StopIteration` carrying the result, whereas one that awaits anything
        yields instead. That is a stronger statement than "it was fast", and it is the property the
        docstring claims — a caller that does not need to wait pays nothing, not even a scheduling
        turn, so the parameter cannot reorder anything.
        """
        gate = CheckpointGate()
        gate.advance(7)

        coroutine = gate.wait_for(7, timeout_ms=MAX_AWAIT_TIMEOUT_MS)
        with pytest.raises(StopIteration) as raised:
            coroutine.send(None)

        assert raised.value.value is True
        assert gate.as_dict()["waits"] == 1

    def test_the_diagnostics_name_every_counter(self) -> None:
        """`as_dict` is a dashboard's schema, so its keys are pinned rather than left to drift."""
        assert set(CheckpointGate().as_dict()) == {
            "reached",
            "waiting",
            "waits",
            "waits_already_satisfied",
            "waits_timed_out",
        }

    def test_the_repr_is_useful_in_a_failure_message(self) -> None:
        """A bare `<CheckpointGate object at 0x...>` in an assertion failure explains nothing."""
        assert repr(CheckpointGate()) == "CheckpointGate(reached=0, waiting=0)"


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------
class TestIngest:
    """`POST /events` — the consumer seam, and the one route pydantic does not validate."""

    @pytest.mark.asyncio
    async def test_an_applied_event_reports_the_checkpoint(self) -> None:
        """The response is the input to the next read's `?await_event_id=`, and nothing more."""
        async with running() as (http, _):
            response = await http.post("/events", json=envelope_payload())

            assert response.status_code == 200
            assert response.json() == {"outcome": "applied", "reason": None, "checkpoint": 1}

    @pytest.mark.asyncio
    async def test_the_event_id_is_not_echoed(self) -> None:
        """Deliberately absent: the checkpoint is stronger, so echoing both invites the weaker.

        The store clamps the checkpoint to a high-water mark, so it is never lower than the event
        just applied — waiting for it therefore guarantees this write is visible. A `Skipped`
        outcome does not carry an event id at all, so the field could not be filled honestly in
        every case.
        """
        async with running() as (http, _):
            body = (await http.post("/events", json=envelope_payload())).json()

            assert "event_id" not in body

    @pytest.mark.asyncio
    async def test_a_duplicate_is_a_200_with_a_reason(self) -> None:
        """⚠️ AT-LEAST-ONCE DELIVERY WORKING CORRECTLY IS NOT AN ERROR.

        A 4xx here would make every redelivery look like a client bug, and a consumer that retried
        on 4xx would loop forever. The reason field is what lets an operator tell "the broker sent
        it twice" from "nothing happened".
        """
        async with running() as (http, _):
            await http.post("/events", json=envelope_payload())

            response = await http.post("/events", json=envelope_payload())

            assert response.status_code == 200
            assert response.json() == {
                "outcome": "skipped",
                "reason": "duplicate",
                "checkpoint": 1,
            }

    @pytest.mark.asyncio
    async def test_an_event_about_another_aggregate_is_skipped_not_refused(self) -> None:
        """A shared topic carries other people's events: not malformed, just not ours.

        Refusing them with a 400 would make a perfectly healthy multi-tenant topic look like a
        stream of client errors.
        """
        async with running() as (http, _):
            response = await http.post("/events", json=envelope_payload(aggregate_type="trade"))

            assert response.status_code == 200
            assert response.json()["reason"] == "not_our_aggregate"

    @pytest.mark.asyncio
    async def test_a_refused_event_is_a_400_carrying_the_decoders_own_message(self) -> None:
        """⚠️ THE DECODER'S WORDS, NOT THE FRAMEWORK'S — WHICH IS WHY PYDANTIC IS NOT IN FRONT.

        A pydantic model here would reject this payload first, in pydantic's vocabulary — so the
        reason a real event was refused would be reported in words that do not appear anywhere in
        the projection's logs. The message asserted below is the one `decode_envelope` produces,
        which is the one an operator will search for at 3am.
        """
        async with running() as (http, _):
            response = await http.post("/events", json=envelope_payload(event_id=OMIT))

            assert response.status_code == 400
            detail = response.json()["detail"]
            assert detail["error"] == "malformed_event"
            assert detail["message"] == "missing required field 'event_id'"

    @pytest.mark.asyncio
    async def test_money_the_service_cannot_store_exactly_is_refused_not_rounded(self) -> None:
        """The most important refusal in the service, reachable over HTTP and reported honestly.

        Three decimal places on a currency this service stores to two is not a rounding decision the
        api layer gets to make quietly — it is a disagreement with the producer, and the only honest
        answer is to refuse and say so.
        """
        async with running() as (http, _):
            response = await http.post("/events", json=envelope_payload(amount="1500.755"))

            assert response.status_code == 400
            message = response.json()["detail"]["message"]
            assert "more precision than this service stores" in message

    @pytest.mark.asyncio
    async def test_a_body_that_is_not_a_json_object_is_refused_by_the_framework(self) -> None:
        """The division of labour as a test: HTTP shape is FastAPI's, semantics are the model's.

        `dict[str, Any]` is not "no validation" — a JSON array still gets a 422, because "the body
        is an object" is a fact about the transport. Everything *inside* the object belongs to
        `decode_envelope`.
        """
        async with running() as (http, _):
            response = await http.post("/events", json=[1, 2, 3])

            assert response.status_code == 422

    @pytest.mark.asyncio
    async def test_the_checkpoint_from_a_post_is_usable_as_a_freshness_token(self) -> None:
        """The round trip the two features exist to make possible, asserted as one flow.

        Post, take the checkpoint from the response, read with it, get the write back — with
        `timeout_ms=0`, so the read cannot have succeeded by waiting.
        """
        async with running() as (http, _):
            checkpoint = (await http.post("/events", json=envelope_payload())).json()["checkpoint"]

            response = await http.get(
                POSITION_PATH, params={"await_event_id": checkpoint, "timeout_ms": 0}
            )

            assert response.status_code == 200
            assert response.json()["version"] == 1

    @pytest.mark.asyncio
    async def test_ingest_advances_the_gate_after_the_listeners_have_run(self) -> None:
        """⚠️ THE ORDERING THAT MAKES READ-YOUR-WRITES TRUE RATHER THAN NEARLY TRUE.

        If the gate advanced before the change listeners ran, a released reader could be told the
        projection had passed event 1 and then be handed the *old* cached version — the exact lie
        the parameter exists to prevent, and intermittent enough to be blamed on the client.

        Observed from inside the store, because from outside both have already happened by the time
        `ingest` returns. `Service.ingest` calls `store.checkpoint()` immediately before advancing
        the gate, so what the cache had refreshed by that moment is the ordering.

        The position is read first so the cache holds an entry for the key: the cache refreshes in
        place and never inserts, so a cold key would show no refresh and the test would pass for the
        wrong reason.
        """
        store = _ObservingStore()
        service = Service(store)
        store.watch(service)
        await service.start()
        assert await service.cache.position_for(KEY) is None
        store.refreshes_seen.clear()

        await service.ingest(envelope_payload())

        assert store.refreshes_seen, "Service.ingest must read the checkpoint after ingesting"
        assert store.refreshes_seen[-1] == 1
        assert service.gate.reached == 1


# ---------------------------------------------------------------------------
# SSE framing
# ---------------------------------------------------------------------------
class TestFraming:
    """The protocol itself: four field names, a blank line, and two rules that are easy to break."""

    def test_a_frame_is_an_event_line_a_data_line_and_a_blank_line(self) -> None:
        assert sse_frame("positions", {"a": 1}) == b'event: positions\ndata: {"a":1}\n\n'

    def test_a_payload_containing_a_newline_still_produces_one_data_line(self) -> None:
        """⚠️ THE INVARIANT THAT MAKES A SINGLE `data:` PREFIX SAFE RATHER THAN LUCKY.

        SSE requires one `data:` prefix per line of payload. A literal newline inside the JSON would
        therefore truncate the document for every client on the other end — and the symptom is a
        parse error nobody can reproduce, because it depends on the *contents* of a counterparty
        name. `json.dumps` escapes newlines inside strings, so its output cannot contain one, and
        this test is what pins that reasoning to a behaviour.
        """
        frame = sse_frame("positions", {"note": "two\nlines"}).decode()

        assert frame.count("data: ") == 1
        assert "\\n" in frame
        assert frame.endswith("\n\n")

    def test_a_comment_frame_carries_no_event_name(self) -> None:
        """The heartbeat is a comment, so no client needs to have heard of it.

        A named `ping` event would make every client with an `addEventListener` map log an unknown
        event every fifteen seconds, forever, in production.
        """
        assert sse_comment("keep-alive") == b": keep-alive\n\n"

    def test_the_headers_defeat_the_three_things_that_break_a_stream_in_the_middle(self) -> None:
        """Pinned as values, because each one is a production failure somebody has already had.

        `no-store` stops a shared cache serving one client's positions to another;
        `x-accel-buffering` stops nginx holding the whole stream until its buffer fills, which is
        the "works locally, silent in staging" bug; `keep-alive` is belt and braces for HTTP/1.1.
        """
        assert stream_headers() == {
            "cache-control": "no-store",
            "x-accel-buffering": "no",
            "connection": "keep-alive",
        }


class TestStreamGenerator:
    """`position_stream` driven as an async generator, where every frame decision lives."""

    @pytest.mark.asyncio
    async def test_the_first_frame_is_the_retry_hint_and_the_second_is_the_snapshot(self) -> None:
        """⚠️ `retry` GOES FIRST, BEFORE THE SNAPSHOT IS READ, AND THE ORDER IS THE POINT.

        The client most likely to disconnect is the one still waiting for a large snapshot — so it
        is the client that most needs to have been told how long to wait before reconnecting.
        Sending the hint after the snapshot would mean the clients that never receive it are exactly
        the ones it was for.
        """
        service = Service()
        await service.ingest(envelope_payload())
        stream = position_stream(service, None)
        try:
            assert await next_chunk(stream) == f"retry: {RETRY_MS}\n\n"

            snapshot = await next_chunk(stream)

            assert snapshot.startswith("event: snapshot\n")
            assert frame_data(snapshot)["positions"][0]["counterparty"] == COUNTERPARTY
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_change_after_the_snapshot_arrives_as_a_positions_frame(self) -> None:
        """The live half: the same shape as the snapshot, under a different event name.

        One shape so a client can hand both to the same render function; a different name because
        the two mean different things to a client keeping state — replace versus merge.
        """
        service = Service()
        stream = position_stream(service, None)
        try:
            await next_chunk(stream)
            assert frame_data(await next_chunk(stream)) == {"positions": []}

            await service.ingest(envelope_payload())

            update = await next_chunk(stream)
            assert update.startswith("event: positions\n")
            assert frame_data(update)["positions"][0]["version"] == 1
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_the_snapshot_is_not_re_sent_as_an_update(self) -> None:
        """⚠️ `mark_delivered` IS WHAT KEEPS SUBSCRIBE-FIRST FROM DUPLICATING EVERYTHING.

        Subscribing before reading the snapshot is what stops a change slipping through the gap —
        but it also means the snapshot's own versions are sitting in the buffer. Without
        `mark_delivered`, the first update frame would re-send the entire snapshot as news, and a
        client counting changes would double every position on connect.

        Asserted by ingesting a *second* event and reading exactly one frame: if the snapshot had
        been re-sent it would arrive first, and the version would be 1 instead of 2.

        ⚠️ THE SECOND EVENT IS A SETTLEMENT, AND IT HAS TO BE SOMETHING THAT MOVES A BUCKET. A
           `matched` event after a `received` one produces two deltas that cancel inside the same
           bucket, so the store commits no change, no listener fires, and this test would wait for a
           frame that is never coming. `settled` moves money from pending to settled, so the
           position genuinely reaches version 2.
        """
        service = Service()
        await service.ingest(envelope_payload())
        stream = position_stream(service, None)
        try:
            await next_chunk(stream)
            await next_chunk(stream)

            await service.ingest(envelope_payload(event_id=2, event_type=EVENT_SETTLED))

            positions = frame_data(await next_chunk(stream))["positions"]
            assert len(positions) == 1
            assert positions[0]["version"] == 2
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_change_committed_during_the_snapshot_is_not_re_sent(self) -> None:
        """⚠️ THE ADR-033 RACE ITSELF, WHICH THE TEST ABOVE DOES NOT ACTUALLY REACH.

        `test_the_snapshot_is_not_re_sent_as_an_update` ingests its first event *before* the stream
        starts, so by the time the subscription exists there is nothing buffered and
        `mark_delivered` has nothing to suppress — deleting the call leaves that test green. The
        mutation matrix found exactly that, and this is the test that closes it.

        The window is reached deterministically by using the frame order: `retry` is yielded after
        the subscription is created and before the snapshot is read, so a generator suspended on
        that first yield is sitting precisely inside the gap ADR-033 is about. An event ingested
        here is buffered by the subscription *and* included in the snapshot that follows it.

        What must happen next is silence. The buffered version is one the client has just been
        given, so `drain_nowait` suppresses it, `drain` loops rather than returning an empty batch,
        and the next thing on the wire is a keep-alive. Without `mark_delivered` it would be a
        `positions` frame repeating the snapshot, and a client counting changes would double every
        position on connect.
        """
        service = Service()
        await service.ingest(envelope_payload())
        stream = position_stream(service, None, heartbeat_seconds=SHORT_HEARTBEAT)
        try:
            assert await next_chunk(stream) == f"retry: {RETRY_MS}\n\n"

            # Suspended on the `retry` yield: subscribed, not yet snapshotted.
            await service.ingest(envelope_payload(event_id=2, event_type=EVENT_SETTLED))

            snapshot = frame_data(await next_chunk(stream))
            assert snapshot["positions"][0]["version"] == 2

            assert await next_chunk(stream) == ": keep-alive\n\n"
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_keyed_subscriber_is_not_told_about_other_positions(self) -> None:
        """The filter, which is the difference between a dashboard and a firehose."""
        service = Service()
        stream = position_stream(service, [KEY])
        try:
            await next_chunk(stream)
            await next_chunk(stream)

            await service.ingest(envelope_payload(counterparty=OTHER_KEY.counterparty))
            await service.ingest(envelope_payload(event_id=2))

            positions = frame_data(await next_chunk(stream))["positions"]
            assert [row["counterparty"] for row in positions] == [COUNTERPARTY]
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_keyed_snapshot_is_read_through_the_cache(self) -> None:
        """ADR-039's other half: a reconnecting client asking for keys must not reach the store.

        Reconnection is the moment a snapshot is most likely to be needed and most likely to be
        needed *again* — a flapping proxy turns a hundred clients into a hundred snapshots — so the
        keyed path is exactly where the cache earns its place.
        """
        service = Service()
        await service.ingest(envelope_payload())
        stream = position_stream(service, [KEY])
        try:
            await next_chunk(stream)
            await next_chunk(stream)

            assert service.cache.as_dict()["misses"] == 1
            assert service.cache.entries == 1
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_wildcard_snapshot_does_not_touch_the_cache(self) -> None:
        """And the wildcard goes to the store, because `all_positions` is deliberately uncached."""
        service = Service()
        await service.ingest(envelope_payload())
        stream = position_stream(service, None)
        try:
            await next_chunk(stream)
            await next_chunk(stream)

            assert service.cache.as_dict()["hit_rate"] is None
            assert service.cache.entries == 0
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_watched_position_that_does_not_exist_is_omitted(self) -> None:
        """Two different facts: "no exposure" and "exposure of zero" — and only one is known.

        A row of zeros for a counterparty nothing has happened to would be a claim this service
        cannot support, and on a settlement screen it is the more dangerous of the two mistakes.
        """
        service = Service()
        stream = position_stream(service, [KEY, OTHER_KEY])
        try:
            await next_chunk(stream)

            assert frame_data(await next_chunk(stream)) == {"positions": []}
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_an_idle_stream_sends_a_keep_alive_comment(self) -> None:
        """⚠️ THE FAILURE THIS PREVENTS IS INVISIBLE TO BOTH ENDS OF THE CONNECTION.

        Proxies and NAT tables reap connections that have carried no bytes for 30 to 60 seconds, and
        neither side is told. A quiet stream is the *normal* state of a settlement view outside
        market hours, so the bug presents as "the dashboard was stale overnight" and cannot be
        reproduced at 11am.

        `heartbeat_seconds=0` takes `wait_for`'s zero-timeout fast path, which never runs the drain
        at all — so this is an exact, clock-free assertion about the timeout *branch*, and two
        consecutive comments prove the loop continues rather than falling out of it.
        """
        service = Service()
        stream = position_stream(service, None, heartbeat_seconds=0)
        try:
            await next_chunk(stream)
            await next_chunk(stream)

            assert await next_chunk(stream) == ": keep-alive\n\n"
            assert await next_chunk(stream) == ": keep-alive\n\n"
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_the_heartbeat_is_a_real_interval_and_not_a_zero_special_case(self) -> None:
        """The same assertion with a genuine timer, because `timeout=0` takes a different branch.

        `asyncio.wait_for(..., 0)` never starts the wrapped coroutine; a positive timeout does, then
        cancels it. Only this version proves the drain is actually attempted and abandoned, which is
        the case the "cancelling a drain cannot lose a batch" argument is about. Ten milliseconds is
        a real interval, and it is bounded by `next_chunk` like every other read here.
        """
        service = Service()
        stream = position_stream(service, None, heartbeat_seconds=SHORT_HEARTBEAT)
        try:
            await next_chunk(stream)
            await next_chunk(stream)

            assert await next_chunk(stream) == ": keep-alive\n\n"
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_heartbeat_does_not_lose_a_batch_that_arrived_after_it(self) -> None:
        """⚠️ THE STRUCTURAL CLAIM, TURNED INTO A TEST: CANCELLING `drain` CANNOT DROP DATA.

        `drain`'s only suspension point is waiting on its wakeup event, and the take-and-clear that
        follows is synchronous — so a `CancelledError` from the heartbeat timeout can only arrive
        *before* the buffer has been taken. The wakeup event stays set, so the next `drain` picks up
        exactly what the cancelled one abandoned.

        ⚠️ A REAL INTERVAL RATHER THAN `heartbeat_seconds=0`, AND THE DIFFERENCE IS THE WHOLE TEST.
           The zero-timeout path never starts the drain, so it could not possibly cancel one — and
           worse, a stream on that path can never deliver a batch at all, so this test written with
           a zero heartbeat would read keep-alives until it hit its bound. Ten milliseconds means
           the drain genuinely runs, genuinely parks on the event, and is genuinely cancelled; the
           frame after that must still carry the change.
        """
        service = Service()
        stream = position_stream(service, None, heartbeat_seconds=SHORT_HEARTBEAT)
        try:
            await next_chunk(stream)
            await next_chunk(stream)
            assert await next_chunk(stream) == ": keep-alive\n\n"

            await service.ingest(envelope_payload())

            chunk = await next_chunk(stream)
            assert chunk.startswith("event: positions\n")
            assert frame_data(chunk)["positions"][0]["counterparty"] == COUNTERPARTY
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_subscriber_that_falls_behind_is_told_before_the_stream_ends(self) -> None:
        """⚠️ THE ONE CASE WHERE ENDING QUIETLY WOULD BE A LIE.

        An overflowed buffer means updates are simply gone. A silent close is indistinguishable from
        a normal one, so a client that reconnects would recover by accident and a client that does
        not would render a view frozen at an arbitrary moment with no indication at all. Naming it
        lets the client decide.

        `max_pending=1` and two distinct positions is the smallest way to produce the condition; the
        real threshold is 256 and the mechanism is identical.
        """
        service = Service(max_pending=1)
        stream = position_stream(service, None)
        try:
            await next_chunk(stream)
            await next_chunk(stream)

            await service.ingest(envelope_payload())
            await service.ingest(envelope_payload(event_id=2, counterparty=OTHER_KEY.counterparty))

            chunk = await next_chunk(stream)
            assert chunk.startswith("event: overflow\n")
            assert frame_data(chunk)["error"] == "overflow"

            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(stream), timeout=STREAM_TIMEOUT)
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_closing_the_hub_ends_the_stream_cleanly(self) -> None:
        """Shutdown from the stream's point of view: the iterator ends, so the response completes.

        A completed response is a client that reconnects on its own schedule. A dropped connection
        is a client that reconnects immediately, all of them at once, at the moment a new process is
        starting up — which is the worst possible time.
        """
        service = Service()
        stream = position_stream(service, None)
        try:
            await next_chunk(stream)
            await next_chunk(stream)

            service.close()

            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(stream), timeout=STREAM_TIMEOUT)
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_failed_snapshot_is_reported_as_an_error_frame(self) -> None:
        """The trade the handover-inside-the-generator decision makes, asserted as a behaviour.

        The response has already started by the time the snapshot is read, so there is no status
        code left to set. The alternative to an `error` frame is closing the connection and letting
        the client guess — and a guessing client renders an empty book, which looks like good news.
        """
        service = Service(_UnreachableStore())
        stream = position_stream(service, None)
        try:
            await next_chunk(stream)

            chunk = await next_chunk(stream)

            assert chunk.startswith("event: error\n")
            assert frame_data(chunk)["error"] == "snapshot_failed"
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(anext(stream), timeout=STREAM_TIMEOUT)
        finally:
            await stream.aclose()

    @pytest.mark.asyncio
    async def test_a_failed_snapshot_still_releases_the_subscription(self) -> None:
        """⚠️ THE LEAK THE HANDOVER WAS MOVED INSIDE THE GENERATOR TO PREVENT.

        Every path out of the generator — a clean end, a client disconnect, a failed snapshot — runs
        the `with` block's `finally`. A subscription left registered is one that is offered every
        change for the life of the process, and a leak that grows with client churn is much worse
        than a degraded error path.
        """
        service = Service(_UnreachableStore())
        stream = position_stream(service, None)
        await next_chunk(stream)
        await next_chunk(stream)

        await stream.aclose()

        assert service.hub.subscribers == 0

    @pytest.mark.asyncio
    async def test_abandoning_the_stream_releases_the_subscription(self) -> None:
        """The ordinary path, same guarantee: closing the iterator unregisters the subscriber."""
        service = Service()
        stream = position_stream(service, None)
        await next_chunk(stream)
        await next_chunk(stream)
        assert service.hub.subscribers == 1

        await stream.aclose()

        assert service.hub.subscribers == 0


# ---------------------------------------------------------------------------
# The stream over HTTP
# ---------------------------------------------------------------------------
class TestStreamRoute:
    """What can be asserted about `/stream/positions` without a socket: the pre-stream failures."""

    @pytest.mark.asyncio
    async def test_a_malformed_key_is_a_400_before_the_stream_starts(self) -> None:
        """The one class of error this endpoint can still report properly.

        Everything after the first byte can only be an `error` frame on a 200, so parsing the keys
        *before* returning the response is what keeps a client's typo a 400. This request
        terminates, which is also why it is safe to make through the buffering in-process transport.
        """
        async with running() as (http, _):
            response = await http.get("/stream/positions", params={"keys": "nonsense"})

            assert response.status_code == 400
            assert response.json()["detail"]["error"] == "malformed_key"

    @pytest.mark.asyncio
    async def test_a_rejected_key_leaves_no_subscription_behind(self) -> None:
        """The 400 happens before `position_stream` is ever iterated, so nothing was registered.

        Worth asserting rather than reasoning about: the route builds the key list and the
        `StreamingResponse` in the same function, and a future refactor that subscribed first would
        leak one subscriber per bad URL — a request an attacker can repeat cheaply.
        """
        async with running() as (http, service):
            response = await http.get("/stream/positions", params={"keys": f"{COUNTERPARTY}/US"})

            assert response.status_code == 400
            assert service.hub.subscribers == 0
            assert service.hub.as_dict()["published"] == 0

    def test_the_stream_path_is_not_shadowed_by_a_counterparty(self) -> None:
        """`/stream/positions`, not `/positions/stream` — one counterparty from unreachable.

        FastAPI matches routes in registration order, so `/positions/{counterparty}` would swallow
        `/positions/stream` if it were registered first, and a counterparty legitimately named
        "stream" would swallow it either way. The failure would present as a 404 for a position that
        exists, which is a long afternoon.

        Asserted against the app's own route table rather than by making a request, because the
        point is the absence of a path.
        """
        paths = {getattr(route, "path", None) for route in create_app().routes}

        assert "/stream/positions" in paths
        assert "/positions/stream" not in paths


class TestStreamOverARealSocket:
    """The one property no in-process test can see: bytes leave the server as they are produced."""

    @pytest.mark.asyncio
    async def test_frames_are_flushed_while_the_response_is_still_open(self) -> None:
        """⚠️ AN SSE ENDPOINT THAT BUFFERS PASSES EVERY OTHER TEST HERE AND IS STILL USELESS.

        A response whose body only arrives when the generator finishes is not a stream: the frames
        are correct, the ordering is correct, and the client sees nothing until the connection ends
        — which for this endpoint is never. Only a real server on a real socket can tell the
        difference, because the buffering would happen in the transport, below everything the
        generator can observe.

        The test reads two frames, posts an event over a *second* connection while the first is
        still open, and reads the frame that results. Every one of those steps is impossible against
        a buffering transport, which is precisely why they are the assertions.

        This is also the only test that exercises the real lifespan, the real `StreamingResponse`,
        the real headers and a real client disconnect at the end of the `with` block. It costs
        roughly a tenth of a second and it is the only end-to-end proof in the file.
        """
        service = Service()
        app = create_app(service)
        config = uvicorn.Config(
            app,
            host="127.0.0.1",
            # Port 0 asks the OS for any free port. A hardcoded port is the standard way for this
            # kind of test to fail on a developer's machine for reasons that have nothing to do with
            # the code, and to fail again on CI when two jobs share a runner.
            port=0,
            log_level="warning",
            access_log=False,
            lifespan="on",
        )
        server = uvicorn.Server(config)
        serving = asyncio.create_task(server.serve())
        try:
            port = await _bound_port(server)
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as http:
                async with http.stream(
                    "GET", "/stream/positions", timeout=STREAM_TIMEOUT
                ) as response:
                    assert response.status_code == 200
                    assert response.headers["content-type"].startswith(SSE_MEDIA_TYPE)
                    assert response.headers["cache-control"] == "no-store"
                    assert response.headers["x-accel-buffering"] == "no"

                    lines = response.aiter_lines()
                    assert await next_frame(lines) == [f"retry: {RETRY_MS}"]
                    assert await next_frame(lines) == [
                        "event: snapshot",
                        'data: {"positions":[]}',
                    ]

                    posted = await http.post("/events", json=envelope_payload())
                    assert posted.status_code == 200

                    frame = await next_frame(lines)
                    assert frame[0] == "event: positions"
                    payload = json.loads(frame[1].removeprefix("data: "))
                    assert payload["positions"][0]["counterparty"] == COUNTERPARTY
        finally:
            # `should_exit` is uvicorn's own graceful stop. Awaiting the task is what makes the
            # assertion below meaningful — and what stops this test leaving a listening socket and a
            # running task behind for whichever test runs next.
            server.should_exit = True
            await asyncio.wait_for(serving, timeout=STREAM_TIMEOUT)

        # The shutdown half of the lifespan ran, which is the other thing only a real server proves.
        assert service.hub.subscribers == 0


async def _bound_port(server: uvicorn.Server) -> int:
    """The port uvicorn actually bound, once it has started listening.

    Polled rather than slept on: the usual case costs a single poll, and the bound turns "the server
    never came up" into a named failure instead of a suite that stops. This is the one place in the
    file that waits on wall-clock time, because binding a socket is a real event in the operating
    system and there is nothing else to synchronise on.
    """
    for _ in range(STARTUP_POLLS):
        if server.started:
            break
        await asyncio.sleep(STARTUP_POLL_SECONDS)
    else:
        raise AssertionError("uvicorn never reported itself started")
    return int(server.servers[0].sockets[0].getsockname()[1])


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------
class TestDiagnostics:
    """The four operator endpoints, and the two decisions that are visible as absences."""

    @pytest.mark.asyncio
    async def test_healthz_is_liveness_only(self) -> None:
        """⚠️ A HEALTH CHECK THAT VERIFIES ITS DEPENDENCIES TURNS ONE OUTAGE INTO A BIGGER ONE.

        Asserted with a store that fails every read: `/healthz` must still be 200, because a slow or
        unreachable store would otherwise fail every replica's check at once, the orchestrator would
        restart all of them together, and a service that could still serve cached reads would serve
        nothing — while the restarts added load to the dependency that was already struggling.

        `reads_attempted == 0` is the real assertion. A 200 alone would also be produced by a check
        that touched the store and swallowed the error, which is a different and worse design.
        """
        store = _UnreachableStore()
        async with running(Service(store)) as (http, _):
            response = await http.get("/healthz")

            assert response.status_code == 200
            assert response.json() == {"status": "ok"}
            assert store.reads_attempted == 0

    @pytest.mark.asyncio
    async def test_there_is_no_readiness_probe(self) -> None:
        """ADR-053 as a test, so an absence with a reason is not mistaken for an omission.

        Real lag is `producer_head_event_id - last_event_id` and only the broker knows the head. A
        readiness probe would therefore have to invent a threshold — "ready if staleness < 30s" —
        which on a quiet stream marks a perfectly caught-up projection as unready and takes a
        healthy service out of rotation for the crime of having nothing to do. `/projection`
        publishes the raw facts instead and whoever can see both sides does the subtraction.
        """
        async with running() as (http, _):
            assert (await http.get("/readyz")).status_code == 404

    @pytest.mark.asyncio
    async def test_projection_publishes_the_numbers_a_lag_alert_needs(self) -> None:
        """The keys are pinned because a dashboard is a consumer, and a renamed metric breaks it.

        `checkpoint` and `last_event_id` are both here and they answer different questions — what a
        consumer would resume from, and the highest event seen. Their disagreement is diagnostic.
        """
        async with running() as (http, _):
            await http.post("/events", json=envelope_payload())

            body = (await http.get("/projection")).json()

            assert body["events_seen"] == 1
            assert body["applied"] == 1
            assert body["checkpoint"] == 1
            assert body["last_event_id"] == 1
            assert body["instructions_remembered"] == 1
            assert body["freshness"]["reached"] == 1
            assert body["skipped"]["duplicate"] == 0

    @pytest.mark.asyncio
    async def test_cache_reports_no_hit_rate_before_there_has_been_traffic(self) -> None:
        """`null`, never `0.0`. A rate of zero is a claim about behaviour; absence is the truth.

        On a dashboard the difference is between a panel that says "no data" and one that draws a
        line along the bottom of the graph, which is what somebody pages about at 2am.
        """
        async with running() as (http, _):
            body = (await http.get("/cache")).json()

            assert body["hit_rate"] is None
            assert body["entries"] == 0

    @pytest.mark.asyncio
    async def test_subscribers_reports_the_number_to_alert_on(self) -> None:
        """`overflow_disconnects` is the one to page on: a client that could not keep up."""
        async with running() as (http, service):
            body = (await http.get("/subscribers")).json()

            assert body == {
                "subscribers": 0,
                "published": 0,
                "overflow_disconnects": 0,
                "detail": [],
            }
            assert service.hub.subscribers == 0

    def test_the_diagnostics_are_not_described_in_the_openapi_schema(self) -> None:
        """The stated cost of returning `as_dict()` verbatim, asserted so it stays a decision.

        The field names belong to the objects that own the numbers, and a pydantic mirror of them
        would be a second place to rename a metric. The trade is that these three bodies are
        described only as "an object with some properties": there is no named schema in
        `components`, so a generated client gets no type for them. They are for an operator with
        `curl`. `/healthz` does have a model — a `$ref` — because a probe's response shape is a
        contract with an orchestrator.
        """
        schema = create_app().openapi()

        def response_schema(path: str) -> dict[str, Any]:
            content = schema["paths"][path]["get"]["responses"]["200"]["content"]
            body: dict[str, Any] = content["application/json"]["schema"]
            return body

        assert response_schema("/healthz")["$ref"].endswith("/HealthPayload")
        for path in ("/projection", "/cache", "/subscribers"):
            assert "$ref" not in response_schema(path)
            assert response_schema(path)["type"] == "object"

    def test_the_documented_routes_are_exactly_the_eight_that_exist(self) -> None:
        """One assertion that fails when a route is added, removed or renamed.

        Worth having because a route is the most public thing in the repository: the docs, the tags
        and the paths are all things a client builds against, and a path changed by accident is
        indistinguishable from a service that is down.
        """
        assert set(create_app().openapi()["paths"]) == {
            "/positions/{counterparty}/{currency}",
            "/positions/{counterparty}",
            "/stream/positions",
            "/events",
            "/healthz",
            "/projection",
            "/cache",
            "/subscribers",
        }

    def test_the_docs_separate_what_a_client_calls_from_what_an_operator_reads(self) -> None:
        """Tags, so a generated page does not present a diagnostics probe as part of the product."""
        schema = create_app().openapi()

        assert schema["paths"]["/positions/{counterparty}"]["get"]["tags"] == ["positions"]
        assert schema["paths"]["/cache"]["get"]["tags"] == ["diagnostics"]


# ---------------------------------------------------------------------------
# Payloads and key parsing
# ---------------------------------------------------------------------------
class TestPayloads:
    """The response models, tested as functions of a `Position` rather than through a request."""

    def test_a_large_amount_renders_as_digits_and_not_in_exponent_form(self) -> None:
        """⚠️ `f"{amount:f}"` RATHER THAN `str(amount)`, AND THIS IS THE VALUE THAT SHOWS WHY.

        `str(Decimal("1E+3"))` is `"1E+3"`. A client parsing that as money is a client that will one
        day parse it wrong — and the value arrives naturally from arithmetic on quantized decimals,
        so this is not a hypothetical shape. Fixed-point formatting is the whole fix.
        """
        assert TotalsPayload.of(Totals(amount=Decimal("1E+3"), count=1)).amount == "1000"
        assert TotalsPayload.of(Totals(amount=Decimal("0.00"), count=0)).amount == "0.00"

    def test_the_trailing_underscore_on_reversed_does_not_reach_the_wire(self) -> None:
        """The model field is `reversed_` to dodge the builtin; the wire name is `reversed`.

        A client has no business inheriting a Python workaround, and `reversed_` is exactly the kind
        of field name that gets quietly corrected by whoever writes the consumer — producing a
        client that reads `undefined` and reports zero.
        """
        payload = PositionPayload.of(Position(key=KEY, reversed_=Totals(Decimal("5.00"), 1)))

        dumped = payload.model_dump()
        assert dumped["reversed"] == {"amount": "5.00", "count": 1}
        assert "reversed_" not in dumped

    def test_each_bucket_reaches_the_field_that_names_it(self) -> None:
        """Four buckets, four distinct amounts, four distinct counts — so no cross-wiring passes.

        This looks like over-specification and it is the opposite. The other full-body assertion in
        this file reads a position with one non-zero bucket, so three of the four fields are
        `{"amount": "0.00", "count": 0}` there and any mix-up *between those three* is invisible.
        The mutation matrix found it by filling `failed` from `settled` and watching the suite stay
        green — which on a settlement view means a failed instruction reading as a completed one,
        the single most expensive wrong answer this payload can give.

        Distinct values in every bucket is what makes the mapping asserted rather than assumed, and
        distinct counts as well as amounts because the two are separate assignments.
        """
        payload = PositionPayload.of(
            Position(
                key=KEY,
                pending=Totals(Decimal("10.01"), 1),
                settled=Totals(Decimal("20.02"), 2),
                failed=Totals(Decimal("30.03"), 3),
                reversed_=Totals(Decimal("40.04"), 4),
            )
        )

        assert payload.model_dump() == {
            "counterparty": KEY.counterparty,
            "currency": KEY.currency,
            "version": 0,
            "updated_at": None,
            "pending": {"amount": "10.01", "count": 1},
            "settled": {"amount": "20.02", "count": 2},
            "failed": {"amount": "30.03", "count": 3},
            "reversed": {"amount": "40.04", "count": 4},
        }

    def test_a_position_nothing_has_touched_reports_no_update_time(self) -> None:
        """`null` rather than "now": the time is *event* time, and there has been no event."""
        assert PositionPayload.of(Position(key=KEY)).updated_at is None

    def test_the_count_survives_the_round_trip(self) -> None:
        """One 4.2m trade and eight hundred small ones are the same exposure, different situations.

        A read model that reported only the sum would make the difference invisible to the screen
        that exists to show it, so the count is part of every bucket rather than a summary field.
        """
        payload = TotalsPayload.of(Totals(amount=Decimal("4200000.00"), count=800))

        assert (payload.amount, payload.count) == ("4200000.00", 800)

    def test_an_error_keeps_its_headers_out_of_its_body(self) -> None:
        """`headers` is a separate parameter so a typo cannot turn a header into a body field.

        `retry-after` in the body would be read by nothing at all, and the mistake is invisible
        until somebody wonders why clients are not backing off.
        """
        error = problem(
            503,
            "not_caught_up",
            "still catching up",
            headers={"retry-after": "1"},
            at=7,
        )

        assert error.status_code == 503
        assert error_detail(error) == {
            "error": "not_caught_up",
            "message": "still catching up",
            "at": 7,
        }
        assert error.headers == {"retry-after": "1"}


class TestKeyParsing:
    """`parse_position_key` — where a URL becomes a key, canonicalised the decoder's way."""

    def test_the_printed_form_of_a_key_parses_back_to_the_key(self) -> None:
        """The property that makes `?keys=` usable: a client can send back what the API printed.

        Without it, every consumer would have to know how to take a key apart and put it together
        again, and the two halves would drift.
        """
        assert parse_position_key(str(KEY)) == KEY

    def test_the_currency_is_upper_cased_and_the_counterparty_is_not(self) -> None:
        """The decoder's exact asymmetry, in one assertion, in the layer that has to match it."""
        parsed = parse_position_key("  AcmeBank /usd")

        assert parsed == PositionKey(counterparty="AcmeBank", currency="USD")

    def test_a_counterparty_containing_a_slash_survives(self) -> None:
        """`rpartition`, not `split`: a counterparty may contain a slash and a currency may not.

        A left-hand split would turn `NORTH/SOUTH FUND/USD` into counterparty `NORTH` and currency
        `SOUTH FUND/USD`, and the resulting 400 would look like the client's fault.
        """
        assert parse_position_key("NORTH/SOUTH FUND/USD") == PositionKey(
            counterparty="NORTH/SOUTH FUND", currency="USD"
        )

    @pytest.mark.parametrize("raw", ["", "ACME-BANK", "/USD", "   /USD"])
    def test_something_that_is_not_a_key_is_a_400_naming_the_form(self, raw: str) -> None:
        """Four ways to send no key, all of them the caller's error, all of them saying so.

        The message includes an example, because "malformed" without a form is a support ticket.

        `pytest.raises(HTTPException)` rather than the bare `Exception`, so `raised.value.detail` is
        a real attribute rather than one reached through an `Exception` that has no such member.
        """
        with pytest.raises(HTTPException) as raised:
            parse_position_key(raw)

        detail = error_detail(raised.value)
        assert raised.value.status_code == 400
        assert detail["error"] == "malformed_key"
        assert "COUNTERPARTY/CURRENCY" in detail["message"]

    @pytest.mark.parametrize("currency", ["US", "USDD", "US1", "12"])
    def test_something_that_is_not_a_currency_code_is_a_400(self, currency: str) -> None:
        """Three letters, and letters — the same rule the decoder applies to an event's currency."""
        with pytest.raises(HTTPException) as raised:
            parse_position_key(f"{COUNTERPARTY}/{currency}")

        assert raised.value.status_code == 400
        assert error_detail(raised.value)["error"] == "malformed_currency"


# ---------------------------------------------------------------------------
# Composition
# ---------------------------------------------------------------------------
class TestComposition:
    """`Service` and `create_app` — the wiring, which is where the bug is if there is one."""

    @pytest.mark.asyncio
    async def test_one_ingest_reaches_both_the_cache_and_the_subscribers(self) -> None:
        """⚠️ THE SINGLE INTEGRATION LINE IN THE WHOLE SERVICE, ASSERTED IN ONE TEST.

        `on_change=combine_listeners(self.cache.on_change, self.hub.publish)` is the entire join
        between four packages that do not know about each other. A refactor that dropped one
        listener would leave the other working, so a test that checked only one of them would pass —
        hence both sides in one test.

        The cache is read first on purpose: it refreshes in place and never inserts, so without a
        cold read to create the entry there would be nothing for the listener to refresh.
        """
        service = Service()
        subscription = service.hub.subscribe()
        assert await service.cache.position_for(KEY) is None

        await service.ingest(envelope_payload())

        cached = await service.cache.position_for(KEY)
        assert cached is not None and cached.version == 1
        assert service.cache.as_dict()["refreshed"] == 1
        delivered = await asyncio.wait_for(subscription.drain(), timeout=STREAM_TIMEOUT)
        assert [position.key for position in delivered] == [KEY]

    @pytest.mark.asyncio
    async def test_the_cache_is_never_handed_to_the_projector_as_a_store(self) -> None:
        """The projector must read the *store*, or the projection would compute from cached values.

        A cache in that position would let the fold read a possibly-stale position, add a delta to
        it, and write the result back as truth — a permanent error rather than a stale read. The
        wiring is asserted by identity, which is the only thing that can go wrong here.
        """
        store = InMemoryPositionStore()
        service = Service(store)

        assert service.store is store
        assert service.projector._store is store

    @pytest.mark.asyncio
    async def test_the_lifespan_closes_every_subscription_on_the_way_down(self) -> None:
        """⚠️ WITHOUT THIS A GRACEFUL SHUTDOWN IS A KILL, AND A KILL LOSES WORK.

        Every open SSE connection has a task parked on its subscription's wakeup event. If nothing
        closes the hub, those tasks are still waiting when the server tries to stop, so shutdown
        hangs until its timeout and is then killed — and a deployment that always ends in a kill is
        one where nothing else gets a chance to finish either.
        """
        service = Service()
        subscription = service.hub.subscribe()

        async with running(service) as (http, _):
            assert (await http.get("/healthz")).status_code == 200
            assert not is_closed(subscription)

        assert is_closed(subscription)
        assert service.hub.subscribers == 0

    @pytest.mark.asyncio
    async def test_the_factory_builds_its_own_service_when_given_none(self) -> None:
        """Production passes a service; the default keeps `uvicorn ... --factory` a one-liner.

        And it is the same code path either way — there is no test-only branch in `create_app`,
        which is the property the factory was worth paying for.
        """
        app = create_app()

        async with app.router.lifespan_context(app):
            transport = ASGITransport(app=app)
            async with AsyncClient(transport=transport, base_url=BASE_URL) as http:
                assert (await http.post("/events", json=envelope_payload())).status_code == 200
                assert (await http.get(POSITION_PATH)).json()["version"] == 1

    @pytest.mark.asyncio
    async def test_two_apps_do_not_share_a_store(self) -> None:
        """The reason there is a factory at all: a module-level `app` would make the store a global.

        Two tests in one process would then observe each other's positions, and a test would pass or
        fail depending on which file it lives in — the failure mode that makes a suite untrustworthy
        rather than merely red.
        """
        async with running() as (first, _):
            await first.post("/events", json=envelope_payload())
            assert (await first.get(POSITION_PATH)).status_code == 200

            async with running() as (second, _):
                assert (await second.get(POSITION_PATH)).status_code == 404

    @pytest.mark.asyncio
    async def test_an_injected_store_is_the_one_that_is_used(self) -> None:
        """The seam a real deployment uses: pass a Postgres-backed store and change nothing else.

        `PositionStore` is the annotation, so this test also states that a plain in-memory store is
        substitutable for whatever a deployment brings — which is what `test_store.py`'s contract
        exists to make true of the replacement.
        """
        store: PositionStore = InMemoryPositionStore()
        async with running(Service(store)) as (http, service):
            await http.post("/events", json=envelope_payload())

            assert service.store is store
            assert await store.checkpoint() == 1

    def test_the_repr_names_what_an_operator_would_want_to_know(self) -> None:
        """A `Service` in a traceback should name its store, its subscribers and its progress."""
        text = repr(Service())

        assert "InMemoryPositionStore" in text
        assert "subscribers=0" in text
        assert "checkpoint_reached=0" in text
