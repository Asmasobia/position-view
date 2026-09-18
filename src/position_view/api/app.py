"""The HTTP surface: eight routes, one framework, and every decision that only exists at the edge.

WHY `create_app(service)` AND NOT A MODULE-LEVEL `app`
---------------------------------------------------------------------------------
A module-level `app = FastAPI()` is the shape every tutorial uses and it makes the application a
side effect of importing a module. Two consequences, both unpleasant: the store is created at import
time, so every test in the process shares it and tests start depending on each other's data; and
there is no way to hand the app a different store without an environment variable and an `if`.

A factory takes the `Service` (which is itself the composition root — see `service.py`) and returns
an app wired to it. Production calls it once; a test calls it per test with a fresh service. There
is no test-only branch anywhere in this file, which is the property worth paying a factory for.

WHY THE ROUTES CLOSE OVER `service` INSTEAD OF USING `Depends`
---------------------------------------------------------------------------------
FastAPI's idiom for this is a dependency — `service: Service = Depends(get_service)` — reaching into
`app.state`. It exists for a real reason: overriding a dependency in a test is how you swap a
collaborator without touching the app. This app does not need that, because the swap happens one
level up, in the `Service` the factory is handed.

What the closure buys is types. `app.state` is an untyped namespace, so `request.app.state.service`
is `Any`, and `mypy --strict` cannot tell you that a renamed method broke a route. The closed-over
variable is a `Service` all the way through. Given the choice between the framework's extension
point and a version the type checker can see, and no requirement for the extension point, this file
takes the types.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any, Final

from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse

from position_view.model import PositionKey, Skipped
from position_view.projection import Refused

from .payloads import (
    HealthPayload,
    IngestedPayload,
    PositionListPayload,
    PositionPayload,
    parse_position_key,
    problem,
)
from .service import DEFAULT_AWAIT_TIMEOUT_MS, MAX_AWAIT_TIMEOUT_MS, Service
from .stream import SSE_MEDIA_TYPE, position_stream, stream_headers

API_TITLE: Final = "position-view"
API_VERSION: Final = "0.1.0"

#: Two tags, so the generated docs separate "what a client calls" from "what an operator reads".
#: Without them every route lands in one flat list and the diagnostics read like part of the
#: product, which is how an operator's probe becomes something a client depends on.
TAG_POSITIONS: Final = "positions"
TAG_DIAGNOSTICS: Final = "diagnostics"


def create_app(service: Service | None = None) -> FastAPI:
    """Build the ASGI application. Pass a `Service` to control the store; omit it for in-memory.

    Everything below is registered against the closure, and the lifespan hook is what ties the app's
    life to the service's: seed the freshness gate on the way up, close every subscription on the
    way down.
    """
    state = service if service is not None else Service()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        """Startup and shutdown, as one function with a `yield` in the middle.

        ⚠️ THE SHUTDOWN HALF IS NOT HOUSEKEEPING. Every open SSE connection has a task parked on its
           subscription's wakeup event. Without `state.close()` those tasks are still waiting when
           the server tries to stop, so a graceful shutdown either hangs until its timeout or is
           killed — and a deployment that always ends in a kill is a deployment where nothing else
           gets a chance to finish either. Closing the hub wakes every waiter, each stream ends
           cleanly, and the clients reconnect to the new process on their own.

        The `finally` matters for the same reason it matters in the hub's own context manager: a
        failure during startup, or a cancelled lifespan, must still release what was taken.
        """
        await state.start()
        try:
            yield
        finally:
            state.close()

    app = FastAPI(
        title=API_TITLE,
        version=API_VERSION,
        summary="A CQRS read model over a settlement event stream: positions, freshness, and SSE.",
        lifespan=lifespan,
    )

    async def wait_until_fresh(await_event_id: int | None, timeout_ms: int) -> None:
        """Honour `?await_event_id=`, or refuse to answer staler than the caller asked for.

        ⚠️ A TIMEOUT HERE RETURNS AN ERROR RATHER THAN A STALE POSITION, AND THAT IS THE POINT OF
           THE PARAMETER. The tempting alternative is to answer anyway with a header saying "this
           may be behind", because an answer feels more useful than a failure. It is not: a client
           that asked for its own write back and receives an older version with a 200 will render
           it, and the person watching sees the number they just changed unchanged, and tries
           again — which is how one stale read becomes a duplicate instruction. A
           503 is a thing the client can retry, log and alert on; a stale 200 is a thing it cannot
           even detect.

        503 rather than 504: nothing upstream timed out — this service simply has not caught up yet,
        which is precisely "temporarily unable to handle the request". `Retry-After` is included
        because the condition is expected to clear on its own, usually within milliseconds.

        The one hazard worth naming: some load balancers eject a backend that returns 5xx. That is
        survivable here because ejection is driven by the health check, and `/healthz` deliberately
        knows nothing about the projection's progress (ADR-053) — so a burst of freshness timeouts
        cannot take a healthy node out of rotation.
        """
        if await_event_id is None:
            return
        if await state.gate.wait_for(await_event_id, timeout_ms=timeout_ms):
            return
        raise problem(
            503,
            "not_caught_up",
            f"the projection has not reached event {await_event_id} within {timeout_ms}ms",
            awaited_event_id=await_event_id,
            checkpoint=state.gate.reached,
            headers={"retry-after": "1"},
        )

    # ----------------------------------------------------------------------------- positions
    #
    # The two read routes are registered longest-path-first for readability rather than necessity:
    # `/positions/{counterparty}/{currency}` and `/positions/{counterparty}` differ in segment
    # count, so no ordering can shadow the other. The route that *would* have been shadowed is the
    # stream: it lives at `/stream/positions` rather than `/positions/stream` because the latter is
    # one counterparty named "stream" away from being unreachable, and the failure would present as
    # a 404 for a position that exists.

    @app.get(
        "/positions/{counterparty}/{currency}",
        response_model=PositionPayload,
        tags=[TAG_POSITIONS],
        summary="One counterparty's exposure in one currency",
    )
    async def read_position(
        counterparty: str,
        currency: str,
        await_event_id: Annotated[int | None, Query(ge=1)] = None,
        timeout_ms: Annotated[int, Query(ge=0, le=MAX_AWAIT_TIMEOUT_MS)] = DEFAULT_AWAIT_TIMEOUT_MS,
    ) -> PositionPayload:
        """The hot path, and the only route served through the cache.

        `await_event_id` is the read-your-writes parameter: pass the `checkpoint` that `POST
        /events` returned and this read will not answer until the projection has passed it. Omit it
        and the read is as fast as the cache, with no coordination at all.
        """
        await wait_until_fresh(await_event_id, timeout_ms)

        # Reassembled into the string form and parsed by the same function the `keys=` parameter
        # uses, so a URL is canonicalised in exactly one place. Doing it inline here would be
        # three lines that have to stay in step with the decoder's rules forever — see
        # `parse_position_key`, which explains why upper-casing the counterparty would be a bug.
        key = parse_position_key(f"{counterparty}/{currency}")

        position = await state.cache.position_for(key)
        if position is None:
            # ⚠️ 404 AND NOT AN EMPTY POSITION. A zeroed body would mean "this counterparty has no
            #    outstanding exposure", which is a real and different fact from "this service has
            #    never heard of it" — and a misspelt counterparty in a URL would then look like good
            #    news. The store makes the same distinction for the same reason.
            raise problem(404, "no_position", f"no position for {key}")
        return PositionPayload.of(position)

    @app.get(
        "/positions/{counterparty}",
        response_model=PositionListPayload,
        tags=[TAG_POSITIONS],
        summary="Every currency this counterparty has exposure in",
    )
    async def read_positions(
        counterparty: str,
        await_event_id: Annotated[int | None, Query(ge=1)] = None,
        timeout_ms: Annotated[int, Query(ge=0, le=MAX_AWAIT_TIMEOUT_MS)] = DEFAULT_AWAIT_TIMEOUT_MS,
    ) -> PositionListPayload:
        """The natural question — "what do we have on with this counterparty?" — and it is uncached.

        ⚠️ THIS ROUTE READS THE WHOLE BOOK AND FILTERS IT, AND THAT IS NOT AN OVERSIGHT (ADR-039).
           `all_positions` is invalidated by a change to *any* position, so caching it on a live
           stream would produce a copy that is wrong more often than right. The cost is honest and
           stated: this is the one read whose work grows with the size of the book, so it belongs on
           a screen a person opens, not in a loop. A deployment that needs it at volume needs a
           store-side index and a cursor, which is a slice this repository does not pretend to have.

        Returns an empty list rather than a 404 for an unknown counterparty. A collection that
        exists and is empty is not a missing resource, and a client rendering a table wants zero
        rows rather than an exception — the 404 belongs on the single-position route, where the
        caller named one specific thing that does not exist.
        """
        await wait_until_fresh(await_event_id, timeout_ms)

        name = counterparty.strip()
        positions = [p for p in await state.store.all_positions() if p.key.counterparty == name]
        return PositionListPayload(
            counterparty=name,
            positions=[PositionPayload.of(position) for position in positions],
        )

    @app.get(
        "/stream/positions",
        tags=[TAG_POSITIONS],
        summary="Live positions over Server-Sent Events",
        response_class=StreamingResponse,
    )
    async def stream_positions(
        keys: Annotated[list[str] | None, Query()] = None,
    ) -> StreamingResponse:
        """A snapshot, then every change from now on, until the client hangs up.

        `?keys=ACME-BANK/USD&keys=BETA-TRUST/USD` watches those positions; omitting `keys` watches
        everything. A malformed key is a 400 *before* the stream starts, which is the one class of
        error this endpoint can still report properly — everything after the first byte can only be
        an `error` frame, and `position_stream` explains why that trade is the right way round.

        Note what is *not* offered: `Last-Event-ID` resumption. SSE has a mechanism for it, and this
        service cannot honour it, because it keeps no history of changes — only current state. A
        client reconnecting is given a fresh snapshot instead, which is correct by construction and
        is the reason the conflating buffer exists. Advertising a resumption we cannot deliver would
        be worse than not advertising one: the client would trust an id we would then ignore.
        """
        watched: list[PositionKey] | None = None
        if keys is not None:
            watched = [parse_position_key(key) for key in keys]
        return StreamingResponse(
            position_stream(state, watched),
            media_type=SSE_MEDIA_TYPE,
            headers=stream_headers(),
        )

    # ----------------------------------------------------------------------------- ingest

    @app.post(
        "/events",
        response_model=IngestedPayload,
        tags=[TAG_POSITIONS],
        summary="Hand one event to the projection",
    )
    async def ingest_event(payload: dict[str, Any]) -> IngestedPayload:
        """The consumer seam, exposed over HTTP so this service can be driven end to end.

        ⚠️ THIS IS NOT A WRITE ENDPOINT FOR A READ MODEL, AND THE DISTINCTION IS WORTH BEING PRECISE
           ABOUT. It does not accept a *position*; there is no way to tell this service what a
           counterparty's exposure is. It accepts an *event*, which is the same thing a broker
           consumer would hand the projector, and the projection decides what that means. In a real
           deployment the events arrive from the stream and this route would be restricted to
           operators, who need it for exactly one job: replaying a single event by hand during an
           incident, against a service they can then query.

        ⚠️ THE BODY IS `dict[str, Any]` AND IS NOT VALIDATED BY PYDANTIC ON THE WAY IN.
           That looks like a gap in a file whose whole subject is validation, and it is the
           opposite. `decode_envelope` is this service's wire contract: it is where `schema_version`
           is checked, where `1 == 1.0 == True` was found to be a real bug, where money is refused
           rather than rounded, and it has 127 tests. A pydantic model here would be a second
           definition of the same contract, and the first one to reject a payload would be the one
           with no tests behind it. Worse, its error shape would be pydantic's rather than the
           decoder's — so the reason a real event was refused would be reported in a vocabulary that
           does not match the projection's own logs. FastAPI validates what belongs to HTTP: paths,
           queries, response shapes. The event belongs to the model.

        A refusal is a 400 carrying the decoder's own message, because the caller sent something
        unreadable and the message names which field and why. A *skip* is not an error at all: a
        duplicate is at-least-once delivery working as designed, so it is a 200 with a reason.
        """
        outcome = await state.ingest(payload)

        if isinstance(outcome, Refused):
            raise problem(400, "malformed_event", outcome.error)

        checkpoint = await state.store.checkpoint()
        if isinstance(outcome, Skipped):
            return IngestedPayload(
                outcome="skipped",
                reason=outcome.reason.value,
                checkpoint=checkpoint,
            )
        return IngestedPayload(outcome="applied", checkpoint=checkpoint)

    # ----------------------------------------------------------------------------- diagnostics
    #
    # These three return `as_dict()` verbatim rather than through a response model, and that is
    # deliberate. The field names belong to the objects that own the numbers — `ProjectionStats`
    # says so in its own docstring — and a pydantic model mirroring them would be a second place to
    # rename a metric, which is how a dashboard breaks for a reason nobody can find. The trade:
    # these bodies are not described in the OpenAPI schema. They are for an operator with `curl` and
    # a dashboard query, not for a generated client.

    @app.get("/healthz", response_model=HealthPayload, tags=[TAG_DIAGNOSTICS])
    async def healthz() -> HealthPayload:
        """Liveness only: is this process able to serve? It checks nothing else, on purpose.

        ⚠️ A HEALTH CHECK THAT VERIFIES ITS DEPENDENCIES TURNS ONE BROKEN DEPENDENCY INTO AN OUTAGE.
           If this touched the store, a slow store would fail every replica's check at once, the
           orchestrator would restart all of them together, and a service that could still serve
           cached reads would instead serve nothing — while the restarts added load to the
           dependency that was already struggling. The store's health belongs in the store's own
           alerting, where one alert fires instead of every replica flapping.

        And there is no `/readyz`, which is a real decision rather than an omission — ADR-053. This
        service genuinely cannot know whether it is caught up: real lag is
        `producer_head_event_id - last_event_id`, and only the broker knows the head. A readiness
        probe would therefore have to invent a threshold ("ready if staleness < 30s"), which on a
        quiet stream marks a perfectly caught-up projection as unready. `/projection` publishes the
        two raw facts instead, and whoever can see both sides does the subtraction.
        """
        return HealthPayload(status="ok")

    @app.get("/projection", tags=[TAG_DIAGNOSTICS])
    async def projection() -> dict[str, Any]:
        """What the projection has done, plus the two numbers a lag alert needs and the store's own.

        `checkpoint` and `last_event_id` are different questions and both are here: the checkpoint
        is what a consumer would resume from, and `last_event_id` is the highest event *seen*. They
        agree in a healthy service and their disagreement is diagnostic.
        """
        return {
            **state.projector.stats.as_dict(),
            "checkpoint": await state.store.checkpoint(),
            "instructions_remembered": await state.store.snapshot_count(),
            "freshness": state.gate.as_dict(),
        }

    @app.get("/cache", tags=[TAG_DIAGNOSTICS])
    async def cache() -> dict[str, Any]:
        """The cache's counters. `hit_rate` is `null` until there has been traffic, never 0.0."""
        return state.cache.as_dict()

    @app.get("/subscribers", tags=[TAG_DIAGNOSTICS])
    async def subscribers() -> dict[str, Any]:
        """Who is streaming, and whether anybody has been cut off for falling behind.

        `overflow_disconnects` is the number to alert on: it means a client could not keep up with a
        subscription it asked for, which is either a client bug or a wildcard that should have been
        a key list.
        """
        return state.hub.as_dict()

    return app
