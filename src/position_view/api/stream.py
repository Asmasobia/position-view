"""Server-Sent Events: the framing, and the snapshot-to-stream handover done in the only safe order.

WHY SSE AND NOT WEBSOCKETS
---------------------------------------------------------------------------------
Every byte here travels one way. A client watching positions has nothing to say back, so a duplex
protocol buys nothing and costs a great deal: a WebSocket needs its own framing, its own ping/pong,
its own reconnect logic in the client, and it is invisible to every HTTP-shaped thing in between —
proxies, curl, a browser's network tab, an ordinary access log. SSE is a `GET` that never ends. It
reconnects on its own in any browser (`EventSource`), it is readable with `curl -N`, and a load
balancer needs no special configuration to pass it through.

WHY THIS IS HAND-WRITTEN AND NOT `sse_starlette`
---------------------------------------------------------------------------------
The protocol is four field names and a blank line between frames. The library would add a dependency
whose job is to write `data: ` for us, and it would take over exactly the two decisions worth making
deliberately — when to send a heartbeat, and what to do when a subscriber has fallen behind. Writing
it out means the interesting parts are visible in this file rather than configured in another one.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Iterable
from typing import TYPE_CHECKING, Any, Final

from position_view.model import Position, PositionKey
from position_view.push import SubscriberOverflow, SubscriptionClosed

from .payloads import PositionPayload

if TYPE_CHECKING:
    from .service import Service

SSE_MEDIA_TYPE: Final = "text/event-stream"

#: How long the stream will sit silent before sending a comment frame.
#:
#: ⚠️ A HEARTBEAT IS NOT OPTIONAL ON A LONG-LIVED IDLE CONNECTION, AND THE FAILURE IT PREVENTS IS
#:    INVISIBLE TO BOTH ENDS. Load balancers, reverse proxies and NAT tables all reap connections
#:    that have carried no bytes for some interval — commonly 30 to 60 seconds. The client is not
#:    told; it simply stops receiving updates for a position that has not changed in a while, which
#:    is indistinguishable from "nothing has happened", and stays that way until something makes it
#:    reconnect. A quiet stream is the *normal* state of a settlement view outside market hours, so
#:    the bug would show up as "the dashboard was stale overnight" and be impossible to reproduce at
#:    11am.
#:
#: 15 seconds is comfortably inside the shortest idle timeout in common use. The cost is one comment
#: frame per connection per 15 seconds, which is a rounding error next to the TCP keep-alives
#: already on the wire.
HEARTBEAT_SECONDS: Final = 15.0

#: What the client is asked to wait before reconnecting, sent once as the SSE `retry` field.
#:
#: Worth setting explicitly rather than accepting the browser default (3 seconds), because a
#: reconnect here is not cheap: this service cannot resume a stream from a point in the past, so
#: every reconnect re-reads a snapshot. On a wildcard subscription that is the whole book. Two
#: seconds is a compromise — quick enough that a real disconnect is not felt, slow enough that a
#: flapping proxy does not turn a hundred clients into a snapshot storm.
RETRY_MS: Final = 2000


def sse_frame(event: str, data: dict[str, Any]) -> bytes:
    """One SSE frame: an event name, one JSON payload line, and the blank line that ends it.

    Exactly one `data:` line, always, and that is safe rather than lucky: `json.dumps` escapes
    newlines inside strings, so its output cannot contain a literal `\\n`. A multi-line payload
    would need one `data:` prefix per line — an easy thing to get wrong, and the symptom is a client
    that silently sees a truncated JSON document.

    Bytes rather than `str` because that is what an ASGI response body is; encoding here means the
    encoding is decided once, in the place that knows the protocol, rather than by whatever the
    framework happens to default to.
    """
    payload = json.dumps(data, separators=(",", ":"))
    return f"event: {event}\ndata: {payload}\n\n".encode()


def sse_comment(text: str) -> bytes:
    """A comment frame: a line starting with `:`, which every SSE client is required to ignore.

    This is the heartbeat. It is deliberately *not* an event with a name like `ping`, because a
    named event is something a client's `addEventListener` map has to know about, and a client that
    has not heard of it will log an unknown-event warning every 15 seconds forever.
    """
    return f": {text}\n\n".encode()


def _positions_frame(event: str, positions: Iterable[Position]) -> bytes:
    """A frame carrying positions, in the one shape both the snapshot and the updates use.

    One shape rather than two, so a client can hand `snapshot` and `positions` frames to the same
    render function. The event *name* still differs, because the two mean different things to a
    client keeping its own state: a snapshot replaces what it holds, an update merges into it.
    """
    return sse_frame(event, {"positions": [PositionPayload.of(p).model_dump() for p in positions]})


async def _snapshot(service: Service, keys: list[PositionKey] | None) -> tuple[Position, ...]:
    """The current state of everything this subscriber is watching.

    Two paths on purpose, and the split is ADR-039's:

    - A named set of keys goes through the *cache*, because a keyed read is exactly what the cache
      exists for, and a reconnecting client asking for the same three positions should not reach the
      store at all.
    - A wildcard goes to the *store*, because `all_positions` is deliberately uncached: it is
      invalidated by a change to any position, so on a live stream a cached copy would be wrong more
      often than it was right.

    Absences are dropped rather than rendered as empty positions. A client that asked to watch a
    counterparty with no activity yet should be told nothing about it, not handed a row of zeros —
    "no exposure" and "exposure of zero" are different facts, and only one of them is known.
    """
    if keys is None:
        return await service.store.all_positions()

    found = [await service.cache.position_for(key) for key in keys]
    return tuple(position for position in found if position is not None)


async def position_stream(
    service: Service,
    keys: list[PositionKey] | None,
    *,
    heartbeat_seconds: float = HEARTBEAT_SECONDS,
    # ⚠️ `AsyncGenerator[bytes, None]` RATHER THAN THE WIDER `AsyncIterator[bytes]`, AND THE
    #    DIFFERENCE IS NOT COSMETIC. `AsyncIterator` promises only `__anext__`; `AsyncGenerator`
    #    also promises `aclose()` and `athrow()`. A caller that has to release the subscription
    #    early — which is every test in this file, and the server itself on a client disconnect —
    #    needs `aclose()`, and under `mypy --strict` it can only call it if the annotation says so.
    #    Narrowing the return type to what the function actually is costs nothing and makes the
    #    cleanup path type-checkable instead of type-ignored.
) -> AsyncGenerator[bytes, None]:
    """The whole endpoint body, as a byte stream: subscribe, snapshot, then updates forever.

    ⚠️ THE FIRST THREE STATEMENTS ARE IN THE ONLY ORDER THAT NEITHER LOSES NOR DUPLICATES DATA, AND
       ADR-033 WORKS THROUGH WHY BOTH OBVIOUS ALTERNATIVES ARE BROKEN.
       Subscribe *first*, so a change committed while the snapshot is being read is buffered rather
       than missed. Snapshot second. Then `mark_delivered(snapshot)`, so the versions the snapshot
       already carried are not immediately re-sent as though they were news. Snapshot-then-subscribe
       loses events in the gap; subscribe-then-stream without `mark_delivered` duplicates them.

    ⚠️ ALL OF THAT HAPPENS INSIDE THE GENERATOR, WHICH IS A DELIBERATE TRADE.
       Doing the handover in the route function instead would allow a clean `500` if the store
       failed, because the response would not have started yet. What it would also allow is a
       subscription created for a response that is never streamed — a client that disconnects in
       that window leaves a subscriber registered forever, being offered every change for the life
       of the process. Inside the generator, the `with` block's `finally` runs however the stream
       ends: normal return, client disconnect (a `CancelledError` thrown in here by the server), or
       a failure. A leak that grows with client churn is worse than a degraded error path, so the
       handover happens where the cleanup is guaranteed, and a mid-handover failure is reported as
       an `error` frame on a 200 that has already begun.
    """
    with service.hub.subscription(keys=keys) as subscription:
        # `retry` is sent on its own before anything else, so a client that disconnects during the
        # snapshot has already been told how long to wait — which is exactly the client most likely
        # to reconnect.
        yield f"retry: {RETRY_MS}\n\n".encode()

        try:
            snapshot = await _snapshot(service, keys)
        except Exception as error:
            # Broad on purpose, and it is not swallowing anything: the response has already started,
            # so there is no status code left to set, and the only two options are "tell the client
            # something went wrong" or "close the connection and let it guess". A client that
            # receives an `error` frame knows to retry rather than to render an empty book, which is
            # what a silent close looks like from the outside.
            yield sse_frame("error", {"error": "snapshot_failed", "message": str(error)})
            return

        subscription.mark_delivered(snapshot)
        yield _positions_frame("snapshot", snapshot)

        while True:
            try:
                # ⚠️ CANCELLING `drain` ON A HEARTBEAT TIMEOUT CANNOT LOSE A BATCH, AND THE REASON
                #    IS STRUCTURAL RATHER THAN LUCKY. `drain`'s only suspension point is waiting on
                #    its wakeup event; the take-and-clear that follows is synchronous. A
                #    `CancelledError` can therefore only ever arrive *before* the buffer has been
                #    taken, never while a batch is in hand — and the wakeup event stays set, so next
                #    `drain` picks up exactly what this one abandoned.
                batch = await asyncio.wait_for(subscription.drain(), timeout=heartbeat_seconds)
            except TimeoutError:
                yield sse_comment("keep-alive")
                continue
            except SubscriptionClosed:
                # The hub was closed, which happens on process shutdown. Ending the iterator closes
                # the response cleanly, so the client sees a completed stream and reconnects on its
                # own schedule rather than seeing a broken connection.
                return
            except SubscriberOverflow as overflow:
                # ⚠️ THE ONE CASE WHERE SILENCE WOULD BE A LIE. This subscriber fell so far behind
                #    that its buffer was discarded, so some updates are simply gone. Ending the
                #    stream quietly would be indistinguishable from a normal close, and the client
                #    would reconnect, re-snapshot, and be right again — or, if it is a client that
                #    does not reconnect, would keep rendering a view frozen at an arbitrary moment
                #    with no indication. Naming it means the client can decide.
                yield sse_frame("overflow", {"error": "overflow", "message": str(overflow)})
                return

            yield _positions_frame("positions", batch)


def stream_headers() -> dict[str, str]:
    """The headers that keep an SSE response from being helpfully broken by something in the middle.

    - `cache-control: no-store` — a cached event stream is not a stream, and a shared cache holding
      one would serve one client's positions to another.
    - `x-accel-buffering: no` — nginx buffers proxied responses by default, which for a stream means
      the client receives nothing until the buffer fills or the connection ends. The symptom is
      "SSE works locally and not in staging", and this header is the fix. It is nginx-specific and
      ignored by everything else, so it costs nothing to send unconditionally.
    - `connection: keep-alive` — belt and braces for HTTP/1.1 intermediaries; harmless on HTTP/2,
      where the server strips it.
    """
    return {
        "cache-control": "no-store",
        "x-accel-buffering": "no",
        "connection": "keep-alive",
    }


__all__ = [
    "HEARTBEAT_SECONDS",
    "RETRY_MS",
    "SSE_MEDIA_TYPE",
    "position_stream",
    "sse_comment",
    "sse_frame",
    "stream_headers",
]
