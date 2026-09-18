"""The inbound event contract: what this service is willing to read off the stream.

WHY THIS ENVELOPE IS RE-DECLARED HERE RATHER THAN IMPORTED FROM THE PRODUCER
---------------------------------------------------------------------------------
The events this read model consumes are published by a separate service (a settlement engine, in the
sibling project) which defines its own `Envelope` dataclass. Importing that class would be less code
and it would be the wrong thing, for a reason that is the whole point of CQRS being two deployables:

  - A shared class is a shared *build*. The read model could no longer be released, rolled back or
    even compiled without the write model's package at a compatible version, which converts an
    asynchronous, decoupled integration into a synchronous, coupled one at the only place where the
    decoupling actually mattered.
  - A shared class hides breakage instead of surfacing it. If the producer renames a field, an
    imported dataclass keeps compiling and this service starts reading `None`. A hand-written
    decoder raises `MalformedEventError` on the very first message, at the boundary, with the field
    name in it.
  - The contract is the *wire format*, not the Python type. Writing the decoder by hand keeps that
    honest: this file is a statement about JSON, and it would still be correct if the producer were
    rewritten in Go tomorrow.

So the duplication is deliberate and is the cheaper of the two costs. What makes it safe is that the
duplication is *narrow* — this file names only the fields this service reads, and ignores the rest.

WHAT THIS SERVICE READS, AND WHAT IT DELIBERATELY IGNORES
---------------------------------------------------------------------------------
`schema_version` is read and checked. Everything else that is not named below is dropped without
comment, including fields added by future producer versions. That is forward compatibility as a
positive decision rather than an accident: a producer that adds a field must not be able to break a
consumer that does not want it, and the only way to guarantee that is for the consumer to be
explicit about what it takes and silent about everything else.

⚠️ ORDERING COMES FROM `event_id`, NOT FROM A TIMESTAMP, AND NOT FROM ARRIVAL ORDER.
   `event_id` is the producer's outbox row id: assigned by one writer, monotonically increasing, and
   unique forever. That single property is what lets this read model be idempotent, because it makes
   "have I already seen this?" answerable by comparing integers instead of by remembering event
   payloads. `occurred_at` cannot do that job — two events can share a timestamp, clocks move
   backwards, and a backlogged relay publishes old events with old timestamps in the right order
   anyway. Arrival order cannot do it either, because at-least-once delivery redelivers.

   The limit of this argument is written down in DECISIONS.md (ADR-005): it holds for ONE producer
   with ONE outbox sequence. Two producers, or a producer whose sequence is reset, breaks it — and
   breaks it silently, which is why the projector treats a *regression* in `event_id` as a
   first-class outcome rather than as an impossible state.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Final

from .money import MoneyError, parse_amount

#: The schema version this service understands. A message declaring anything else is refused at the
#: boundary rather than half-read.
#:
#: Refusing a *higher* version looks over-cautious — surely a v2 message is a v1 message plus
#: fields? Sometimes. And sometimes v2 is where `amount` became a minor-unit integer, in which case
#: optimistically reading it as a decimal turns 1500 dollars into 15 dollars in a report somebody
#: trades on. The version exists precisely so that this decision is explicit; the honest default for
#: money is to stop.
SUPPORTED_SCHEMA_VERSION: Final = 1

#: The event types that move a position. Written out as constants because a string literal at three
#: call sites is three places to make the same typo, and a typo'd event type does not raise — it
#: silently classifies every matching event as "not for us" and the projection stays quietly empty.
EVENT_RECEIVED: Final = "instruction.received"
EVENT_VALIDATED: Final = "instruction.validated"
EVENT_MATCHED: Final = "instruction.matched"
EVENT_SETTLED: Final = "instruction.settled"
EVENT_FAILED: Final = "instruction.failed"
EVENT_REVERSED: Final = "instruction.reversed"

#: The aggregate type this read model projects. The stream is shared, so a filter is required rather
#: than optional: a `payment.*` event on the same topic must be skipped, not mis-parsed.
AGGREGATE_INSTRUCTION: Final = "instruction"


class MalformedEventError(Exception):
    """The message is not something this service can read at all.

    ⚠️ A DISTINCT TYPE, FOR AN OPERATIONAL REASON RATHER THAN AN AESTHETIC ONE.

       A consumer has to sort every failure into "retry this" or "give up on this", because that
       decision is the difference between waiting for a database to come back and blocking a
       partition forever. A malformed message is the clearest possible PERMANENT failure: the same
       bytes will not become valid in thirty seconds. Retrying it is pure waste, and retrying it
       forever stalls every well-formed message queued behind it.

       This is deliberately *not* the same as "an event I do not care about". An unknown event type
       is normal and is handled by returning a skip; a missing `event_id` is a bug in something and
       is raised. Collapsing the two would mean a producer bug looked exactly like routine
       filtering.
    """


@dataclass(frozen=True)
class Envelope:
    """One decoded event. Frozen, because an event is history.

    Mutating a consumed event is always a mistake — it records something that already happened, and
    the only honest thing to do with it is read it. Freezing makes the accidental version fail at
    the point of the mistake instead of producing a projection that disagrees with the log it was
    built from."""

    #: The producer's outbox row id: the deduplication key and the ordering token. See the module
    #: docstring for why this and not a timestamp.
    event_id: int
    event_type: str

    #: Which kind of thing the event is about. Read and compared rather than assumed, because the
    #: topic is shared: a `payment.settled` event has a plausible-looking payload and would project
    #: into a position that no settlement instruction backs. Filtering on the event type alone is
    #: not enough — two aggregate types can legitimately share a verb.
    aggregate_type: str
    aggregate_id: uuid.UUID

    #: When the event HAPPENED, not when it was published or consumed. Those differ by however far
    #: the relay was behind, so using publication time makes every "as of" report subtly wrong after
    #: any backlog — wrong in a way that is nearly impossible to notice and impossible to
    #: reconstruct later. This service uses it for one thing only: measuring lag.
    occurred_at: datetime

    #: The two fields that decide *which* position this event belongs to. Read out of the payload
    #: and promoted onto the envelope because they are the routing key for this read model, and
    #: because a projection function that has to reach into `data["counterparty"]` is a projection
    #: function that cannot be type-checked.
    counterparty: str
    currency: str

    #: The instruction's amount. `Decimal`, never `float` — see `money.py`.
    amount: Decimal


def _require(payload: dict[str, Any], field: str) -> Any:
    """Read a required field, or say precisely which one was missing.

    The alternative, `payload[field]`, raises `KeyError: 'counterparty'` — which is the same
    information, in a form that reaches an operator as an unhandled exception rather than as a
    classified permanent failure. The wrapper exists so that every missing field produces one
    exception type and one readable message, because the first question asked about a poison message
    is always "what was wrong with it", and the answer should be in the log line.
    """
    if field not in payload:
        raise MalformedEventError(f"missing required field {field!r}")
    value = payload[field]
    if value is None:
        # Distinguished from absence on purpose. A `null` in JSON is a producer that *tried* to send
        # the field, which usually means an upstream optional column rather than a serialiser bug —
        # a different investigation, so a different message.
        raise MalformedEventError(f"field {field!r} is null")
    return value


def decode_envelope(payload: dict[str, Any]) -> Envelope:
    """Turn one decoded JSON object into an `Envelope`, or refuse it.

    Everything this function rejects, it rejects *before* the projection sees it. That ordering is
    the point: a projection that has to defend itself against a missing `amount` is a projection
    whose arithmetic is tangled up with parsing, and the arithmetic is the part that has to be
    obviously correct. One boundary, one place where untrusted input becomes a typed value.

    WHY THE TYPE CHECKS ARE EXPLICIT RATHER THAN LEFT TO DUCK TYPING
      `int(payload["event_id"])` would accept `"12"`, `12.9` and `True` — three different producer
      bugs, all silently absorbed, one of which (`12.9` → `12`) makes two distinct events share a
      deduplication key. `isinstance` checks are more code and they turn every one of those into a
      refusal at the boundary with the offending value in the message.
    """
    event_id = _require(payload, "event_id")
    # `bool` is a subclass of `int` in Python, so `isinstance(True, int)` is True. Excluding it
    # explicitly is not pedantry: `{"event_id": true}` is a real serialiser bug (a field that used
    # to be a flag), and letting it through means every such event deduplicates against event id 1.
    if isinstance(event_id, bool) or not isinstance(event_id, int):
        raise MalformedEventError(f"event_id must be an integer, got {type(event_id).__name__}")
    if event_id < 1:
        # Zero and negatives are refused because the projector uses "last applied event id" with 0
        # as its "nothing yet" sentinel. Allowing 0 on the wire would make a real event
        # indistinguishable from an empty projection — a bug that only shows up as a duplicate being
        # applied once.
        raise MalformedEventError(f"event_id must be positive, got {event_id}")

    schema_version = _require(payload, "schema_version")
    # The TYPE is checked as well as the value, and the reason is Python's `==` on numbers:
    # `1 == 1.0` and `1 == True` are both True, so a bare `!= SUPPORTED_SCHEMA_VERSION` accepts
    # `{"schema_version": 1.0}` and `{"schema_version": true}` as compatible. Neither is a message
    # this service has ever been verified against, and this field's entire job is to be the one
    # place where "we have not verified this" is said out loud. A float or a bool here means the
    # producer's serialiser is doing something unexamined, which is exactly when to stop.
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        raise MalformedEventError(
            f"schema_version must be an integer, got {type(schema_version).__name__}"
        )
    if schema_version != SUPPORTED_SCHEMA_VERSION:
        raise MalformedEventError(
            f"unsupported schema_version {schema_version!r}; "
            f"this service reads {SUPPORTED_SCHEMA_VERSION}"
        )

    event_type = _require(payload, "event_type")
    if not isinstance(event_type, str):
        raise MalformedEventError(f"event_type must be a string, got {type(event_type).__name__}")

    aggregate_type = _require(payload, "aggregate_type")
    if not isinstance(aggregate_type, str):
        raise MalformedEventError(
            f"aggregate_type must be a string, got {type(aggregate_type).__name__}"
        )

    raw_aggregate_id = _require(payload, "aggregate_id")
    if not isinstance(raw_aggregate_id, str):
        raise MalformedEventError("aggregate_id must be a string containing a UUID")
    try:
        aggregate_id = uuid.UUID(raw_aggregate_id)
    except ValueError as exc:
        # `from exc` deliberately: the ValueError's message names what was wrong with the string,
        # and discarding it would leave an operator with "bad aggregate_id" and no idea why.
        raise MalformedEventError(f"aggregate_id is not a UUID: {raw_aggregate_id!r}") from exc

    occurred_at = _parse_timestamp(_require(payload, "occurred_at"))

    data = payload.get("data")
    if not isinstance(data, dict):
        raise MalformedEventError("data must be an object")

    counterparty = _require(data, "counterparty")
    if not isinstance(counterparty, str) or not counterparty.strip():
        raise MalformedEventError("data.counterparty must be a non-empty string")

    currency = _require(data, "currency")
    if not isinstance(currency, str) or len(currency) != 3 or not currency.isalpha():
        # Three alphabetic characters is ISO 4217, and checking it here rather than trusting the
        # producer matters because currency is half of the position key: `"usd"`, `"USD "` and
        # `"US Dollar"` would silently become three separate positions that never reconcile.
        raise MalformedEventError(f"data.currency must be a 3-letter code, got {currency!r}")

    try:
        amount = parse_amount(_require(data, "amount"))
    except MoneyError as exc:
        # Re-wrapped rather than propagated. `MoneyError` is the money module's vocabulary; the
        # boundary's vocabulary is `MalformedEventError`, and a caller that has to catch both is a
        # caller that will eventually catch only one.
        raise MalformedEventError(f"data.amount is not usable as money: {exc}") from exc

    return Envelope(
        event_id=event_id,
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=aggregate_id,
        occurred_at=occurred_at,
        # Upper-cased at the boundary, so the position key is canonical by construction and no
        # downstream comparison has to remember to fold case. Doing this in the projection instead
        # would mean every future reader of the state had to know the rule.
        counterparty=counterparty.strip(),
        currency=currency.upper(),
        amount=amount,
    )


def _parse_timestamp(value: Any) -> datetime:
    """Parse an ISO-8601 timestamp and force it to be timezone-aware.

    ⚠️ A NAIVE DATETIME IS REFUSED RATHER THAN ASSUMED TO BE UTC.
       Assuming is the tempting default and it is how lag measurements end up off by the difference
       between the producer's local zone and this process's — a number that looks plausible, changes
       twice a year, and is never questioned because it is only ever a few hours. A producer that
       omits the offset has a bug worth fixing at the producer.

    `fromisoformat` in 3.11+ handles the `Z` suffix and offsets, so no third-party parser is needed
    for this. It does *not* handle every ISO-8601 form in existence, which is fine: the contract is
    what this producer emits, not the standard in full.
    """
    if not isinstance(value, str):
        raise MalformedEventError(f"occurred_at must be a string, got {type(value).__name__}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise MalformedEventError(f"occurred_at is not an ISO-8601 timestamp: {value!r}") from exc
    if parsed.tzinfo is None:
        raise MalformedEventError(
            f"occurred_at has no timezone offset: {value!r}; "
            "a naive timestamp cannot be compared against this service's clock"
        )
    # Normalised to UTC so that every stored and compared timestamp is in one zone. Keeping the
    # original offset would be more faithful and would make every comparison in the codebase a place
    # where somebody could forget that two aware datetimes in different zones sort correctly but do
    # not format comparably.
    return parsed.astimezone(UTC)
