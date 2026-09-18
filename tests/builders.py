"""Builders shared by the model tests.

WHY THIS IS `builders.py` AND NOT `conftest.py`
---------------------------------------------------------------------------------
`conftest.py` is pytest's plugin file: hooks, fixtures, collection tweaks. Nothing here is any of
those, and putting helpers in it would make them arrive by magic — a reader of a test file would see
`make_envelope(...)` with no import line to follow. An ordinary module means an ordinary import, and
the import is the answer to "where did this come from?".

WHY THESE ARE PLAIN FUNCTIONS AND NOT PYTEST FIXTURES
---------------------------------------------------------------------------------
A fixture is the right tool when a test needs a resource *set up and torn down* — a temporary
directory, a client, a running task. None of that applies to a frozen dataclass. What these tests
need is the opposite: a value that is mostly default and differs in the one field the test is about,
constructed inline so the difference is visible on the assertion's own screen.

Written as a fixture, `test_rejects_naive_timestamp` would take a `payload` argument and mutate it,
and a reader would have to open another file to learn what a valid payload even looks like. Written
as a builder with keyword overrides, the test reads
`envelope_payload(occurred_at="2026-03-01T09:30")` and the interesting part is the only part written
down.

THE ONE RULE THESE BUILDERS FOLLOW: A DEFAULT MUST BE VALID AND UNREMARKABLE
Every default below produces an event this service accepts. That is what makes an override
meaningful — if a test overriding `currency` fails, it failed because of the currency, and not
because the builder's default amount happened to be something the boundary also refuses.
"""

from __future__ import annotations

import uuid
from decimal import Decimal
from typing import Any

from position_view.model import (
    AGGREGATE_INSTRUCTION,
    EVENT_RECEIVED,
    SUPPORTED_SCHEMA_VERSION,
    Bucket,
    Envelope,
    InstructionSnapshot,
    PositionKey,
    decode_envelope,
)


class _Omit:
    """The "this field is absent" marker. A class so its repr is readable in a failure message."""

    def __repr__(self) -> str:
        return "<OMIT>"


#: Passed as a field value to mean "leave this field out of the payload entirely".
#:
#: A sentinel is needed because `None` is not available for the job: the decoder treats a *missing*
#: field and an explicitly *null* field as two different failures on purpose (a serialiser that
#: omitted a key versus one that sent an empty column), so the tests must express both.
OMIT = _Omit()

#: A fixed instruction id rather than a fresh `uuid4()` per call.
#:
#: Randomness in a builder is a small trap: two events built by the same helper would be about two
#: different instructions, so a test that means "the same instruction, twice" silently becomes "two
#: unrelated instructions" and passes for the wrong reason. Tests that genuinely need a second
#: instruction pass `aggregate_id=` explicitly, which also documents that they need one.
INSTRUCTION_ID = uuid.UUID("11111111-1111-4111-8111-111111111111")
OTHER_INSTRUCTION_ID = uuid.UUID("22222222-2222-4222-8222-222222222222")


def instruction_id(index: int) -> uuid.UUID:
    """A deterministic instruction id from a small integer, for tests that need many of them.

    Derived from the index rather than generated, for the same reason `INSTRUCTION_ID` is a
    constant: the concurrency test drives fifty instructions at once and a failure has to name
    *which* one is short. `uuid4()` would give a fresh id per run, so the failure message would be
    unreproducible and the test would be impossible to bisect.

    The `4` and `8` are the version and variant nibbles, so the result is a well-formed v4 UUID and
    goes through `uuid.UUID(...)` at the boundary exactly like a real one.
    """
    return uuid.UUID(f"{index:08x}-0000-4000-8000-000000000000")


#: Timestamps are written as literals with an explicit offset, because "no timezone" is one of the
#: things the boundary refuses and a builder using `datetime.now()` could not test that.
OCCURRED_AT = "2026-03-01T09:30:00+00:00"
LATER = "2026-03-01T10:15:00+00:00"

#: Deliberately a value with real cents in it. A default of `"1500.00"` would let a bug that drops
#: the fractional part pass every test that did not specifically look for it.
AMOUNT = "1500.75"

COUNTERPARTY = "ACME-BANK"
CURRENCY = "USD"

KEY = PositionKey(counterparty=COUNTERPARTY, currency=CURRENCY)
OTHER_KEY = PositionKey(counterparty="BETA-TRUST", currency=CURRENCY)


def envelope_payload(**overrides: Any) -> dict[str, Any]:
    """A valid wire payload, with any field replaced by a keyword argument.

    Overrides for the three payload fields (`counterparty`, `currency`, `amount`) are lifted into
    `data` automatically, so a test does not have to know which level of the JSON a field lives at
    just to change it. `data` itself can still be overridden wholesale, which is how the tests for a
    missing or non-object `data` are written.
    """
    data_fields = {"counterparty", "currency", "amount"}
    data: dict[str, Any] = {
        "counterparty": COUNTERPARTY,
        "currency": CURRENCY,
        "amount": AMOUNT,
    }
    payload: dict[str, Any] = {
        "event_id": 1,
        "schema_version": SUPPORTED_SCHEMA_VERSION,
        "event_type": EVENT_RECEIVED,
        "aggregate_type": AGGREGATE_INSTRUCTION,
        "aggregate_id": str(INSTRUCTION_ID),
        "occurred_at": OCCURRED_AT,
        "data": data,
    }

    for name, value in overrides.items():
        if name in data_fields:
            data[name] = value
        else:
            payload[name] = value

    # Applied after the overrides, and over a materialised list because deleting from a dict while
    # iterating it raises.
    for name in [name for name, value in payload.items() if isinstance(value, _Omit)]:
        del payload[name]
    for name in [name for name, value in data.items() if isinstance(value, _Omit)]:
        del data[name]

    return payload


def make_envelope(**overrides: Any) -> Envelope:
    """A decoded `Envelope`, built by going through the real decoder.

    Constructing `Envelope(...)` directly would be faster and would let a test build an envelope the
    decoder could never produce — a lower-case currency, an un-stripped counterparty — and then
    assert that the projection handles it. Such a test would pass and mean nothing, because the
    projection is only ever handed values the boundary has already canonicalised.

    Going through `decode_envelope` makes every projection test implicitly assert that its input is
    reachable from the wire.
    """
    return decode_envelope(envelope_payload(**overrides))


def make_snapshot(
    *,
    key: PositionKey = KEY,
    amount: str = AMOUNT,
    bucket: Bucket = Bucket.PENDING,
    last_event_id: int = 1,
    instruction_id: uuid.UUID = INSTRUCTION_ID,
) -> InstructionSnapshot:
    """The projection's memory of one instruction. Keyword-only, because every field is significant.

    Positional arguments here would produce calls like `make_snapshot(KEY, "1500.75", PENDING, 1)`,
    and the strings-and-integers in the middle are exactly the kind of thing that gets silently
    transposed. The names cost a few characters and remove the class of bug entirely.
    """
    return InstructionSnapshot(
        instruction_id=instruction_id,
        key=key,
        amount=Decimal(amount),
        bucket=bucket,
        last_event_id=last_event_id,
    )
