"""The wire shapes this service *returns*, and the parsing of what arrives in a path or a query.

⚠️ MONEY IS A JSON STRING, NEVER A JSON NUMBER, AND THAT IS THE MOST IMPORTANT LINE IN THIS FILE.
   A JSON number is a double in every mainstream parser. `JSON.parse('{"amount": 1500.75}')` in a
   browser gives an IEEE 754 binary float, and the client is now one addition away from
   `1500.7500000000001` — or, on a large book, away from losing a cent in a place nobody can trace.
   The whole point of `model/money.py` trapping `Inexact` is that this service refuses to be
   approximately right about money; serialising as a number would hand that guarantee away at the
   last possible moment, in the layer least likely to be reviewed for arithmetic.

   The cost is real and worth stating: every consumer has to parse the string, and a naive one will
   do `parseFloat` and be back where it started. That is a consumer bug this API cannot prevent —
   but a string at least makes it a *choice* they made, visible in their code, rather than something
   the transport did to them silently.

WHY PYDANTIC IS HERE AND NOT AT THE EVENT BOUNDARY
---------------------------------------------------------------------------------
These models describe *responses*, plus the small path and query values FastAPI is genuinely good at
validating. What they deliberately do not describe is an incoming event: `POST /events` hands its
body straight to `decode_envelope`, because that decoder is this service's actual wire contract, it
has 127 tests of its own, and its refusals carry reasons written for a human reading a log at 3am. A
pydantic model in front of it would be a second, subtly different definition of the same contract —
and the one that rejected first would be the one nobody had tested. See ADR-051.
"""

from __future__ import annotations

from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, Field

from position_view.model import Position, PositionKey, Totals

#: The currency rules the event decoder applies, restated here because the api layer has to
#: canonicalise a URL the same way the decoder canonicalised the event — see `parse_position_key`.
CURRENCY_LENGTH = 3


class TotalsPayload(BaseModel):
    """One bucket: how much, and how many instructions make it up.

    The count is not decoration and it is not derivable from the amount. One 4.2m trade and eight
    hundred small ones are the same exposure and completely different operational situations, and a
    read model that reported only the sum would make the difference invisible to the screen that
    exists to show it.
    """

    amount: str = Field(examples=["1500.75"])
    count: int = Field(examples=[3])

    @classmethod
    def of(cls, totals: Totals) -> TotalsPayload:
        # `:f` rather than `str()`: fixed-point formatting, so a value that ever ended up with a
        # large exponent renders as digits instead of as `1.5E+3`. `str(Decimal)` is exponential for
        # some values, and a client parsing `"1.5E+3"` as money is a client that will one day parse
        # it wrong.
        return cls(amount=f"{totals.amount:f}", count=totals.count)


class PositionPayload(BaseModel):
    """One counterparty's exposure in one currency, as a client sees it.

    The key is flattened into two top-level fields rather than nested as `{"key": {...}}`, because
    every consumer of this shape wants to put it in a table row, and a client that has to reach
    through one more level to label a row is a client that will render `[object Object]` at least
    once.
    """

    counterparty: str = Field(examples=["ACME-BANK"])
    currency: str = Field(examples=["USD"])

    #: The version is part of the contract, not an implementation detail leaking out. It is what
    #: makes a client's own de-duplication possible: SSE delivers at-least-once, and "ignore any
    #: version not greater than the one I hold" is the entire client-side rule. Hiding it would
    #: force every consumer to invent a worse test for the same question.
    version: int = Field(examples=[7])

    #: Event time — when the last event that changed this position *occurred* — not the time this
    #: response was built. A position from a stream that is an hour behind must look an hour old,
    #: because that is the fact an operator is trying to establish. `None` before anything has
    #: touched the position, which in practice only appears in a synthetic empty position.
    updated_at: str | None = Field(default=None, examples=["2026-03-01T09:30:00+00:00"])

    pending: TotalsPayload
    settled: TotalsPayload
    failed: TotalsPayload

    # The model field is `reversed_` — trailing underscore, to avoid shadowing the builtin in code
    # that reads positions all day. The wire name has no business inheriting that workaround, so it
    # is spelled `reversed` here, which is what a client would guess.
    reversed: TotalsPayload

    @classmethod
    def of(cls, position: Position) -> PositionPayload:
        return cls(
            counterparty=position.key.counterparty,
            currency=position.key.currency,
            version=position.version,
            updated_at=None if position.updated_at is None else position.updated_at.isoformat(),
            pending=TotalsPayload.of(position.pending),
            settled=TotalsPayload.of(position.settled),
            failed=TotalsPayload.of(position.failed),
            reversed=TotalsPayload.of(position.reversed_),
        )


class PositionListPayload(BaseModel):
    """Every currency this counterparty has exposure in.

    Wrapped in an object rather than returned as a bare JSON array. A bare array cannot grow: the
    day this needs a cursor, a total, or a "the projection was at event 900 when this was built"
    field, an object can gain one and an array is a breaking change for every consumer at once.
    """

    counterparty: str
    positions: list[PositionPayload]


class IngestedPayload(BaseModel):
    """What `POST /events` reports back: what happened, and how to read your own write.

    `checkpoint` is the field that makes the freshness parameter usable. A client that has just
    posted an event gets back the point the projection has reached, so it can pass that straight to
    `?await_event_id=` on its next read instead of guessing or polling.

    The event's own id is deliberately *not* echoed, even though the caller might expect it. The
    checkpoint is the more useful of the two and strictly stronger: the store clamps it to a
    high-water mark, so it is never lower than the event just applied, and waiting for it therefore
    guarantees this write is visible. Echoing the id as well would invite a client to wait for the
    weaker of the two conditions for no benefit — and to get it from the *outcome*, which for a skip
    does not carry one.
    """

    outcome: str = Field(examples=["applied"])

    #: Present only for a skip, and it is the `SkipReason` value verbatim. A skip is not an error —
    #: `duplicate` is at-least-once delivery working correctly — so this is reported with a 2xx and
    #: a reason rather than as a failure the caller should retry.
    reason: str | None = None

    checkpoint: int


class HealthPayload(BaseModel):
    """Deliberately about this process and nothing else — ADR-053 covers why there is no readiness
    probe alongside it."""

    status: str = Field(examples=["ok"])


def problem(
    status_code: int,
    error: str,
    message: str,
    *,
    headers: dict[str, str] | None = None,
    **extra: Any,
) -> HTTPException:
    """One error shape for the whole API: `{"detail": {"error": ..., "message": ..., ...}}`.

    FastAPI's default handler renders an `HTTPException`'s `detail` as the response body under a
    `detail` key, and passing a dict rather than a string is what keeps the envelope identical for
    every failure. That matters more than it looks: a client writing error handling against an API
    where one endpoint returns `{"detail": "not found"}` and another returns `{"detail": {...}}`
    will write `String(body.detail)` and print `[object Object]` to somebody's screen.

    `error` is a stable slug for code to branch on; `message` is prose for a human. Two fields
    because conflating them produces the worst of both — clients end up matching on substrings of a
    sentence, and then the sentence cannot be improved without breaking them.

    `headers` is separate from `**extra` so that a response header cannot be turned into a body
    field by a typo. The two are not interchangeable, and `retry-after` in the body would be read by
    nothing.
    """
    return HTTPException(
        status_code=status_code,
        detail={"error": error, "message": message, **extra},
        headers=headers,
    )


def parse_position_key(raw: str) -> PositionKey:
    """Parse `ACME-BANK/USD` into a `PositionKey`, applying exactly the decoder's canonicalisation.

    ⚠️ "EXACTLY THE DECODER'S" IS THE WHOLE REQUIREMENT, AND IT IS ASYMMETRIC ON PURPOSE.
       `decode_envelope` strips the counterparty and upper-cases the currency — it does *not*
       upper-case the counterparty. So this function must not either. An api layer that
       helpfully upper-cased both would 404 on every position belonging to a counterparty stored as
       `AcmeBank`, and the bug would present as "the data is missing" rather than as "the URL was
       transformed", which is a much longer afternoon.

    Used for the SSE `keys=` parameter, where a client sends the same string form the API prints.
    """
    # `rpartition` and not `partition`, because a counterparty may contain a slash and a currency
    # may not: a left-hand split would turn `NORTH/SOUTH FUND/USD` into counterparty `NORTH`.
    #
    # There is deliberately no separate "is there a separator at all?" check, which is worth saying
    # because its absence looks like an oversight. `rpartition` returns `("", "", raw)` when there
    # is nothing to find, so a key with no slash arrives with an empty counterparty and the guard
    # catches it, with the right message. An explicit `if not separator` alongside it would be a
    # condition no input can reach on its own — dead logic that reads as a safeguard, which is worse
    # than no safeguard. The mutation matrix is what surfaced it: deleting the clause changed no
    # observable behaviour, so no test could possibly have failed.
    counterparty, _, currency = raw.rpartition("/")
    if not counterparty.strip():
        raise problem(
            400,
            "malformed_key",
            f"'{raw}' is not a position key; the form is COUNTERPARTY/CURRENCY, e.g. ACME-BANK/USD",
        )
    if len(currency) != CURRENCY_LENGTH or not currency.isalpha():
        raise problem(
            400,
            "malformed_currency",
            f"'{currency}' is not a currency code; three letters expected, as in USD",
        )
    return PositionKey(counterparty=counterparty.strip(), currency=currency.upper())
