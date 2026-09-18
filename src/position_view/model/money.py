"""Money parsing and money arithmetic, kept in one place because both are easy to get wrong.

WHY MONEY IS NOT A FLOAT, STATED ONCE SO IT NEVER HAS TO BE ARGUED AGAIN
---------------------------------------------------------------------------------
`0.1 + 0.2 == 0.30000000000000004`. That is not a curiosity, it is the reason a read model built on
floats will eventually report a position of `-0.000000000000002` for an account that is exactly
flat, and the reason a "is this position closed?" check written as `== 0` will be false forever.

A position in this service is a *running total that is added to and subtracted from thousands of
times*, which is the single worst case for binary floating point: every operation contributes error
in an unpredictable direction, and the errors do not cancel. `Decimal` is exact for the decimal
fractions money is actually denominated in, so a value added and then subtracted returns to exactly
the number it started from — which is the property the whole projection rests on, since a transition
between buckets is literally a subtraction followed by an addition.

WHY A `float` INPUT IS REFUSED RATHER THAN CONVERTED
---------------------------------------------------------------------------------
`Decimal(0.1)` is `0.1000000000000000055511151231257827021181583404541015625`. The conversion is
lossless and therefore faithfully preserves damage that was done before this function was called.
Accepting a float would mean the exactness of everything downstream depended on nobody upstream ever
serialising money as a JSON number — a guarantee no producer can make and no consumer can check.

So the accepted representations are a *string* (`"1500.00"`, the correct way to put money in JSON)
and an *integer* (`1500`, unambiguous because it has no fractional part to lose). A JSON float is a
refusal with a message that says what to send instead.
"""

from __future__ import annotations

from decimal import (
    Context,
    Decimal,
    DecimalException,
    DivisionByZero,
    Inexact,
    InvalidOperation,
    Overflow,
)
from typing import Any, Final

#: The exponent every amount is normalised to. Two decimal places covers the currencies this service
#: will realistically see, and normalising at the boundary means `"1500"`, `"1500.0"` and
#: `"1500.00"` become one value — so a position built from a mix of them does not depend on which
#: spelling happened to arrive.
#:
#: ⚠️ THIS IS A KNOWN SIMPLIFICATION AND IT IS RECORDED, NOT HIDDEN. JPY has zero minor units and
#:    BHD has three; a truly multi-currency ledger needs the exponent to be a property of the
#:    currency, which means a currency table, which means this module needs a dependency it does not
#:    have yet. See DECISIONS.md ADR-004. Normalising everything to two places is wrong for those
#:    currencies in the *presentation* sense (¥1500.00 is odd) but not in the arithmetic sense,
#:    because the trailing zeros are exact.
MONEY_EXPONENT: Final = Decimal("0.01")

#: An upper bound on a single amount, enforced at the boundary.
#:
#: The reason is not that a trillion-unit trade is impossible. It is that `Decimal` has no natural
#: size limit, so a producer bug that sends `"1e999"` produces a position total that is technically
#: correct, formats to a kilobyte of digits, and is indistinguishable in a dashboard from a real
#: number. A bound turns that into one refused event instead of a permanently poisoned position —
#: and a poisoned position cannot be un-poisoned without replaying the whole stream.
MAX_AMOUNT: Final = Decimal("1e15")

#: The arithmetic context `quantize` runs in, and why this module needs a constant for it at all.
#:
#: ⚠️ `Decimal.quantize` DOES NOT REFUSE TO ROUND BY DEFAULT — IT ROUNDS SILENTLY.
#:    `Decimal("1500.005").quantize(Decimal("0.01"))` returns `Decimal("1500.00")` and raises
#:    nothing, because the default context's rounding mode is ROUND_HALF_EVEN and `InvalidOperation`
#:    is only signalled when the *result* needs more digits than the context's precision. This was
#:    checked at a prompt rather than assumed, because assuming it is how a service ends up with a
#:    docstring promising exactness and code delivering half a cent of drift per event.
#:
#: Trapping `Inexact` is the mechanism that actually delivers the promise: the decimal spec signals
#: `Inexact` whenever a result differs from the exact mathematical result, so a value with real
#: precision beyond two places raises, while one with only trailing zeros (`"1500.0000"`) passes.
#: `Rounded` is deliberately NOT trapped, because it fires for discarded zeros too — trapping it
#: would refuse `"1500.0000"`, which is the same number spelled differently rather than a mismatch.
#:
#: The other three traps are the interpreter's own defaults, restated because `Context(traps=[...])`
#: replaces the trap set rather than adding to it: omitting them would make `InvalidOperation`
#: (e.g. an amount too large for the precision) return a NaN instead of raising.
_EXACT_CONTEXT: Final = Context(traps=[Inexact, InvalidOperation, DivisionByZero, Overflow])


class MoneyError(Exception):
    """A value that cannot be used as money.

    Raised by this module and re-wrapped at the event boundary (see `events.decode_envelope`). It is
    its own type so that the parsing rules can be tested directly, without constructing an envelope
    around every case.
    """


def parse_amount(value: Any) -> Decimal:
    """Turn an untrusted JSON value into an exact, bounded, normalised `Decimal`.

    The order of the checks matters and is not arbitrary:

      1. Reject `bool` before `int`, because `True` is an `int` in Python and `{"amount": true}` is
         a real serialiser bug that would otherwise become one unit of currency.
      2. Reject `float` before conversion, for the reason in the module docstring.
      3. Reject non-finite values before comparison, because `Decimal("NaN") > x` raises rather than
         returning False — so a NaN would escape a bounds check written the obvious way, and land in
         a position where it makes every subsequent total NaN forever.
      4. Only then check sign and magnitude.
    """
    if isinstance(value, bool):
        raise MoneyError("a boolean is not an amount")
    if isinstance(value, float):
        raise MoneyError(
            "an amount must not be a JSON number with a fractional part; "
            'send it as a string such as "1500.00" so no precision is lost in transit'
        )
    if not isinstance(value, str | int):
        raise MoneyError(f"an amount must be a string or an integer, got {type(value).__name__}")

    try:
        amount = Decimal(value)
    except DecimalException as exc:
        # `Decimal("")` and `Decimal("1,500.00")` both land here, as `InvalidOperation` — which is a
        # subclass of `DecimalException`, so catching the base class covers every malformed-literal
        # signal without listing them. The offending value goes in the message because "invalid
        # amount" without the value is an unactionable log line.
        raise MoneyError(f"not a decimal number: {value!r}") from exc

    # `is_finite()` covers NaN, sNaN, Infinity and -Infinity in one call. Note that `Decimal("NaN")`
    # constructs successfully — the string is valid decimal syntax — so this check cannot be folded
    # into the try block above.
    if not amount.is_finite():
        raise MoneyError(f"not a finite amount: {value!r}")

    if amount < 0:
        # A negative *instruction* amount is refused because direction in this domain is carried by
        # the event type (a reversal), not by the sign of the money. Allowing both would give two
        # encodings for one fact, and the projection would have to guess which one the producer
        # meant.
        raise MoneyError(f"an amount must not be negative: {value!r}")
    if amount > MAX_AMOUNT:
        raise MoneyError(f"amount exceeds the maximum this service accepts: {value!r}")

    return quantize(amount)


def quantize(amount: Decimal) -> Decimal:
    """Normalise to the money exponent, refusing to round away real precision.

    ⚠️ THIS DELIBERATELY DOES NOT ROUND, AND MAKING IT NOT ROUND TAKES EXPLICIT WORK.
       See `_EXACT_CONTEXT`: the default behaviour of `Decimal.quantize` is to round quietly, so the
       refusal comes from trapping `Inexact` rather than from anything `quantize` does by itself.

       Rounding here would mean `"1500.005"` silently became `"1500.00"` and the read model would
       disagree with the write model by half a cent per event, forever, invisibly. Money that does
       not fit the exponent is a contract mismatch between two services, and the only safe response
       is to say so loudly at the boundary.

       The place where rounding IS correct is presentation, and presentation is not this module's
       job.

    Normalising is still a *change* to the value's spelling: `"1500"` becomes `Decimal("1500.00")`.
    That is the point — two spellings of one amount must not produce two different serialisations of
    one position.
    """
    try:
        return amount.quantize(MONEY_EXPONENT, context=_EXACT_CONTEXT)
    except DecimalException as exc:
        raise MoneyError(
            f"amount {amount} has more precision than this service stores "
            f"(exponent {MONEY_EXPONENT}); rounding it here would silently "
            "disagree with the producer"
        ) from exc


#: The zero every position starts from, at the right exponent.
#:
#: Written as a constant because `Decimal(0)` has exponent 0, so `Decimal(0) + Decimal("1.00")` is
#: `Decimal("1.00")` but `str(Decimal(0))` is `"0"` rather than `"0.00"` — a difference that shows
#: up only in serialised output, only for positions that happen to be empty, and therefore only in
#: production.
ZERO: Final = Decimal("0.00")
