"""Tests for the money boundary: what is accepted as an amount, and what is refused and why.

WHAT THESE TESTS ARE REALLY PROTECTING
---------------------------------------------------------------------------------
Not "does `parse_amount` work". They are protecting a property of the whole service that cannot be
tested anywhere else: **an amount that enters this system is exact, and stays exact.** Every other
test in the repo assumes it. The projection's undo-then-redo arithmetic is only idempotent because
`x - x == 0` holds without qualification, which is true for `Decimal` and false for `float`.

So the interesting tests here are the refusals. `parse_amount("1500.00") == Decimal("1500.00")` is
worth one line; `parse_amount(1500.0)` raising is worth a paragraph, because a version of this
function that accepted it would pass every happy-path test in the suite and lose a fraction of a
cent per event in production.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from position_view.model import MAX_AMOUNT, MONEY_EXPONENT, ZERO, MoneyError, parse_amount, quantize


class TestAcceptedRepresentations:
    """The two things a producer may send, and what they normalise to."""

    @pytest.mark.parametrize(
        ("sent", "expected"),
        [
            # The canonical form: money as a JSON string, already at the stored exponent.
            ("1500.75", "1500.75"),
            # Fewer decimal places than stored. Normalised up, so `"1500"` and `"1500.00"` cannot
            # produce two different serialisations of the same position.
            ("1500", "1500.00"),
            ("1500.0", "1500.00"),
            # MORE decimal places, but only zeros. Accepted deliberately: `"1500.0000"` is the same
            # number as `"1500.00"`, spelled by a producer whose column happens to be NUMERIC(18,4).
            # Refusing it would break an integration over a difference that is not a difference.
            ("1500.0000", "1500.00"),
            # A JSON integer. Unambiguous — there is no fractional part to lose — so it is accepted
            # as a convenience for producers whose amounts are whole units.
            (1500, "1500.00"),
            (0, "0.00"),
            # Zero as a string, and the exponent it lands on. `Decimal("0")` would serialise as
            # `"0"`, which is why `ZERO` exists as a constant at all.
            ("0", "0.00"),
            ("0.00", "0.00"),
            # The exact upper bound is inclusive: the check is `>`, not `>=`, because a bound that
            # refuses the bound itself is a bound nobody can describe in a document.
            ("1e15", "1000000000000000.00"),
        ],
    )
    def test_normalises_to_two_places(self, sent: str | int, expected: str) -> None:
        assert parse_amount(sent) == Decimal(expected)

    def test_normalisation_is_about_spelling_not_value(self) -> None:
        """`"1500"` and `"1500.0000"` are equal *before* parsing; the point is the shared exponent.

        `Decimal("1500") == Decimal("1500.0000")` is already True, so an equality assertion alone
        would pass even if `quantize` did nothing at all. The exponent is the thing being tested,
        and `str()` is what exposes it — which is also exactly how the difference would reach a
        client, as two different strings in two JSON responses for the same position.
        """
        assert str(parse_amount("1500")) == str(parse_amount("1500.0000")) == "1500.00"

    def test_result_is_exact_under_addition_and_subtraction(self) -> None:
        """The property the whole projection rests on, asserted directly rather than assumed.

        A thousand additions followed by a thousand subtractions of the same amount must return to
        exactly zero — not "close to zero". This is the test that would fail if anyone ever changed
        the representation to `float`, and it is here rather than in the projection tests because
        this module is where the guarantee is made.
        """
        amount = parse_amount("0.07")
        total = ZERO
        for _ in range(1000):
            total += amount
        for _ in range(1000):
            total -= amount
        assert total == ZERO
        # Not `== 0` alone: a float implementation would fail the first assertion, but a *sloppy*
        # Decimal implementation could pass it while drifting the exponent, so the spelling is
        # checked too.
        assert str(total) == "0.00"


class TestRefusals:
    """Every value the boundary rejects, and the distinction each rejection preserves."""

    def test_float_is_refused_even_when_it_looks_exact(self) -> None:
        """`1500.0` is refused, and this is the single most counter-intuitive rule in the module.

        `Decimal(1500.0)` happens to be exactly `Decimal("1500")`, because 1500 is representable in
        binary. So this specific value would convert losslessly, and accepting it would be *fine* —
        until the day the producer sends `1500.10`, which is not representable, and by then the code
        path is established and the failure is a rounding error nobody can trace to a serialiser.

        The rule is therefore about the type, not the value: a JSON number with a fractional part is
        refused on principle, because a consumer cannot tell a safe float from an unsafe one after
        the fact.
        """
        with pytest.raises(MoneyError, match="must not be a JSON number"):
            parse_amount(1500.0)

    def test_float_message_says_what_to_send_instead(self) -> None:
        """The refusal names the fix, because the person reading it works on the *other* service.

        A message of "invalid amount" makes the producer's author guess. Naming the string form
        turns a cross-team debugging session into a one-line change. This is asserted because a
        helpful error message is a feature that silently rots the moment nothing depends on it.
        """
        with pytest.raises(MoneyError, match=r'string such as "1500\.00"'):
            parse_amount(0.1)

    def test_bool_is_refused_before_int(self) -> None:
        """`True` is an `int` in Python, so the order of the checks is what makes this work.

        `isinstance(True, int)` is True, so a `bool` reaches `Decimal(True)` and becomes
        `Decimal("1")` — one unit of currency, from a field that used to be a flag. The check for
        `bool` therefore has to come first, and this test fails if anyone reorders them.
        """
        with pytest.raises(MoneyError, match="boolean"):
            parse_amount(True)
        with pytest.raises(MoneyError, match="boolean"):
            parse_amount(False)

    @pytest.mark.parametrize(
        "sent",
        [
            None,
            [],
            {},
            ["1500.00"],
            {"amount": "1500.00"},
        ],
    )
    def test_wrong_type_is_refused_with_the_type_named(self, sent: object) -> None:
        """The type goes in the message. `Decimal(None)` raises `TypeError`, not a decimal signal,
        so without this check the failure would escape the module as an unclassified exception and
        be
        retried forever by a consumer that could not tell it was permanent."""
        with pytest.raises(MoneyError, match="must be a string or an integer"):
            parse_amount(sent)

    @pytest.mark.parametrize(
        "sent",
        [
            "",
            " ",
            "abc",
            # Thousands separators: a real producer bug, and one that a naive `float(value)` would
            # also reject — but with a `ValueError` rather than a classified refusal.
            "1,500.00",
            # A currency symbol attached to the number. The currency belongs in its own field.
            "$1500.00",
            "1500.00 USD",
            # A lone space, which is *not* the same case as `" 1500.00 "` below — see
            # `test_surrounding_whitespace_is_tolerated`.
            "\t",
            "\n",
        ],
    )
    def test_unparseable_string_is_refused_with_the_value_quoted(self, sent: str) -> None:
        with pytest.raises(MoneyError, match="not a decimal number"):
            parse_amount(sent)

    @pytest.mark.parametrize("sent", [" 1500.00 ", "\t1500.00\n", "1500.00 "])
    def test_surrounding_whitespace_is_tolerated(self, sent: str) -> None:
        """A padded amount is accepted; that is `Decimal`'s behaviour rather than a decision here.

        `Decimal(" 1500.00 ")` strips leading and trailing whitespace and parses normally. That was
        checked, not assumed — the first version of this test asserted a refusal and failed, which
        is the useful kind of failure: the comment above it had been describing a rule the code did
        not have.

        It is left as-is rather than tightened, for a reason worth stating: a padded numeric string
        is
        *unambiguous*. Nothing about the value is in doubt, so refusing it would reject a real event
        to enforce a formatting preference. Contrast `"1,500.00"`, which is refused — there the
        separator is a locale question, and a locale question about money is never cosmetic.
        """
        assert parse_amount(sent) == Decimal("1500.00")

    @pytest.mark.parametrize(
        "sent", ["NaN", "-NaN", "sNaN", "Infinity", "-Infinity", "inf", "-inf"]
    )
    def test_non_finite_is_refused_before_any_comparison(self, sent: str) -> None:
        """The check order here is not cosmetic — a NaN would defeat the bounds check.

        ⚠️ `Decimal("NaN") > Decimal("1e15")` does not return False. It raises `InvalidOperation`,
           because comparing against a NaN is a signalling operation in the decimal spec. So a
           bounds check written the obvious way and placed first would let a NaN through by
           *crashing on it in an unrelated line*, and a bounds check using `<` would let it through
           silently. Either way a NaN reaches a position total, and from then on every sum involving
           that position is NaN — permanently, because the arithmetic never recovers.

        These strings are all valid decimal literals, which is why the check cannot be folded into
        the `try` around `Decimal(value)`: construction succeeds.
        """
        with pytest.raises(MoneyError, match="not a finite amount"):
            parse_amount(sent)

    def test_nan_would_have_escaped_a_bounds_check(self) -> None:
        """The premise of the test above, asserted directly so the reasoning is not just a claim.

        If a future refactor removes the `is_finite()` check, this test still passes — it tests
        `Decimal`, not this module — but it documents *why* the check exists, in a form that
        survives somebody deciding the comment above is out of date.
        """
        from decimal import InvalidOperation

        with pytest.raises(InvalidOperation):
            _ = Decimal("NaN") > MAX_AMOUNT

    @pytest.mark.parametrize("sent", ["-1", "-0.01", "-1500.75", -1, "-1e15"])
    def test_negative_is_refused(self, sent: str | int) -> None:
        """Direction is carried by the event type, not by the sign of the money.

        A `reversed` event means money came back; a negative amount on a `received` event would mean
        the same thing in a second, undocumented encoding. Two encodings for one fact means the
        projection has to guess which the producer meant, and it will guess wrong for one of them.
        """
        with pytest.raises(MoneyError, match="must not be negative"):
            parse_amount(sent)

    def test_negative_zero_is_accepted_and_normalised(self) -> None:
        """`"-0.00"` is not negative, and the check must not accidentally refuse it.

        `Decimal("-0.00") < 0` is False — negative zero compares equal to zero — so this value falls
        through to `quantize`. It is asserted because the obvious "reject anything starting with a
        minus sign" implementation would refuse it, and a producer emitting `-0.00` for a zero-value
        instruction is doing nothing wrong.
        """
        assert parse_amount("-0.00") == ZERO

    @pytest.mark.parametrize("sent", ["1e16", "1000000000000000.01", "1e999", "9" * 40])
    def test_above_the_bound_is_refused(self, sent: str) -> None:
        """The bound is not about plausibility, it is about containment.

        `"1e999"` is a perfectly good `Decimal`. It is also a value that, once added to a position,
        makes that position's total a kilobyte of digits that no dashboard can render and no human
        can distinguish from a real number — and it cannot be removed without replaying the whole
        stream. One refused event is cheaper than one permanently poisoned position.
        """
        with pytest.raises(MoneyError, match="exceeds the maximum"):
            parse_amount(sent)

    def test_excess_precision_is_refused_rather_than_rounded(self) -> None:
        """Half a cent, refused loudly instead of absorbed quietly. See `quantize`.

        This is the case that would be easiest to "fix" by adding a rounding mode, and doing so
        would make the read model disagree with the write model by a fraction of a cent per event,
        forever, with no error anywhere. The refusal says out loud that two services disagree about
        the shape of money, which is a conversation, not a rounding decision.
        """
        with pytest.raises(MoneyError, match="more precision than this service stores"):
            parse_amount("1500.005")

    def test_refusal_message_names_the_exponent(self) -> None:
        """So the producer's author learns the actual rule, not just that they broke one."""
        with pytest.raises(MoneyError, match=r"exponent 0\.01"):
            parse_amount("0.001")


class TestQuantize:
    """`quantize` on its own, because it is also called on values that never came off the wire."""

    def test_default_quantize_would_have_rounded_silently(self) -> None:
        """⚠️ THE FACT THAT MAKES THIS MODULE NECESSARY, PINNED DOWN AS AN EXECUTABLE ASSERTION.

        `Decimal.quantize` does **not** refuse to round by default. It applies the context's
        rounding mode — ROUND_HALF_EVEN — and returns a rounded result with no signal at all. So the
        exactness this service promises comes entirely from trapping `Inexact`, and a refactor that
        "simplifies" `quantize` back to `amount.quantize(MONEY_EXPONENT)` would compile, pass every
        happy-path test, and silently reintroduce the drift.

        This test asserts the *standard library's* behaviour rather than ours, which is unusual and
        deliberate: it is the premise of our design, it was checked rather than assumed, and if a
        future Python changes it this test is where that shows up.
        """
        assert Decimal("1500.005").quantize(MONEY_EXPONENT) == Decimal("1500.00")

    def test_ours_refuses_what_the_default_would_have_rounded(self) -> None:
        """The same input, through this module, is a refusal. That difference is the feature."""
        with pytest.raises(MoneyError):
            quantize(Decimal("1500.005"))

    @pytest.mark.parametrize(
        ("sent", "expected"),
        [
            ("1500", "1500.00"),
            ("1500.7", "1500.70"),
            ("1500.75", "1500.75"),
            # Trailing zeros beyond the exponent are discarded without complaint: `Rounded` is
            # signalled but not trapped, because discarding a zero does not change the value.
            ("1500.750000", "1500.75"),
            ("0.000", "0.00"),
        ],
    )
    def test_exact_values_pass_through(self, sent: str, expected: str) -> None:
        assert str(quantize(Decimal(sent))) == expected

    def test_is_idempotent(self) -> None:
        """Quantising twice must equal quantising once, including the exponent.

        Not a theoretical concern: the projector re-quantises totals after arithmetic, so a
        `quantize` that shifted the exponent on a second application would make a position's
        serialisation depend on how many times it had been written.
        """
        once = quantize(Decimal("1500.7"))
        assert str(quantize(once)) == str(once) == "1500.70"

    def test_refuses_a_value_too_large_for_the_context_precision(self) -> None:
        """A magnitude past the arithmetic context's precision raises rather than returning a NaN.

        This is why the three interpreter-default traps are restated in `_EXACT_CONTEXT`:
        `Context(traps=[...])` *replaces* the trap set, so listing only `Inexact` would leave
        `InvalidOperation` untrapped, and an over-large value would come back as `Decimal("NaN")` —
        into a position total, silently. `parse_amount` bounds the value before it gets here, but
        `quantize` is public and must not have a NaN-returning path of its own.
        """
        with pytest.raises(MoneyError):
            quantize(Decimal("1e400"))


def test_zero_is_at_the_stored_exponent() -> None:
    """`ZERO` exists as a constant for one reason: `Decimal(0)` serialises as `"0"`, not `"0.00"`.

    An empty position built from `Decimal(0)` would render `"0"` while a position that had money and
    returned to nothing would render `"0.00"` — the same state, two spellings, in a JSON response a
    client may be diffing. The arithmetic is identical either way, which is exactly why this is the
    kind of bug that reaches production.
    """
    assert str(ZERO) == "0.00"
    assert str(Decimal(0)) == "0"
    assert ZERO == Decimal(0)


def test_the_bound_and_the_exponent_are_the_documented_values() -> None:
    """Pins the two constants other services are told about, so a change to either is deliberate.

    Both appear in the README and in an ADR. A test that fails when they change is how a documented
    number stops being a comment that used to be true.
    """
    assert MONEY_EXPONENT == Decimal("0.01")
    assert MAX_AMOUNT == Decimal("1e15")
