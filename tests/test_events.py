"""Tests for the event boundary: exactly which messages become an `Envelope`, and which are refused.

WHAT A BOUNDARY TEST IS FOR
---------------------------------------------------------------------------------
The decoder is the only place in this service where untrusted input becomes a typed value. Every
other module is written on the assumption that it never sees a lower-case currency, a naive
timestamp, a float amount or a missing field — and that assumption is only true because of what is
asserted in this file. These tests are the contract the rest of the codebase is allowed to rely on.

THE MOST IMPORTANT DISTINCTION IN HERE: REFUSED versus IGNORED
A malformed message is refused (`MalformedEventError`, permanent, do not retry). An *irrelevant*
message — an unknown event type, another aggregate's event, a field this service does not read — is
decoded successfully and dropped later by the projection. Collapsing those two would mean either a
producer bug looked like routine filtering, or 90% of a shared topic looked like a producer bug.
Both mistakes are silent in one direction and deafening in the other, which is why several tests
below exist only to assert that something is NOT rejected.
"""

from __future__ import annotations

import dataclasses
import uuid
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from builders import AMOUNT, INSTRUCTION_ID, OCCURRED_AT, OMIT, envelope_payload
from position_view.model import (
    AGGREGATE_INSTRUCTION,
    EVENT_RECEIVED,
    SUPPORTED_SCHEMA_VERSION,
    Envelope,
    MalformedEventError,
    MoneyError,
    decode_envelope,
)


class TestHappyPath:
    """A valid message, field by field: a decoder that reads the wrong field still 'works'."""

    def test_decodes_every_field_it_claims_to_read(self) -> None:
        envelope = decode_envelope(envelope_payload())

        assert envelope.event_id == 1
        assert envelope.event_type == EVENT_RECEIVED
        assert envelope.aggregate_type == AGGREGATE_INSTRUCTION
        assert envelope.aggregate_id == INSTRUCTION_ID
        assert envelope.occurred_at == datetime(2026, 3, 1, 9, 30, tzinfo=UTC)
        assert envelope.counterparty == "ACME-BANK"
        assert envelope.currency == "USD"
        assert envelope.amount == Decimal("1500.75")

    def test_amount_is_a_decimal_and_not_a_float(self) -> None:
        """An `isinstance` assertion, which is usually a smell and is the point here.

        `envelope.amount == Decimal("1500.75")` would pass for a float too — `1500.75 ==
        Decimal("1500.75")` is True in Python. So the equality check alone cannot detect the one
        mistake that matters most, and the type has to be asserted directly.
        """
        assert isinstance(decode_envelope(envelope_payload()).amount, Decimal)

    def test_the_envelope_is_frozen(self) -> None:
        """An event is history, so mutating it is always a mistake — and it fails at the mistake.

        Without the freeze, the plausible bug is a handler "fixing up" an envelope in place
        (rounding an amount, defaulting a counterparty) and a projection built from values that were
        never on the wire. The projection would then disagree with the log it was supposedly derived
        from, and a replay would produce a different answer — the one thing a read model must never
        do.
        """
        envelope = decode_envelope(envelope_payload())
        with pytest.raises(dataclasses.FrozenInstanceError):
            envelope.amount = Decimal("1.00")  # type: ignore[misc]

    def test_two_identical_payloads_decode_to_equal_envelopes(self) -> None:
        """Dataclass equality, relied on by the tests and by the projector's duplicate detection."""
        assert decode_envelope(envelope_payload()) == decode_envelope(envelope_payload())


class TestCanonicalisation:
    """What the boundary *changes*, so that nothing downstream has to remember to."""

    @pytest.mark.parametrize("sent", ["usd", "Usd", "uSd", "USD"])
    def test_currency_is_upper_cased(self, sent: str) -> None:
        """Currency is half of the position key, so its case cannot be left to the producer.

        `"usd"` and `"USD"` reaching the projection as two strings means two positions for one real
        exposure, each holding half the money, neither reconcilable against the write model — and
        nothing anywhere raises. Folding case at the boundary makes the split unrepresentable rather
        than merely unlikely.
        """
        assert decode_envelope(envelope_payload(currency=sent)).currency == "USD"

    @pytest.mark.parametrize("sent", [" ACME-BANK", "ACME-BANK ", "\tACME-BANK\n", "  ACME-BANK  "])
    def test_counterparty_is_stripped(self, sent: str) -> None:
        """Same argument as currency: the other half of the key, padded by a fixed-width source."""
        assert decode_envelope(envelope_payload(counterparty=sent)).counterparty == "ACME-BANK"

    def test_counterparty_case_is_NOT_folded(self) -> None:
        """And here is the limit of canonicalisation, asserted so the asymmetry is deliberate.

        Currency has 180-odd legal values defined by a standard, so upper-casing is lossless. A
        counterparty identifier is whatever the upstream system calls it, and `"AcmeBank"` may be a
        different entity from `"ACMEBANK"` in a system this service does not own. Folding case would
        be *merging two positions*, which is a far worse failure than splitting one.

        The honest version of this rule: canonicalise where the value space is defined elsewhere and
        known; leave it alone where it is somebody else's identifier.
        """
        assert decode_envelope(envelope_payload(counterparty="AcmeBank")).counterparty == "AcmeBank"

    def test_timestamp_is_normalised_to_utc_without_changing_the_instant(self) -> None:
        """An offset is honoured and converted — the instant survives, the representation does not.

        Storing the original offset would be more faithful and would make every later comparison a
        place where somebody could forget that two aware datetimes in different zones sort correctly
        but do not *format* comparably. One zone everywhere removes the question.
        """
        envelope = decode_envelope(envelope_payload(occurred_at="2026-03-01T14:30:00+05:00"))
        assert envelope.occurred_at == datetime(2026, 3, 1, 9, 30, tzinfo=UTC)
        assert envelope.occurred_at.tzinfo is UTC

    def test_z_suffix_is_accepted(self) -> None:
        """`Z` is what almost every producer actually emits, and 3.11+ parses it natively.

        Worth a test rather than a comment because this is the single most common timestamp form on
        the wire, and it is also the form that a hand-rolled parser or an older Python would reject.
        """
        assert decode_envelope(
            envelope_payload(occurred_at="2026-03-01T09:30:00Z")
        ).occurred_at == (datetime(2026, 3, 1, 9, 30, tzinfo=UTC))

    def test_a_non_utc_offset_is_preserved_as_an_instant_not_a_wall_clock(self) -> None:
        """The same wall-clock time in two zones must decode to two different instants.

        This is the assertion that fails if anyone ever "simplifies" the conversion to
        `parsed.replace(tzinfo=UTC)`, which keeps the digits and changes the moment — a bug that
        shifts lag measurements by hours and looks like a slow consumer.
        """
        utc = decode_envelope(envelope_payload(occurred_at="2026-03-01T09:30:00+00:00"))
        tokyo = decode_envelope(envelope_payload(occurred_at="2026-03-01T09:30:00+09:00"))
        assert tokyo.occurred_at == utc.occurred_at - timedelta(hours=9)


class TestForwardCompatibility:
    """What the boundary deliberately does NOT refuse."""

    def test_unknown_top_level_fields_are_ignored(self) -> None:
        """A producer adding a field must not be able to break a consumer that does not want it.

        This is the whole reason the decoder names what it reads instead of validating the document
        as a closed schema. A strict schema would turn every additive producer change into a
        coordinated release across every consumer, which is precisely the coupling the event stream
        exists to avoid.
        """
        envelope = decode_envelope(
            envelope_payload(trace_id="abc-123", producer_version="2.7.1", tags=["urgent"])
        )
        assert envelope.event_id == 1

    def test_unknown_data_fields_are_ignored(self) -> None:
        """The same rule one level down, where most new fields actually appear."""
        payload = envelope_payload()
        payload["data"]["settlement_date"] = "2026-03-03"
        payload["data"]["desk"] = "RATES"
        assert decode_envelope(payload).amount == Decimal(AMOUNT)

    def test_an_unknown_event_type_decodes_successfully(self) -> None:
        """⚠️ NOT A REFUSAL. An event type this view ignores is normal traffic, not a bad message.

        On a shared stream most events are somebody else's. Refusing them here would classify
        routine traffic as a permanent failure, and any sane consumer reacts to permanent failures
        by alerting or dead-lettering — so the service would page a human for working correctly.

        The filtering happens in `project`, which returns a counted skip. That is a different
        outcome with a different reaction, and keeping them apart is what makes the counters
        readable.
        """
        envelope = decode_envelope(envelope_payload(event_type="instruction.archived"))
        assert envelope.event_type == "instruction.archived"

    def test_another_aggregates_event_decodes_successfully(self) -> None:
        """Same argument for the aggregate type: `project` skips it, the decoder does not."""
        envelope = decode_envelope(envelope_payload(aggregate_type="payment"))
        assert envelope.aggregate_type == "payment"

    @pytest.mark.parametrize(
        "sent",
        [
            "11111111-1111-4111-8111-111111111111",
            # No dashes, and braced, and the URN form: all accepted by `uuid.UUID`, all the same id.
            "11111111111141118111111111111111",
            "{11111111-1111-4111-8111-111111111111}",
            "urn:uuid:11111111-1111-4111-8111-111111111111",
            # Upper case hex. A UUID is a 128-bit number, not a string, so case cannot matter.
            "11111111-1111-4111-8111-111111111111".upper(),
        ],
    )
    def test_every_spelling_of_the_same_uuid_decodes_to_the_same_id(self, sent: str) -> None:
        """Parsing into `uuid.UUID` rather than keeping the string is what makes this true.

        If `aggregate_id` were stored as text, these five payloads would be five different
        instructions — five snapshots, five contributions to a position, from one real instruction.
        The type is doing real work here, not decorating a string.
        """
        assert decode_envelope(envelope_payload(aggregate_id=sent)).aggregate_id == INSTRUCTION_ID


class TestEventId:
    """The dedup key and ordering token, and therefore the field with the strictest rules."""

    def test_missing_is_refused_by_name(self) -> None:
        with pytest.raises(MalformedEventError, match="missing required field 'event_id'"):
            decode_envelope(envelope_payload(event_id=OMIT))

    def test_null_is_a_different_message_from_missing(self) -> None:
        """Two failures, two messages, because they point at two different upstream bugs.

        An absent key is usually a version mismatch — a producer that does not know the field
        exists. An explicit `null` is a producer that meant to send it and had nothing to send,
        which is usually a nullable column. The investigations are different, so the log lines are
        different.
        """
        with pytest.raises(MalformedEventError, match="field 'event_id' is null"):
            decode_envelope(envelope_payload(event_id=None))

    @pytest.mark.parametrize("sent", ["1", "abc", 1.0, 12.9, [1], {"id": 1}])
    def test_non_integer_is_refused_with_the_type_named(self, sent: Any) -> None:
        """`int(payload["event_id"])` would have accepted three of these, and that is the argument.

        `"12"` becomes 12, `12.9` becomes 12 — and the second one is the dangerous case, because two
        distinct events (`12.9` and `12.1`) would collapse onto one deduplication key. One of them
        would then be silently dropped as a duplicate, and the position would be short by its amount
        with no error anywhere in the system.
        """
        with pytest.raises(MalformedEventError, match="event_id must be an integer"):
            decode_envelope(envelope_payload(event_id=sent))

    def test_bool_is_refused_although_it_is_an_int(self) -> None:
        """`isinstance(True, int)` is True in Python, so `bool` needs its own clause.

        `{"event_id": true}` is a real serialiser bug — a field that used to be a flag — and letting
        it through means every such event deduplicates against event id 1. The first one applies and
        every subsequent one is discarded as a duplicate.
        """
        with pytest.raises(MalformedEventError, match="event_id must be an integer, got bool"):
            decode_envelope(envelope_payload(event_id=True))

    @pytest.mark.parametrize("sent", [0, -1, -999])
    def test_zero_and_negatives_are_refused(self, sent: int) -> None:
        """0 is reserved as the projector's "nothing applied yet" sentinel.

        A real event with id 0 would be indistinguishable from an empty projection, so the very
        first event of a rebuild would compare equal to the checkpoint and be skipped as a duplicate
        — one missing instruction, at the start of every replay, forever.
        """
        with pytest.raises(MalformedEventError, match="event_id must be positive"):
            decode_envelope(envelope_payload(event_id=sent))

    def test_one_is_accepted_because_the_bound_is_inclusive(self) -> None:
        assert decode_envelope(envelope_payload(event_id=1)).event_id == 1

    def test_a_very_large_id_is_accepted(self) -> None:
        """Python ints are unbounded, and an outbox sequence on a busy stream gets large.

        Asserted so that nobody adds an upper bound by analogy with `MAX_AMOUNT`. The analogy does
        not hold: a huge amount poisons a total, while a huge event id is just a number that got
        counted to.
        """
        assert decode_envelope(envelope_payload(event_id=2**63)).event_id == 2**63


class TestSchemaVersion:
    """The one field whose entire purpose is to make a breaking change loud."""

    def test_missing_is_refused(self) -> None:
        with pytest.raises(MalformedEventError, match="missing required field 'schema_version'"):
            decode_envelope(envelope_payload(schema_version=OMIT))

    @pytest.mark.parametrize("sent", [0, 2, 99, "1", None])
    def test_anything_other_than_the_supported_version_is_refused(self, sent: Any) -> None:
        """Including a *higher* version, which looks over-cautious and is not.

        Surely v2 is v1 plus fields? Sometimes. And sometimes v2 is where `amount` became a
        minor-unit integer, in which case optimistically reading it as a decimal turns 1500 dollars
        into 15 dollars in a report somebody trades on. The version field exists precisely so that
        this is a decision rather than an accident, and the honest default for money is to stop.
        """
        with pytest.raises(MalformedEventError, match="schema_version"):
            decode_envelope(envelope_payload(schema_version=sent))

    @pytest.mark.parametrize("sent", [1.0, True])
    def test_a_numerically_equal_non_integer_is_refused(self, sent: Any) -> None:
        """⚠️ `1 == 1.0` AND `1 == True` ARE BOTH TRUE IN PYTHON, SO `!=` ALONE IS NOT ENOUGH.

        This test was written expecting a refusal and initially failed, because the check really was
        just `schema_version != SUPPORTED_SCHEMA_VERSION` — so `{"schema_version": true}` decoded as
        a supported message. The type check exists because of this test, which is the only reason
        the case is worth keeping: it is not a hypothetical, it caught the code.

        Why refuse rather than shrug? The value is numerically right, so nothing would go wrong
        *today*. But this field is the one place the service says "I have been verified against this
        producer", and a serialiser emitting `true` for an integer version has not been examined by
        anybody. Accepting it means the next real incompatibility arrives through a path already
        established as trusted.
        """
        with pytest.raises(MalformedEventError, match="schema_version must be an integer"):
            decode_envelope(envelope_payload(schema_version=sent))

    def test_the_supported_version_is_the_documented_one(self) -> None:
        assert SUPPORTED_SCHEMA_VERSION == 1


class TestRequiredFieldTypes:
    """The remaining scalar fields. Mostly mechanical, and each one guards a specific misread."""

    @pytest.mark.parametrize("sent", [1, 1.5, None, [], {}, True])
    def test_event_type_must_be_a_string(self, sent: Any) -> None:
        with pytest.raises(MalformedEventError, match=r"event_type|is null"):
            decode_envelope(envelope_payload(event_type=sent))

    @pytest.mark.parametrize("sent", [1, None, ["instruction"]])
    def test_aggregate_type_must_be_a_string(self, sent: Any) -> None:
        with pytest.raises(MalformedEventError, match=r"aggregate_type|is null"):
            decode_envelope(envelope_payload(aggregate_type=sent))

    @pytest.mark.parametrize(
        "sent", ["not-a-uuid", "", "1111", "11111111-1111-4111-8111-11111111111"]
    )
    def test_malformed_uuid_is_refused_with_the_value_quoted(self, sent: str) -> None:
        with pytest.raises(MalformedEventError, match="aggregate_id is not a UUID"):
            decode_envelope(envelope_payload(aggregate_id=sent))

    @pytest.mark.parametrize("sent", [1, 1.5, [], {}])
    def test_non_string_uuid_is_refused_before_parsing(self, sent: Any) -> None:
        """A separate message from a malformed string, because `uuid.UUID(1)` raises `TypeError`.

        Left unguarded, an integer `aggregate_id` would escape this module as a `TypeError` rather
        than as a classified permanent failure — so a consumer could not tell it should stop
        retrying.
        """
        with pytest.raises(MalformedEventError, match="must be a string containing a UUID"):
            decode_envelope(envelope_payload(aggregate_id=sent))

    def test_the_uuid_parse_error_is_chained(self) -> None:
        """`from exc` is asserted, because the discarded message is the actionable half.

        `uuid.UUID`'s own error says *what* was wrong with the string — bad length, bad hex digit.
        Dropping it leaves an operator with "not a UUID" and a value, and no idea which of the two
        plausible producer bugs they are looking at.
        """
        with pytest.raises(MalformedEventError) as caught:
            decode_envelope(envelope_payload(aggregate_id="nope"))
        assert isinstance(caught.value.__cause__, ValueError)


class TestTimestamp:
    """The field where "close enough" produces numbers that are wrong in an invisible way."""

    def test_missing_is_refused(self) -> None:
        with pytest.raises(MalformedEventError, match="missing required field 'occurred_at'"):
            decode_envelope(envelope_payload(occurred_at=OMIT))

    @pytest.mark.parametrize("sent", [1740000000, 1740000000.0, [], {}])
    def test_a_non_string_is_refused_rather_than_treated_as_an_epoch(self, sent: Any) -> None:
        """An epoch integer is a plausible thing for a producer to send and is not this contract.

        Accepting it would mean guessing seconds versus milliseconds, and guessing wrong by a factor
        of a thousand puts every event in 1970 or in the year 57000 — which shows up as a lag metric
        so absurd that people assume the metric is broken rather than the parse.
        """
        with pytest.raises(MalformedEventError, match="occurred_at must be a string"):
            decode_envelope(envelope_payload(occurred_at=sent))

    @pytest.mark.parametrize(
        "sent",
        ["yesterday", "2026-13-01T00:00:00Z", "01/03/2026", "2026-03-01 09:30:00 UTC", ""],
    )
    def test_unparseable_is_refused_with_the_value_quoted(self, sent: str) -> None:
        with pytest.raises(MalformedEventError, match="not an ISO-8601 timestamp"):
            decode_envelope(envelope_payload(occurred_at=sent))

    @pytest.mark.parametrize(
        "sent", ["2026-03-01T09:30:00", "2026-03-01T09:30:00.123456", "2026-03-01"]
    )
    def test_naive_is_refused_rather_than_assumed_to_be_utc(self, sent: str) -> None:
        """⚠️ THE TEMPTING DEFAULT, REFUSED ON PURPOSE.

        Assuming UTC is one line of code and it is how a lag measurement ends up permanently off by
        the difference between the producer's local zone and this process's. That error looks
        plausible (a few hours), changes twice a year with daylight saving, and is never questioned
        because nobody has a reason to suspect the parse. A producer that omits the offset has a bug
        worth fixing at the producer, and the refusal is what makes them fix it.

        Note that `"2026-03-01"` — a bare date — is refused by this rule and not by the format rule:
        `fromisoformat` parses it happily into midnight, naive.
        """
        with pytest.raises(MalformedEventError, match="no timezone offset"):
            decode_envelope(envelope_payload(occurred_at=sent))

    def test_the_naive_refusal_explains_why(self) -> None:
        """The message names the consequence, not just the rule, because the reader is elsewhere."""
        with pytest.raises(MalformedEventError, match="cannot be compared against this service"):
            decode_envelope(envelope_payload(occurred_at="2026-03-01T09:30:00"))

    def test_a_fractional_second_with_an_offset_is_accepted(self) -> None:
        """Microseconds are kept. They are not used for ordering — `event_id` is — but discarding
        them would make two events on the same instruction look simultaneous in a log."""
        envelope = decode_envelope(envelope_payload(occurred_at="2026-03-01T09:30:00.123456+00:00"))
        assert envelope.occurred_at.microsecond == 123456

    def test_a_non_utc_zone_that_is_not_a_whole_hour_is_handled(self) -> None:
        """India is +05:30, Nepal is +05:45. Offsets are not all whole hours, and an implementation
        that multiplied hours would be wrong for a large fraction of the world."""
        envelope = decode_envelope(envelope_payload(occurred_at="2026-03-01T15:15:00+05:45"))
        assert envelope.occurred_at == datetime(2026, 3, 1, 9, 30, tzinfo=UTC)

    def test_an_already_utc_aware_value_is_unchanged(self) -> None:
        expected = datetime(2026, 3, 1, 9, 30, tzinfo=timezone(timedelta(0)))
        assert decode_envelope(envelope_payload()).occurred_at == expected


class TestDataObject:
    """`data` and the three fields inside it that decide which position an event belongs to."""

    @pytest.mark.parametrize("sent", [OMIT, None, "counterparty=ACME", 1, [], ["ACME"]])
    def test_data_must_be_an_object(self, sent: Any) -> None:
        """Missing and null are *not* distinguished here, unlike the scalar fields, and that is a
        deliberate inconsistency: a payload with no `data` at all is not a version mismatch in one
        field, it is not this contract at all. One message covers every shape of that failure."""
        with pytest.raises(MalformedEventError, match="data must be an object"):
            decode_envelope(envelope_payload(data=sent))

    def test_missing_counterparty_is_refused(self) -> None:
        with pytest.raises(MalformedEventError, match="missing required field 'counterparty'"):
            decode_envelope(envelope_payload(counterparty=OMIT))

    @pytest.mark.parametrize("sent", ["", " ", "\t\n", "   "])
    def test_blank_counterparty_is_refused(self, sent: str) -> None:
        """A blank counterparty would become a real position with a real total and no owner.

        And it would *aggregate*: every event with a blank counterparty joins the same position, so
        a producer bug affecting many instructions produces one large, plausible-looking,
        meaningless row that somebody eventually has to explain.
        """
        with pytest.raises(MalformedEventError, match="counterparty must be a non-empty string"):
            decode_envelope(envelope_payload(counterparty=sent))

    @pytest.mark.parametrize("sent", [1, [], {}, True])
    def test_non_string_counterparty_is_refused(self, sent: Any) -> None:
        with pytest.raises(MalformedEventError, match="counterparty must be a non-empty string"):
            decode_envelope(envelope_payload(counterparty=sent))

    @pytest.mark.parametrize("sent", ["US", "USDX", "U", "", "US1", "1234", "$$$", "US ", " USD"])
    def test_currency_must_be_three_letters(self, sent: str) -> None:
        """ISO 4217 is three alphabetic characters, and this check is about the position key.

        `"usd"`, `"US Dollar"` and `"USD "` would otherwise become three separate positions holding
        three fractions of one exposure, and no reconciliation against the write model would ever
        line up. Note that `" USD"` is refused rather than stripped — unlike the counterparty. A
        padded currency code means the producer is emitting a fixed-width field into a place where
        the value space is a known three characters, and that is worth a refusal rather than a
        silent repair.
        """
        with pytest.raises(MalformedEventError, match="currency must be a 3-letter code"):
            decode_envelope(envelope_payload(currency=sent))

    @pytest.mark.parametrize("sent", [1, [], {}, True])
    def test_non_string_currency_is_refused(self, sent: Any) -> None:
        with pytest.raises(MalformedEventError, match="currency must be a 3-letter code"):
            decode_envelope(envelope_payload(currency=sent))

    def test_missing_amount_is_refused(self) -> None:
        with pytest.raises(MalformedEventError, match="missing required field 'amount'"):
            decode_envelope(envelope_payload(amount=OMIT))

    @pytest.mark.parametrize("sent", [1500.75, "abc", "-1", "1e999", "1500.005", True, None, []])
    def test_an_unusable_amount_is_refused_as_a_malformed_event(self, sent: Any) -> None:
        """Every money rule reaches the wire as one kind of failure. That re-wrap is the point.

        `MoneyError` is `money.py`'s vocabulary; `MalformedEventError` is the boundary's. A caller
        forced to catch both will eventually catch only one, and the one it forgets will be the one
        that escapes as an unclassified exception and gets retried forever.
        """
        with pytest.raises(MalformedEventError, match=r"amount is not usable as money|is null"):
            decode_envelope(envelope_payload(amount=sent))

    def test_the_money_error_is_chained_and_its_message_is_kept(self) -> None:
        """Re-wrapped, not swallowed: the reason survives in both the message and `__cause__`.

        Asserting `__cause__` matters because `raise X from Y` and `raise X` are one word apart and
        produce identical behaviour until somebody needs the traceback.
        """
        with pytest.raises(MalformedEventError, match="more precision") as caught:
            decode_envelope(envelope_payload(amount="1500.005"))
        assert isinstance(caught.value.__cause__, MoneyError)


class TestErrorType:
    """The exception type itself, since a consumer routes on it."""

    def test_malformed_is_not_a_value_error(self) -> None:
        """⚠️ A DELIBERATELY NARROW TYPE, ASSERTED SO IT STAYS NARROW.

        If `MalformedEventError` subclassed `ValueError`, every `except ValueError` anywhere in a
        consumer — including ones written for entirely unrelated reasons — would swallow a poison
        message and turn a classified permanent failure into a silent skip. Inheriting from
        `Exception` keeps the routing decision explicit at the one place that should be making it.
        """
        assert issubclass(MalformedEventError, Exception)
        assert not issubclass(MalformedEventError, ValueError)

    def test_money_error_and_malformed_event_error_are_unrelated_types(self) -> None:
        """Neither is a subclass of the other, so `except MoneyError` at the consumer cannot
        accidentally catch a boundary failure and vice versa."""
        assert not issubclass(MoneyError, MalformedEventError)
        assert not issubclass(MalformedEventError, MoneyError)


def test_the_decoder_does_not_mutate_the_payload_it_was_given() -> None:
    """The caller's dict is left alone, because the caller may well log it after the failure.

    A decoder that upper-cased `data["currency"]` in place would make the log line disagree with
    what the producer actually sent, and the first thing anybody does with a rejected message is
    compare it against what the producer's own logs claim to have published.
    """
    payload = envelope_payload(currency="usd", counterparty=" ACME-BANK ")
    before = {**payload, "data": {**payload["data"]}}

    decode_envelope(payload)

    assert payload == before
    assert payload["data"]["currency"] == "usd"
    assert payload["data"]["counterparty"] == " ACME-BANK "


def test_the_envelope_carries_no_field_this_service_does_not_use() -> None:
    """A structural assertion: the envelope's field list is the contract, so it is pinned.

    The reason to pin it is drift in one specific direction. Adding a field to `Envelope` is easy
    and each addition is individually defensible, and after five of them the "narrow duplication"
    argument in the module docstring is no longer true — the class is a copy of the producer's
    schema, and the coupling it was written to avoid is back.
    """
    assert [f.name for f in dataclasses.fields(Envelope)] == [
        "event_id",
        "event_type",
        "aggregate_type",
        "aggregate_id",
        "occurred_at",
        "counterparty",
        "currency",
        "amount",
    ]


def test_a_uuid_object_is_not_accepted_where_a_string_belongs() -> None:
    """The wire format is JSON, so `aggregate_id` is a string — even though `uuid.UUID` would work.

    Accepting a `UUID` object would make the decoder convenient to call from Python tests and would
    quietly permit a code path that no real message can take, which is how a decoder ends up with
    behaviour that is only ever exercised by its own tests.
    """
    with pytest.raises(MalformedEventError, match="must be a string containing a UUID"):
        decode_envelope(envelope_payload(aggregate_id=uuid.uuid4()))


def test_the_default_builder_payload_is_valid() -> None:
    """The builders' own guarantee, asserted once here rather than assumed by every other test.

    If the default payload were invalid, dozens of override tests would fail for a reason unrelated
    to what they are testing, and the failure would look like a decoder bug. One test makes the
    diagnosis immediate.
    """
    envelope = decode_envelope(envelope_payload())
    assert envelope.occurred_at == datetime.fromisoformat(OCCURRED_AT)
    assert envelope.amount == Decimal(AMOUNT)
