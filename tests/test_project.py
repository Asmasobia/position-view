"""Tests for the projection step — where every hard question about a read model is decided.

WHY THIS FILE IS THE CENTRE OF THE SUITE
---------------------------------------------------------------------------------
`project` is pure: two values in, one value out. That is not a stylistic preference, it is what
makes the following affordable, and none of it would be affordable against a database:

  - **all 24 combinations** of four prior buckets against six event types, asserted individually;
  - the **order of the five checks**, which is a design decision that no single-case test can see;
  - **replay idempotence**, by folding a stream twice and comparing the result — the property the
    whole service rests on, tested here in milliseconds with no storage at all.

THE ONE BEHAVIOUR MOST WORTH READING FIRST
`test_the_undo_half_uses_the_stored_amount_not_the_events`. An amended instruction is the case where
a plausible implementation drifts by exactly the amendment delta, forever, silently. Everything else
in this file is ordinary; that test is the reason `InstructionSnapshot` exists.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal

import pytest

from builders import (
    AMOUNT,
    INSTRUCTION_ID,
    KEY,
    OTHER_KEY,
    make_envelope,
    make_snapshot,
)
from position_view.model import (
    EVENT_FAILED,
    EVENT_MATCHED,
    EVENT_RECEIVED,
    EVENT_REVERSED,
    EVENT_SETTLED,
    EVENT_VALIDATED,
    Applied,
    Bucket,
    Envelope,
    Outcome,
    Position,
    PositionKey,
    Skipped,
    SkipReason,
    bucket_for,
    is_legal_transition,
    project,
)

ALL_EVENT_TYPES = [
    EVENT_RECEIVED,
    EVENT_VALIDATED,
    EVENT_MATCHED,
    EVENT_SETTLED,
    EVENT_FAILED,
    EVENT_REVERSED,
]

#: The seven (prior bucket, event type) pairs a transition may legally take, written out.
#:
#: ⚠️ WRITTEN OUT RATHER THAN COMPUTED, AND THE REASON IS A FAILURE MODE IN THE TESTS THEMSELVES.
#:    The first version of the invariant tests below iterated all 24 pairs and called
#:    `pytest.skip("illegal transitions produce no deltas")` for the rest — a skip condition run
#:    by calling the function under test. The suite reported "83 passed, 34 skipped", and a change
#:    that wrongly made a legal transition illegal would have converted a real assertion into a skip
#:    and stayed green. A test that can silently stop testing is worse than no test.
#:
#: Written as data, the same change turns those rows into failures. The cost is that this list must
#: be maintained by hand, which `test_the_legal_pairs_table_is_complete` makes safe: it cross-checks
#: all 24 pairs against `is_legal_transition`, so a stale table fails loudly at one obvious place.
LEGAL_PAIRS = [
    (Bucket.PENDING, EVENT_RECEIVED),
    (Bucket.PENDING, EVENT_VALIDATED),
    (Bucket.PENDING, EVENT_MATCHED),
    (Bucket.PENDING, EVENT_SETTLED),
    (Bucket.PENDING, EVENT_FAILED),
    (Bucket.PENDING, EVENT_REVERSED),
    (Bucket.SETTLED, EVENT_REVERSED),
]


def applied(outcome: Outcome) -> Applied:
    """Narrow an `Outcome` to `Applied`, failing the test with a readable message if it is not.

    Written as a helper rather than repeating `assert isinstance(...)` in thirty tests, and it earns
    its place twice over: it also satisfies mypy, which otherwise reports an error on every
    `outcome.changes` access because `Outcome` is a union and `Skipped` has no `changes`.

    A `cast` would silence mypy without checking anything, so the assertion stays.
    """
    assert isinstance(outcome, Applied), f"expected Applied, got {outcome!r}"
    return outcome


def skipped(outcome: Outcome) -> Skipped:
    """The mirror image of `applied`, for the same two reasons."""
    assert isinstance(outcome, Skipped), f"expected Skipped, got {outcome!r}"
    return outcome


def _target(event_type: str) -> Bucket:
    """`bucket_for` with the `None` case turned into a test failure rather than a type error.

    Every call site here passes one of the six event types this view reads, so `None` is unreachable
    — but `bucket_for` returns `Bucket | None` and mypy is right to insist. Asserting is better than
    a `cast`: if somebody adds an event type to `ALL_EVENT_TYPES` without mapping it, this fails
    with a clear message instead of an `AttributeError` further down.
    """
    bucket = bucket_for(event_type)
    assert bucket is not None, f"{event_type!r} has no bucket"
    return bucket


class TestFiltering:
    """The two cheapest checks, which on a shared stream handle most of the traffic."""

    def test_another_aggregates_event_is_skipped(self) -> None:
        """A `payment.settled` on the same topic has a plausible payload and is not ours.

        Filtering on the event type alone would not catch this — two aggregate types can
        legitimately share a verb, and `payment.settled` would map to `Bucket.SETTLED` and project
        into a position no settlement instruction backs.
        """
        outcome = skipped(project(None, make_envelope(aggregate_type="payment")))
        assert outcome.reason is SkipReason.NOT_OUR_AGGREGATE
        assert "payment" in outcome.detail

    def test_an_unknown_event_type_is_skipped(self) -> None:
        outcome = skipped(project(None, make_envelope(event_type="instruction.archived")))
        assert outcome.reason is SkipReason.UNKNOWN_EVENT_TYPE
        assert "instruction.archived" in outcome.detail

    def test_the_aggregate_check_comes_before_the_event_type_check(self) -> None:
        """An event failing both is reported as the *cheaper* and more general reason.

        This matters for the counters rather than for correctness. `NOT_OUR_AGGREGATE` says "this
        topic carries other people's events", which is architecture; `UNKNOWN_EVENT_TYPE` says "our
        aggregate grew an event we do not read", which is a producer change worth noticing.
        Reporting the second for another aggregate's unknown event would bury that signal in noise.
        """
        outcome = skipped(
            project(None, make_envelope(aggregate_type="payment", event_type="payment.archived"))
        )
        assert outcome.reason is SkipReason.NOT_OUR_AGGREGATE

    def test_filtering_happens_before_the_snapshot_is_consulted(self) -> None:
        """A foreign event is skipped identically whether or not we know the instruction.

        Written because the opposite is a tempting optimisation — look up the snapshot once, then
        decide — and it inverts the cost: the common case (somebody else's event) would pay for a
        storage read, which on a shared stream is most of the reads the service makes.
        """
        for snapshot in (None, make_snapshot()):
            outcome = skipped(project(snapshot, make_envelope(aggregate_type="payment")))
            assert outcome.reason is SkipReason.NOT_OUR_AGGREGATE


class TestFirstSighting:
    """No snapshot: nothing to undo, so the change is a single addition."""

    def test_opens_a_position_and_records_the_instruction(self) -> None:
        outcome = applied(project(None, make_envelope(event_id=42)))

        assert outcome.first_seen is True
        assert outcome.snapshot.instruction_id == INSTRUCTION_ID
        assert outcome.snapshot.key == KEY
        assert outcome.snapshot.bucket is Bucket.PENDING
        assert outcome.snapshot.amount == Decimal(AMOUNT)
        assert outcome.snapshot.last_event_id == 42

        assert len(outcome.changes) == 1
        change = outcome.changes[0]
        assert change.key == KEY
        assert len(change.deltas) == 1
        assert change.deltas[0].bucket is Bucket.PENDING
        assert change.deltas[0].amount == Decimal(AMOUNT)
        assert change.deltas[0].count == 1

    @pytest.mark.parametrize("event_type", ALL_EVENT_TYPES)
    def test_any_event_type_may_open_an_instruction(self, event_type: str) -> None:
        """⚠️ INCLUDING `settled`, WHICH LOOKS WRONG AND IS DELIBERATE.

        On a full replay from the start of the stream, a `settled` as an instruction's first event
        is impossible. But this service will also be started against a stream whose retention window
        has moved past an instruction's `received` event, and then `settled` genuinely *is* the
        earliest truth available.

        Refusing it to protect the workflow model would discard a real instruction and get nothing
        in return — the earlier events are gone either way. Accepting it records what is knowable,
        and `first_seen` is what lets an operator tell the two situations apart: a rebuild makes
        every event a first sighting (expected), while a steady trickle of first sightings that are
        not `received` means retention is shorter than the projection's lag, and positions are being
        built from partial history. Nothing else in the system can detect that.
        """
        outcome = applied(project(None, make_envelope(event_type=event_type)))
        assert outcome.first_seen is True
        assert outcome.snapshot.bucket is bucket_for(event_type)

    def test_the_snapshot_carries_the_envelopes_values(self) -> None:
        """A first sighting has no prior state, so every field comes from the event.

        Worth asserting explicitly because the transition path deliberately mixes stored and
        event-supplied values, and this is the one path where mixing is impossible.
        """
        outcome = applied(
            project(
                None, make_envelope(amount="2500.00", counterparty="BETA-TRUST", currency="usd")
            )
        )
        assert outcome.snapshot.amount == Decimal("2500.00")
        assert outcome.snapshot.key == PositionKey(counterparty="BETA-TRUST", currency="USD")


class TestDeduplication:
    """The two comparisons that make this read model idempotent, and why there are two."""

    def test_the_same_event_twice_is_a_duplicate(self) -> None:
        """Normal, expected, and the reason this service can be restarted without reconciling.

        At-least-once delivery guarantees this happens. It is counted rather than logged as an error
        precisely because it is routine — but the *rate* is worth watching: a rising one means the
        consumer keeps failing after applying and before committing its offset, which is a real
        problem whose only symptom is this counter climbing.
        """
        snapshot = make_snapshot(last_event_id=7)
        outcome = skipped(project(snapshot, make_envelope(event_id=7, event_type=EVENT_SETTLED)))
        assert outcome.reason is SkipReason.DUPLICATE
        assert "7" in outcome.detail

    def test_an_older_event_is_out_of_order_and_not_a_duplicate(self) -> None:
        """⚠️ TWO REASONS, BECAUSE THEY DEMAND TWO REACTIONS.

        A duplicate is delivery working as documented. An event *older* than the one already applied
        means the ordering guarantee this entire design rests on has broken — two producers, a reset
        outbox sequence, or a partitioning change — and no amount of retrying will fix it.

        Collapsing them into one "already handled" counter would hide the second inside the first,
        which is the worst possible place to hide it: `DUPLICATE` is expected to be non-zero, so a
        broken ordering guarantee would look like healthy background noise forever.
        """
        snapshot = make_snapshot(last_event_id=7)
        outcome = skipped(project(snapshot, make_envelope(event_id=6, event_type=EVENT_SETTLED)))
        assert outcome.reason is SkipReason.OUT_OF_ORDER
        assert "6" in outcome.detail
        assert "7" in outcome.detail

    def test_a_newer_event_is_applied(self) -> None:
        snapshot = make_snapshot(last_event_id=7)
        outcome = applied(project(snapshot, make_envelope(event_id=8, event_type=EVENT_SETTLED)))
        assert outcome.snapshot.last_event_id == 8
        assert outcome.first_seen is False

    def test_the_comparison_is_strictly_greater(self) -> None:
        """The boundary between the three answers, checked at the exact adjacent ids.

        Off-by-one here is not a cosmetic bug: `>=` would treat a redelivery as new and apply it
        twice, doubling an instruction's contribution to a position.
        """
        snapshot = make_snapshot(last_event_id=7)
        assert (
            skipped(project(snapshot, make_envelope(event_id=6))).reason is SkipReason.OUT_OF_ORDER
        )
        assert skipped(project(snapshot, make_envelope(event_id=7))).reason is SkipReason.DUPLICATE
        assert isinstance(
            project(snapshot, make_envelope(event_id=8, event_type=EVENT_SETTLED)), Applied
        )

    def test_a_large_gap_forward_is_applied_without_complaint(self) -> None:
        """Gaps are normal and are NOT evidence of loss, which is why nothing here checks for them.

        `event_id` is the producer's global outbox sequence, so from one instruction's point of view
        the numbers jump by however many other aggregates were written in between. A projection that
        treated a gap as a missing event would alarm constantly on a healthy system.
        """
        snapshot = make_snapshot(last_event_id=7)
        outcome = applied(
            project(snapshot, make_envelope(event_id=9_000_000, event_type=EVENT_SETTLED))
        )
        assert outcome.snapshot.last_event_id == 9_000_000


class TestCheckOrder:
    """The design decision that only a test comparing two failing conditions can pin down."""

    def test_out_of_order_is_reported_before_an_illegal_transition(self) -> None:
        """⚠️ THE ORDERING THAT SENDS PEOPLE TO THE RIGHT PROBLEM.

        An out-of-order event very often *also* describes an illegal transition — an old `received`
        arriving after a `settled` is both. Reporting it as `ILLEGAL_TRANSITION` would send somebody
        looking for a producer bug in the settlement engine, when the actual fault is delivery order
        in the transport, and the two investigations share no steps at all.

        So step 3 (ordering) runs before step 4 (transition legality), and this test is what keeps
        it there — the source reads perfectly well with the two blocks swapped.
        """
        snapshot = make_snapshot(bucket=Bucket.SETTLED, last_event_id=7)
        outcome = skipped(project(snapshot, make_envelope(event_id=5, event_type=EVENT_RECEIVED)))
        assert outcome.reason is SkipReason.OUT_OF_ORDER

    def test_duplicate_is_reported_before_an_illegal_transition(self) -> None:
        """Same argument: a redelivered `settled` against a settled snapshot is not a producer bug.

        Without this ordering, every redelivery of a terminal event would be counted as an illegal
        transition — a counter documented as "should be flat zero, needs a human" would sit at the
        redelivery rate, and the alert built on it would be turned off within a week.
        """
        snapshot = make_snapshot(bucket=Bucket.SETTLED, last_event_id=7)
        outcome = skipped(project(snapshot, make_envelope(event_id=7, event_type=EVENT_SETTLED)))
        assert outcome.reason is SkipReason.DUPLICATE

    def test_the_event_type_check_comes_before_deduplication(self) -> None:
        """An unknown event type is not our business even when its id looks like a duplicate.

        The id space is shared across aggregates, so an unrelated event's id colliding with an
        instruction's `last_event_id` is ordinary. Reporting that as a duplicate would attribute
        another aggregate's traffic to this instruction's delivery behaviour.
        """
        snapshot = make_snapshot(last_event_id=7)
        outcome = skipped(project(snapshot, make_envelope(event_id=7, event_type="instruction.x")))
        assert outcome.reason is SkipReason.UNKNOWN_EVENT_TYPE

    def test_an_illegal_transition_names_both_ends(self) -> None:
        """Because the first question is always "from what, to what?"."""
        snapshot = make_snapshot(bucket=Bucket.FAILED, last_event_id=7)
        outcome = skipped(project(snapshot, make_envelope(event_id=8, event_type=EVENT_SETTLED)))
        assert outcome.reason is SkipReason.ILLEGAL_TRANSITION
        assert "failed" in outcome.detail
        assert "settled" in outcome.detail


class TestTransitionArithmetic:
    """Undo what this projection did, then redo what the event says. The heart of the design."""

    def test_a_settlement_moves_money_between_buckets(self) -> None:
        snapshot = make_snapshot(bucket=Bucket.PENDING, amount="1500.75", last_event_id=7)
        outcome = applied(
            project(snapshot, make_envelope(event_id=8, event_type=EVENT_SETTLED, amount="1500.75"))
        )

        assert len(outcome.changes) == 1
        removal, addition = outcome.changes[0].deltas
        assert (removal.bucket, removal.amount, removal.count) == (
            Bucket.PENDING,
            Decimal("-1500.75"),
            -1,
        )
        assert (addition.bucket, addition.amount, addition.count) == (
            Bucket.SETTLED,
            Decimal("1500.75"),
            1,
        )

    def test_the_undo_half_uses_the_stored_amount_not_the_events(self) -> None:
        """⚠️ THE MOST IMPORTANT TEST IN THIS REPOSITORY. READ THE FAILURE MODE BEFORE THE CODE.

        An instruction is received for 1500.75 and later settles for 2000.00 — the amount was
        amended upstream between the two events. The correct deltas are:

            -1500.75 from pending   (the amount this projection actually added)
            +2000.00 to settled     (the amount the event now states)

        The tempting implementation uses `envelope.amount` for both halves, because it is right
        there and it is right in every test where the amount never changes — which is every
        happy-path test anybody writes. What it produces on an amendment is:

            -2000.00 from pending   ← 499.25 too much
            +2000.00 to settled

        …leaving pending permanently short by the amendment delta. No exception, no log line, no
        counter: the position is simply wrong from then until the stream is replayed, and it is
        wrong by an amount that looks like a rounding artefact rather than a bug.

        This is the entire reason `InstructionSnapshot.amount` exists, and this test is the only
        thing standing between that field and somebody deleting it as redundant.
        """
        snapshot = make_snapshot(bucket=Bucket.PENDING, amount="1500.75", last_event_id=7)
        outcome = applied(
            project(snapshot, make_envelope(event_id=8, event_type=EVENT_SETTLED, amount="2000.00"))
        )

        removal, addition = outcome.changes[0].deltas
        assert removal.amount == Decimal("-1500.75"), "the undo must use the amount that was added"
        assert addition.amount == Decimal("2000.00"), (
            "the redo must use the amount that is now true"
        )
        assert outcome.snapshot.amount == Decimal("2000.00"), (
            "and the snapshot must remember the new one"
        )

    def test_the_undo_half_uses_the_stored_bucket_not_the_targets(self) -> None:
        """The bucket has the same failure mode as the amount, and it is even quieter.

        Subtracting from the target bucket instead of the stored one turns a settlement into
        `-1500.75 settled, +1500.75 settled` — a net zero. Pending keeps the money forever, settled
        never receives it, and because the deltas cancel, `with_deltas` returns the same object and
        the version does not even bump. The instruction becomes invisible.
        """
        snapshot = make_snapshot(bucket=Bucket.PENDING, amount="1500.75", last_event_id=7)
        outcome = applied(project(snapshot, make_envelope(event_id=8, event_type=EVENT_SETTLED)))
        removal, addition = outcome.changes[0].deltas
        assert removal.bucket is Bucket.PENDING
        assert addition.bucket is Bucket.SETTLED

    def test_a_no_op_transition_still_produces_deltas(self) -> None:
        """`validated` after `received`: PENDING→PENDING, two deltas that net to zero.

        The deltas are emitted rather than suppressed, and the netting is decided by
        `Position.with_deltas`. Keeping the two concerns apart is what allows the version rule to
        live in one place — `project` describes what the event means, and the state decides whether
        that means anything observable.
        """
        snapshot = make_snapshot(bucket=Bucket.PENDING, amount="1500.75", last_event_id=7)
        outcome = applied(project(snapshot, make_envelope(event_id=8, event_type=EVENT_VALIDATED)))
        removal, addition = outcome.changes[0].deltas
        assert removal.bucket is addition.bucket is Bucket.PENDING
        assert removal.amount + addition.amount == Decimal("0.00")
        assert removal.count + addition.count == 0

    def test_an_amendment_on_a_no_op_transition_is_not_a_no_op(self) -> None:
        """PENDING→PENDING with a changed amount moves real money, and must not net to zero.

        The plausible shortcut — "same bucket, skip it" — would silently discard every amendment
        that arrives on a `validated` or `matched` event, which is exactly where amendments arrive.
        """
        snapshot = make_snapshot(bucket=Bucket.PENDING, amount="1500.75", last_event_id=7)
        outcome = applied(
            project(snapshot, make_envelope(event_id=8, event_type=EVENT_MATCHED, amount="2000.00"))
        )
        removal, addition = outcome.changes[0].deltas
        assert removal.amount + addition.amount == Decimal("499.25")

    def test_the_new_snapshot_replaces_every_field(self) -> None:
        snapshot = make_snapshot(bucket=Bucket.PENDING, amount="1500.75", last_event_id=7)
        outcome = applied(
            project(
                snapshot,
                make_envelope(event_id=8, event_type=EVENT_SETTLED, amount="2000.00"),
            )
        )
        assert outcome.snapshot.bucket is Bucket.SETTLED
        assert outcome.snapshot.amount == Decimal("2000.00")
        assert outcome.snapshot.last_event_id == 8
        assert outcome.snapshot.instruction_id == INSTRUCTION_ID


class TestCrossPositionMove:
    """A correction to the counterparty or the currency: the money has to leave and arrive."""

    def test_a_corrected_counterparty_produces_two_changes(self) -> None:
        """Two positions, and both must be published — the old one *goes down*.

        A subscriber watching only the old counterparty has to see the decrease, or its view keeps
        the money forever. That is why the projector is handed both keys rather than just the new
        one.
        """
        snapshot = make_snapshot(key=KEY, bucket=Bucket.PENDING, amount="1500.75", last_event_id=7)
        outcome = applied(
            project(
                snapshot,
                make_envelope(event_id=8, event_type=EVENT_SETTLED, counterparty="BETA-TRUST"),
            )
        )

        assert len(outcome.changes) == 2
        old, new = outcome.changes
        assert old.key == KEY
        assert old.deltas[0].bucket is Bucket.PENDING
        assert old.deltas[0].amount == Decimal("-1500.75")
        assert old.deltas[0].count == -1
        assert new.key == OTHER_KEY
        assert new.deltas[0].bucket is Bucket.SETTLED
        assert new.deltas[0].amount == Decimal(AMOUNT)
        assert new.deltas[0].count == 1

    def test_a_corrected_currency_produces_two_changes(self) -> None:
        """Currency is half the key, so changing it is the same move as changing the party."""
        snapshot = make_snapshot(key=KEY, last_event_id=7)
        outcome = applied(
            project(snapshot, make_envelope(event_id=8, event_type=EVENT_SETTLED, currency="EUR"))
        )
        assert [change.key for change in outcome.changes] == [
            KEY,
            PositionKey(counterparty="ACME-BANK", currency="EUR"),
        ]

    def test_the_removal_comes_first(self) -> None:
        """Irrelevant to correctness — the two positions are disjoint — and pinned anyway.

        The order is what a log of the change set reads like, and "money left A, money arrived at B"
        is the sequence a person reconstructing an incident expects. Asserting it costs one line and
        prevents a diff that looks like a behaviour change.
        """
        snapshot = make_snapshot(key=KEY, last_event_id=7)
        outcome = applied(
            project(snapshot, make_envelope(event_id=8, event_type=EVENT_SETTLED, currency="EUR"))
        )
        assert outcome.changes[0].deltas[0].count == -1
        assert outcome.changes[1].deltas[0].count == 1

    def test_an_amendment_and_a_move_at_once_are_both_honoured(self) -> None:
        """The two rules compose: the stored amount leaves the old key, the new one arrives.

        This is the case where using the envelope's amount for both halves would move money *and*
        get the quantity wrong, in two different positions, from one event.
        """
        snapshot = make_snapshot(key=KEY, amount="1500.75", last_event_id=7)
        outcome = applied(
            project(
                snapshot,
                make_envelope(
                    event_id=8,
                    event_type=EVENT_SETTLED,
                    counterparty="BETA-TRUST",
                    amount="2000.00",
                ),
            )
        )
        old, new = outcome.changes
        assert old.deltas[0].amount == Decimal("-1500.75")
        assert new.deltas[0].amount == Decimal("2000.00")

    def test_a_move_within_the_same_bucket_is_still_two_changes(self) -> None:
        """PENDING→PENDING across two counterparties nets to zero *globally* and not per position.

        An implementation that checked "did the bucket change?" before "did the key change?" would
        treat this as a no-op and leave the money on the wrong counterparty permanently.
        """
        snapshot = make_snapshot(key=KEY, bucket=Bucket.PENDING, last_event_id=7)
        outcome = applied(
            project(
                snapshot,
                make_envelope(event_id=8, event_type=EVENT_VALIDATED, counterparty="BETA-TRUST"),
            )
        )
        assert len(outcome.changes) == 2
        assert outcome.changes[0].deltas[0].bucket is Bucket.PENDING
        assert outcome.changes[1].deltas[0].bucket is Bucket.PENDING

    def test_the_same_key_spelled_differently_is_not_a_move(self) -> None:
        """The boundary already canonicalised the key, so `"usd"` cannot look like a change.

        This is the test that connects the two modules: if `decode_envelope` stopped upper-casing, a
        stream of lower-case currencies would be read as an endless series of corrections, moving
        money back and forth between two positions on every event.
        """
        snapshot = make_snapshot(key=KEY, last_event_id=7)
        outcome = applied(
            project(
                snapshot,
                make_envelope(
                    event_id=8, event_type=EVENT_SETTLED, currency="usd", counterparty=" ACME-BANK "
                ),
            )
        )
        assert len(outcome.changes) == 1


class TestEveryCombination:
    """All 24 prior-bucket x event-type pairs. The claim "we handle every case", made checkable."""

    @pytest.mark.parametrize("event_type", ALL_EVENT_TYPES)
    @pytest.mark.parametrize("current", list(Bucket))
    def test_outcome_agrees_with_the_transition_table(
        self, current: Bucket, event_type: str
    ) -> None:
        """The projection must accept exactly what `is_legal_transition` permits — no more, no less.

        Cross-checking two implementations of one rule is usually a bad test, because it passes when
        both are wrong together. It is worth it here for a different reason: the transition table is
        tested exhaustively *against written-out expectations* in `test_positions.py`, so this test
        is not checking the rule, it is checking that `project` actually consults it — and consults
        it for every one of the 24 pairs rather than for the four somebody thought of.
        """
        target = bucket_for(event_type)
        assert target is not None

        snapshot = make_snapshot(bucket=current, last_event_id=7)
        outcome = project(snapshot, make_envelope(event_id=8, event_type=event_type))

        if is_legal_transition(current, target):
            assert applied(outcome).snapshot.bucket is target
        else:
            assert skipped(outcome).reason is SkipReason.ILLEGAL_TRANSITION

    def test_the_legal_pairs_table_is_complete(self) -> None:
        """`LEGAL_PAIRS` must be exactly the set `is_legal_transition` permits — no more, no fewer.

        This is the one place the hand-written table is cross-checked against the implementation, so
        a rule change fails here, once, with a readable diff, instead of quietly turning the
        invariant tests below into a smaller suite.
        """
        computed = {
            (current, event_type)
            for current in Bucket
            for event_type in ALL_EVENT_TYPES
            if is_legal_transition(current, _target(event_type))
        }
        assert computed == set(LEGAL_PAIRS)
        assert len(LEGAL_PAIRS) == 7

    @pytest.mark.parametrize(("current", "event_type"), LEGAL_PAIRS)
    def test_an_applied_transition_always_conserves_count(
        self, current: Bucket, event_type: str
    ) -> None:
        """Across every legal pair: one instruction leaves one bucket and arrives in one bucket.

        Summed over all the deltas an event produces, the counts must cancel — because an
        instruction that already exists is being *moved*, never created. The one exception is a
        first sighting, which is not in this table.

        This is the invariant that catches an entire family of bugs at once: a missing `count=-1`, a
        removal emitted twice, a move that only publishes one of its two positions.
        """
        snapshot = make_snapshot(bucket=current, last_event_id=7)
        outcome = applied(project(snapshot, make_envelope(event_id=8, event_type=event_type)))
        total = sum(delta.count for change in outcome.changes for delta in change.deltas)
        assert total == 0

    @pytest.mark.parametrize(("current", "event_type"), LEGAL_PAIRS)
    def test_an_applied_transition_conserves_money_when_the_amount_is_unchanged(
        self, current: Bucket, event_type: str
    ) -> None:
        """The amount analogue of the count invariant, and why it needs the caveat in its name.

        Money is only conserved when the event does not amend it — which is the normal case, and the
        case where a sign error in the undo half would otherwise pass unnoticed because the two
        halves are the same magnitude.
        """
        snapshot = make_snapshot(bucket=current, amount=AMOUNT, last_event_id=7)
        outcome = applied(project(snapshot, make_envelope(event_id=8, event_type=event_type)))
        total = sum(
            (delta.amount for change in outcome.changes for delta in change.deltas), Decimal("0.00")
        )
        assert total == Decimal("0.00")


class TestPurity:
    """`project` is a function of its two arguments. Several other properties depend on that."""

    def test_the_snapshot_it_was_given_is_not_modified(self) -> None:
        """The projector reads a snapshot, calls this, and writes the result. If the input were
        mutated, the "what did I previously do?" question would answer itself with the new state,
        and
        the undo half of every transition would subtract the wrong amount."""
        snapshot = make_snapshot(bucket=Bucket.PENDING, amount="1500.75", last_event_id=7)
        before = dataclasses.asdict(snapshot)
        project(snapshot, make_envelope(event_id=8, event_type=EVENT_SETTLED, amount="2000.00"))
        assert dataclasses.asdict(snapshot) == before

    def test_the_same_inputs_produce_equal_outcomes(self) -> None:
        """Deterministic: no clock, no randomness, no ambient state. Two calls, one answer.

        This is what makes a replay reproducible. A projection that consulted `datetime.now()` — for
        an `updated_at`, say — would produce a different result on every run, and "replaying the
        stream gives the same positions" would stop being checkable.
        """
        snapshot = make_snapshot(last_event_id=7)
        envelope = make_envelope(event_id=8, event_type=EVENT_SETTLED)
        assert project(snapshot, envelope) == project(snapshot, envelope)

    def test_outcomes_are_frozen(self) -> None:
        """The results are values, so a caller cannot edit a change set on its way to storage."""
        outcome = applied(project(None, make_envelope()))
        with pytest.raises(dataclasses.FrozenInstanceError):
            outcome.first_seen = False  # type: ignore[misc]
        with pytest.raises(dataclasses.FrozenInstanceError):
            outcome.changes[0].key = OTHER_KEY  # type: ignore[misc]

    def test_a_skip_carries_a_reason_and_a_detail_but_no_state(self) -> None:
        """`Skipped` deliberately has no snapshot field: nothing was applied, so nothing is written.

        A skip that carried a snapshot would invite the projector to write it, and writing an
        unchanged snapshot on every ignored event turns a shared stream into a write-heavy workload
        for no benefit.
        """
        outcome = skipped(project(None, make_envelope(aggregate_type="payment")))
        assert {field.name for field in dataclasses.fields(outcome)} == {"reason", "detail"}

    def test_every_skip_reason_has_a_detail(self) -> None:
        """Because a counter tells you *that* something was skipped; only the log says which one.

        Every reason is produced at least once here, which also means this test fails if a new
        `SkipReason` is added without a path that emits it — a dead enum member that a dashboard
        would display forever as a permanent zero.
        """
        cases: dict[SkipReason, Outcome] = {
            SkipReason.NOT_OUR_AGGREGATE: project(None, make_envelope(aggregate_type="payment")),
            SkipReason.UNKNOWN_EVENT_TYPE: project(None, make_envelope(event_type="instruction.x")),
            SkipReason.DUPLICATE: project(
                make_snapshot(last_event_id=7), make_envelope(event_id=7)
            ),
            SkipReason.OUT_OF_ORDER: project(
                make_snapshot(last_event_id=7), make_envelope(event_id=6)
            ),
            SkipReason.ILLEGAL_TRANSITION: project(
                make_snapshot(bucket=Bucket.FAILED, last_event_id=7),
                make_envelope(event_id=8, event_type=EVENT_SETTLED),
            ),
        }
        assert set(cases) == set(SkipReason)
        for reason, outcome in cases.items():
            assert skipped(outcome).reason is reason
            assert skipped(outcome).detail != ""


class TestFoldingAStream:
    """A miniature projector, in the test file, so the composition is proved before storage exists.

    WHY THIS BELONGS IN SLICE 1 RATHER THAN WAITING FOR THE REAL STORE
    The interesting properties of a projection — that deltas compose into correct totals, and that a
    replay is idempotent — are properties of the *pure* core. Testing them against a database would
    prove the same thing more slowly while also testing the database, and if the fold were wrong the
    failure would arrive dressed as a storage bug.

    Everything below folds outcomes by hand into two dicts. That is a deliberate 20-line duplication
    of what `projection/` will do for real, and it is worth it: this fold has no transactions, no
    checkpoint and no I/O, so a failure here can only be the arithmetic.
    """

    @staticmethod
    def fold(envelopes: list[Envelope]) -> dict[PositionKey, Position]:
        """Apply a stream to empty state and return the positions. The projector's job, inlined."""
        snapshots: dict[str, object] = {}
        positions: dict[PositionKey, Position] = {}

        for envelope in envelopes:
            key_of_instruction = str(envelope.aggregate_id)
            previous = snapshots.get(key_of_instruction)
            outcome = project(previous, envelope)  # type: ignore[arg-type]
            if isinstance(outcome, Skipped):
                continue
            for change in outcome.changes:
                position = positions.get(change.key, Position(key=change.key))
                positions[change.key] = position.with_deltas(change.deltas, at=envelope.occurred_at)
            snapshots[key_of_instruction] = outcome.snapshot

        return positions

    def stream(self) -> list[Envelope]:
        """One instruction through its whole life, plus a second one that fails.

        Two instructions rather than one, because a single-instruction stream cannot distinguish a
        position total from an instruction amount — and every count assertion would pass for an
        implementation that ignored counts entirely.
        """
        return [
            make_envelope(event_id=1, event_type=EVENT_RECEIVED, amount="1500.75"),
            make_envelope(event_id=2, event_type=EVENT_VALIDATED, amount="1500.75"),
            make_envelope(event_id=3, event_type=EVENT_MATCHED, amount="1500.75"),
            make_envelope(
                event_id=4,
                event_type=EVENT_RECEIVED,
                amount="500.25",
                aggregate_id="22222222-2222-4222-8222-222222222222",
            ),
            make_envelope(event_id=5, event_type=EVENT_SETTLED, amount="1500.75"),
            make_envelope(
                event_id=6,
                event_type=EVENT_FAILED,
                amount="500.25",
                aggregate_id="22222222-2222-4222-8222-222222222222",
            ),
        ]

    def test_the_totals_are_right(self) -> None:
        positions = self.fold(self.stream())
        position = positions[KEY]

        assert position.pending.amount == Decimal("0.00")
        assert position.pending.count == 0
        assert position.settled.amount == Decimal("1500.75")
        assert position.settled.count == 1
        assert position.failed.amount == Decimal("500.25")
        assert position.failed.count == 1
        assert position.reversed_.amount == Decimal("0.00")

    def test_the_counts_sum_to_the_number_of_instructions(self) -> None:
        """The cheapest consistency check a projection has on itself, and it is available for free.

        Two instructions went in, so the four counts must total two. A projection that dropped a
        removal would total three; one that dropped an addition would total one. Neither shows up in
        an amount assertion if the amounts happen to be equal.
        """
        position = self.fold(self.stream())[KEY]
        total = sum(position.totals_for(bucket).count for bucket in Bucket)
        assert total == 2

    def test_the_version_counts_only_the_observable_changes(self) -> None:
        """Six events, four of which changed something visible in this position.

        `received` (1), the second instruction's `received` (2), `settled` (3) and `failed` (4)
        change the numbers; `validated` and `matched` do not. So the version is 4 and not 6 — and a
        client polling this position was woken four times rather than six, which on a real stream is
        the difference between a quiet service and a loud one.
        """
        assert self.fold(self.stream())[KEY].version == 4

    def test_updated_at_is_the_last_events_own_time(self) -> None:
        """Event time, not wall clock, carried through the fold from the envelope."""
        stream = self.stream()
        assert self.fold(stream)[KEY].updated_at == stream[-1].occurred_at

    def test_replaying_the_stream_twice_produces_an_identical_result(self) -> None:
        """⚠️ THE PROPERTY THE ENTIRE SERVICE RESTS ON, AND THE REASON FOR EVERY SNAPSHOT FIELD.

        The stream is folded once, then folded again over the *same* starting state — every event
        delivered a second time. Every duplicate is recognised by `event_id` and skipped, so the
        second pass changes nothing at all: same totals, same counts, same versions.

        Without this, a consumer restart is a reconciliation exercise. With it, a restart is a
        no-event.
        """
        stream = self.stream()
        once = self.fold(stream)
        twice = self.fold(stream + stream)
        assert once == twice

    def test_replaying_from_a_mid_stream_position_reaches_the_same_answer(self) -> None:
        """Order of arrival within the *applied* set does not matter as long as ids increase.

        Concretely: interleaving the two instructions differently produces the same final positions,
        because each instruction's state is tracked separately. This is what makes it safe for the
        stream to carry many aggregates on one partition.
        """
        stream = self.stream()
        reordered = [stream[3], stream[0], stream[1], stream[5], stream[2], stream[4]]
        assert self.fold(reordered) == self.fold(stream)

    def test_an_out_of_order_event_does_not_corrupt_the_totals(self) -> None:
        """A stale `received` arriving after settlement is dropped, and the totals stay right.

        This is the end-to-end version of the check-order test: without the ordering comparison, the
        stale event would be applied as a transition back to PENDING, and the position would report
        the instruction as both settled and in flight — with a count of 2 for one instruction.
        """
        stream = self.stream()
        stale = make_envelope(event_id=2, event_type=EVENT_RECEIVED, amount="1500.75")
        position = self.fold([*stream, stale])[KEY]
        assert position.settled == self.fold(stream)[KEY].settled
        assert sum(position.totals_for(bucket).count for bucket in Bucket) == 2
