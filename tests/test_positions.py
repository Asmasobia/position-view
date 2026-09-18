"""Tests for the read model's state: buckets, keys, totals, and the version rule.

THE TWO CLAIMS IN THIS FILE THAT ARE WORTH MORE THAN THE REST COMBINED
---------------------------------------------------------------------------------
1. **The transition table is complete.** Sixteen bucket pairs, every one of them asserted, because a
   transition rule that is merely *usually* right produces a position that is wrong in a way no
   downstream check can detect. A table-driven test is the only honest way to say "all of them".

2. **A no-op returns the same object.** Not an equal object — the *same* one, asserted with `is`.
   Three of the six event types this view reads map to the same bucket, so on a busy stream most
   events change nothing observable. Everything downstream (SSE fan-out, cache invalidation, client
   polling) is built on "version changed means content changed", and that only holds if a net-zero
   transition is invisible.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from builders import KEY, OTHER_KEY, make_snapshot
from position_view.model import (
    EVENT_FAILED,
    EVENT_MATCHED,
    EVENT_RECEIVED,
    EVENT_REVERSED,
    EVENT_SETTLED,
    EVENT_VALIDATED,
    TERMINAL_BUCKETS,
    ZERO,
    Bucket,
    BucketDelta,
    Position,
    PositionKey,
    Totals,
    bucket_for,
    is_legal_transition,
)

AT = datetime(2026, 3, 1, 9, 30, tzinfo=UTC)
LATER = datetime(2026, 3, 1, 10, 15, tzinfo=UTC)

#: `TERMINAL_BUCKETS` in a deterministic order, hoisted here rather than sorted inline in the
#: `parametrize` call below.
#:
#: Sorted at all because a `frozenset` has no defined iteration order, and an unordered parametrised
#: test produces ids that shuffle between runs — which makes a diff of two CI logs unreadable for no
#: benefit.
#:
#: Hoisted because of how mypy infers inside a decorator argument. `parametrize`'s second parameter
#: is typed as an iterable of `object`, so a `sorted(...)` in that position has its type variable
#: solved from the *expected* type rather than from the argument: the element type comes out as
#: `object`, and `key=lambda bucket: bucket.value` then fails with "object has no attribute value".
#: The explicit annotation here gives the inference somewhere to start, and the lambda type-checks.
TERMINAL_ORDER: list[Bucket] = sorted(TERMINAL_BUCKETS, key=lambda bucket: bucket.value)


class TestBucketMapping:
    """Which event type means which bucket: where the write model's states are collapsed."""

    @pytest.mark.parametrize(
        ("event_type", "expected"),
        [
            # Three event types, one bucket. This collapse is the whole reason this is a read model
            # rather than a replica: RECEIVED, VALIDATED and MATCHED are different stages of the
            # producer's workflow and the same *risk*, and risk is the question this view answers.
            (EVENT_RECEIVED, Bucket.PENDING),
            (EVENT_VALIDATED, Bucket.PENDING),
            (EVENT_MATCHED, Bucket.PENDING),
            (EVENT_SETTLED, Bucket.SETTLED),
            (EVENT_FAILED, Bucket.FAILED),
            (EVENT_REVERSED, Bucket.REVERSED),
        ],
    )
    def test_maps_the_six_event_types_this_view_reads(
        self, event_type: str, expected: Bucket
    ) -> None:
        assert bucket_for(event_type) is expected

    @pytest.mark.parametrize(
        "event_type",
        [
            "instruction.archived",
            "payment.settled",
            # Case matters. Event types are identifiers on a wire contract, not prose, and a
            # case-insensitive lookup would quietly accept a producer that changed its convention.
            "Instruction.Settled",
            "INSTRUCTION.SETTLED",
            # Near-misses, which are the realistic typo shapes.
            "instruction.settle",
            "instruction.settled ",
            "settled",
            "",
        ],
    )
    def test_returns_none_for_anything_else(self, event_type: str) -> None:
        """None means "not for this view", which is a normal answer and not an error.

        On a shared stream most events land here. The projection counts these separately from
        failures precisely so that "we ignore 90% of this topic" is a visible fact — and so that the
        interesting alert is the count dropping to zero (somebody moved the topic), not it being
        high.
        """
        assert bucket_for(event_type) is None

    def test_every_bucket_is_reachable_from_some_event_type(self) -> None:
        """A bucket no event can produce is dead state that still shows up in every API response.

        Written as a set comparison rather than four assertions so that adding a fifth `Bucket`
        member fails here immediately, at the place that would otherwise silently start serialising
        a total that is structurally always zero.
        """
        reachable = {
            bucket_for(event_type)
            for event_type in (
                EVENT_RECEIVED,
                EVENT_VALIDATED,
                EVENT_MATCHED,
                EVENT_SETTLED,
                EVENT_FAILED,
                EVENT_REVERSED,
            )
        }
        assert reachable == set(Bucket)

    def test_terminal_buckets_are_the_documented_two(self) -> None:
        assert TERMINAL_BUCKETS == frozenset({Bucket.FAILED, Bucket.REVERSED})


class TestLegalTransitions:
    """All sixteen pairs. Not a sample — the whole table, with the reason attached to each row."""

    @pytest.mark.parametrize(
        ("current", "target", "legal", "why"),
        [
            # ── From PENDING: everything is legal, including staying put. ──────────────────────
            (Bucket.PENDING, Bucket.PENDING, True, "validated/matched after received"),
            (Bucket.PENDING, Bucket.SETTLED, True, "the ordinary happy path"),
            (Bucket.PENDING, Bucket.FAILED, True, "settlement was attempted and failed"),
            (
                Bucket.PENDING,
                Bucket.REVERSED,
                True,
                # Unusual, and allowed on purpose: a cancellation of something that never settled.
                # Refusing it would discard a real event to protect a workflow rule this projection
                # has no standing to enforce.
                "cancelled before settling",
            ),
            # ── From SETTLED: only a reversal, because the money has already moved. ────────────
            (Bucket.SETTLED, Bucket.REVERSED, True, "that is what a reversal is"),
            (
                Bucket.SETTLED,
                Bucket.PENDING,
                False,
                # Money does not become in-flight again. An event claiming it does is almost always
                # this projection reading an event from *before* its checkpoint, i.e. a broken
                # ordering guarantee rather than a business fact.
                "settled money cannot become pending",
            ),
            (
                Bucket.SETTLED,
                Bucket.FAILED,
                False,
                # The bucket named FAILED means "never settled". Putting already-settled money there
                # is not a rounding of the truth, it is the opposite of it — and the producer has a
                # distinct event (`reversed`) for what actually happened.
                "a failure after settlement is a reversal, and has its own event",
            ),
            (Bucket.SETTLED, Bucket.SETTLED, False, "a second settlement of settled money"),
            # ── From FAILED and REVERSED: nothing, ever. ──────────────────────────────────────
            (Bucket.FAILED, Bucket.PENDING, False, "terminal"),
            (Bucket.FAILED, Bucket.SETTLED, False, "terminal"),
            (Bucket.FAILED, Bucket.FAILED, False, "terminal, including to itself"),
            (Bucket.FAILED, Bucket.REVERSED, False, "terminal"),
            (Bucket.REVERSED, Bucket.PENDING, False, "terminal"),
            (Bucket.REVERSED, Bucket.SETTLED, False, "terminal"),
            (Bucket.REVERSED, Bucket.FAILED, False, "terminal"),
            (Bucket.REVERSED, Bucket.REVERSED, False, "terminal, including to itself"),
        ],
    )
    def test_the_whole_table(self, current: Bucket, target: Bucket, legal: bool, why: str) -> None:
        assert is_legal_transition(current, target) is legal, why

    def test_the_table_above_is_actually_exhaustive(self) -> None:
        """Guards the guard: proves the parametrised table covers all sixteen pairs.

        A table-driven test only means "all of them" if somebody counted, and a table that quietly
        drops a row while a `Bucket` member is added still passes. This computes the product and
        checks the arithmetic, so the claim in the docstring above is verified rather than asserted.
        """
        assert len(Bucket) == 4
        pairs = {(current, target) for current in Bucket for target in Bucket}
        assert len(pairs) == 16

    def test_self_transition_is_legal_only_for_pending(self) -> None:
        """The asymmetry that surprises people, isolated so it is clearly deliberate.

        PENDING→PENDING happens constantly (`validated`, then `matched`). SETTLED→SETTLED means an
        instruction settled twice, which is either a replay past the checkpoint or a producer bug —
        and applying it would add the amount to the settled total a second time.
        """
        assert is_legal_transition(Bucket.PENDING, Bucket.PENDING) is True
        for bucket in (Bucket.SETTLED, Bucket.FAILED, Bucket.REVERSED):
            assert is_legal_transition(bucket, bucket) is False

    @pytest.mark.parametrize("terminal", TERMINAL_ORDER)
    @pytest.mark.parametrize("target", list(Bucket))
    def test_nothing_leaves_a_terminal_bucket(self, terminal: Bucket, target: Bucket) -> None:
        """Written as a product rather than four rows, so a new `Bucket` member is covered for free.

        The row-by-row table above documents the reasoning; this one enforces the rule structurally.
        Both are worth having: the first would still pass if somebody added a fifth bucket, and the
        second would not explain itself.
        """
        assert is_legal_transition(terminal, target) is False


class TestPositionKey:
    """Two strings, and the three properties everything else depends on."""

    def test_string_form_is_the_addressable_one(self) -> None:
        """Used in URLs, log lines and SSE event names, so it is defined once and pinned here."""
        assert str(KEY) == "ACME-BANK/USD"

    def test_is_hashable_and_usable_as_a_mapping_key(self) -> None:
        """Frozen means hashable, and hashable is what lets a store be a dict and changes a set.

        Not a theoretical property: the projector hands the fan-out hub a *set* of changed keys, and
        an unhashable key would fail there rather than here — at runtime, under load, in the one
        code path that only executes when something has actually changed.
        """
        assert {KEY: 1}[PositionKey(counterparty="ACME-BANK", currency="USD")] == 1
        assert len({KEY, PositionKey(counterparty="ACME-BANK", currency="USD")}) == 1

    def test_currency_is_part_of_the_identity(self) -> None:
        """Two currencies are two positions, and no amount of code can make them one.

        Summing across currencies needs FX rates, FX rates have timestamps, and a read model that
        quietly adds dollars to yen produces a number that is confidently wrong. Making currency
        part of the key means that number is not expressible.
        """
        usd = PositionKey(counterparty="ACME-BANK", currency="USD")
        eur = PositionKey(counterparty="ACME-BANK", currency="EUR")
        assert usd != eur
        assert len({usd, eur}) == 2

    def test_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            KEY.currency = "EUR"  # type: ignore[misc]


class TestTotals:
    """An amount and a count, and the reason the count is not decoration."""

    def test_defaults_to_an_empty_bucket_at_the_stored_exponent(self) -> None:
        """`Totals()` is the state of a bucket nothing has happened to, and it serialises as "0.00".

        The exponent is asserted because `Decimal(0)` would serialise as `"0"` — see `money.ZERO`.
        An empty bucket and a bucket that returned to zero must render identically.
        """
        assert Totals() == Totals(amount=ZERO, count=0)
        assert str(Totals().amount) == "0.00"

    def test_count_and_amount_are_independent_facts(self) -> None:
        """One 4.2m trade and eight hundred small ones are the same exposure, different situations.

        The count is also the cheapest consistency check the projection has on itself: the four
        counts must sum to the number of instruction snapshots, which is asserted by the replay test
        in the projector slice.
        """
        assert Totals(amount=Decimal("4200000.00"), count=1) != Totals(
            amount=Decimal("4200000.00"), count=800
        )


class TestTotalsFor:
    """The bucket accessor, and why it is a `match` and not a dict."""

    @pytest.mark.parametrize(
        ("bucket", "expected"),
        [
            (Bucket.PENDING, Totals(amount=Decimal("100.00"), count=1)),
            (Bucket.SETTLED, Totals(amount=Decimal("200.00"), count=2)),
            (Bucket.FAILED, Totals(amount=Decimal("300.00"), count=3)),
            (Bucket.REVERSED, Totals(amount=Decimal("400.00"), count=4)),
        ],
    )
    def test_returns_the_right_bucket(self, bucket: Bucket, expected: Totals) -> None:
        """Four distinct values, so a copy-paste error returning the wrong field cannot pass.

        This is the shape of test that matters for an accessor: if every bucket held the same
        totals, `totals_for` could return `self.pending` for all four inputs and the test would be
        green.
        """
        position = Position(
            key=KEY,
            pending=Totals(amount=Decimal("100.00"), count=1),
            settled=Totals(amount=Decimal("200.00"), count=2),
            failed=Totals(amount=Decimal("300.00"), count=3),
            reversed_=Totals(amount=Decimal("400.00"), count=4),
        )
        assert position.totals_for(bucket) == expected

    def test_covers_every_member_of_the_enum(self) -> None:
        """The `match` has no `case _`, so an unhandled member would return None implicitly.

        mypy's exhaustiveness checking is the real guard — `--strict` rejects a `match` over an enum
        that misses a member when the function is annotated to return `Totals`. This test is the
        runtime half, for the case where a member is added and the type check is not run.
        """
        position = Position(key=KEY)
        for bucket in Bucket:
            assert position.totals_for(bucket) is not None


class TestWithDeltas:
    """The fold, the version rule, and the identity guarantee."""

    def test_a_single_addition_lands_in_the_named_bucket(self) -> None:
        position = Position(key=KEY).with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1500.75"), count=1),), at=AT
        )
        assert position.pending == Totals(amount=Decimal("1500.75"), count=1)
        assert position.settled == Totals()
        assert position.version == 1
        assert position.updated_at == AT

    def test_a_transition_moves_money_between_buckets(self) -> None:
        """The two-delta shape the projection actually emits: undo the old, redo the new.

        Note the assertion that `pending` returns to exactly `Totals()` — same amount *and* same
        count. A projection that decremented the amount and forgot the count would leave a bucket
        reporting "0.00 across 1 instruction", which is the kind of nonsense that reaches a
        dashboard and gets explained away as a display bug.
        """
        opened = Position(key=KEY).with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1500.75"), count=1),), at=AT
        )
        settled = opened.with_deltas(
            (
                BucketDelta(bucket=Bucket.PENDING, amount=Decimal("-1500.75"), count=-1),
                BucketDelta(bucket=Bucket.SETTLED, amount=Decimal("1500.75"), count=1),
            ),
            at=LATER,
        )
        assert settled.pending == Totals()
        assert settled.settled == Totals(amount=Decimal("1500.75"), count=1)
        assert settled.version == 2
        assert settled.updated_at == LATER

    def test_the_original_is_untouched(self) -> None:
        """Immutability, asserted rather than assumed, because the fan-out hub holds old references.

        A subscriber that was handed version 1 must keep seeing version 1's numbers. If positions
        were mutable, "the thing I published" and "the thing that is true now" would be one aliased
        object, and a subscriber could observe a state that was never a consistent whole.
        """
        original = Position(key=KEY)
        original.with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1500.75"), count=1),), at=AT
        )
        assert original.pending == Totals()
        assert original.version == 0
        assert original.updated_at is None

    def test_a_net_zero_transition_returns_the_same_object(self) -> None:
        """⚠️ THE IDENTITY ASSERTION. `is`, not `==`, and the difference is the whole feature.

        `validated` following `received` produces `-1500.75 pending` and `+1500.75 pending`. Nothing
        observable changed, so nothing downstream should be woken: no SSE push, no cache eviction,
        no "yes it changed" answer to a polling client.

        An equal-but-new object would satisfy `==` and defeat every `is` comparison the hub uses to
        skip work — so the test has to be the stricter one, or it would pass for an implementation
        that reintroduces exactly the churn this rule exists to prevent.
        """
        position = Position(key=KEY).with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1500.75"), count=1),), at=AT
        )
        unchanged = position.with_deltas(
            (
                BucketDelta(bucket=Bucket.PENDING, amount=Decimal("-1500.75"), count=-1),
                BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1500.75"), count=1),
            ),
            at=LATER,
        )
        assert unchanged is position

    def test_a_no_op_does_not_advance_updated_at(self) -> None:
        """So `version` stays a content identifier rather than an activity indicator.

        If a no-op touched `updated_at`, two positions could share a version and differ in content,
        and every consumer treating the version as a content hash would be subtly wrong — subtly
        enough that it would be found by a client, not by us.
        """
        position = Position(key=KEY).with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1500.75"), count=1),), at=AT
        )
        unchanged = position.with_deltas(
            (
                BucketDelta(bucket=Bucket.PENDING, amount=Decimal("-1500.75"), count=-1),
                BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1500.75"), count=1),
            ),
            at=LATER,
        )
        assert unchanged.updated_at == AT
        assert unchanged.version == position.version

    def test_an_empty_delta_tuple_is_a_no_op(self) -> None:
        """Defensive, and cheap: the fold over nothing must not bump a version.

        There is no code path in the projection that emits zero deltas today. This asserts the
        behaviour anyway, because "no deltas" is the natural representation of a future event type
        that is recorded but changes no money, and the version rule should already be right when
        somebody adds one.
        """
        position = Position(key=KEY)
        assert position.with_deltas((), at=AT) is position

    def test_a_zero_valued_delta_is_a_no_op_only_if_the_count_is_zero_too(self) -> None:
        """A zero-amount instruction still moves the count, and the count is observable.

        This is the case that would break an implementation whose change detection compared only
        amounts: a zero-value instruction settling would move a count from pending to settled with
        no amount change at all, and a version that did not bump would leave every client showing
        the instruction in the wrong bucket forever.
        """
        position = Position(key=KEY)
        counted = position.with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=ZERO, count=1),), at=AT
        )
        assert counted is not position
        assert counted.pending == Totals(amount=ZERO, count=1)
        assert counted.version == 1

        truly_nothing = position.with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=ZERO, count=0),), at=AT
        )
        assert truly_nothing is position

    def test_deltas_across_all_four_buckets_are_applied_in_one_call(self) -> None:
        """The fold handles every bucket, and one call produces one version bump.

        The per-bucket `match` inside the loop is the part being exercised: an implementation that
        assigned to the wrong local would silently put money in the wrong bucket, and only a test
        touching all four with distinct amounts can see it.
        """
        position = Position(key=KEY).with_deltas(
            (
                BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1.00"), count=1),
                BucketDelta(bucket=Bucket.SETTLED, amount=Decimal("2.00"), count=1),
                BucketDelta(bucket=Bucket.FAILED, amount=Decimal("3.00"), count=1),
                BucketDelta(bucket=Bucket.REVERSED, amount=Decimal("4.00"), count=1),
            ),
            at=AT,
        )
        assert position.pending == Totals(amount=Decimal("1.00"), count=1)
        assert position.settled == Totals(amount=Decimal("2.00"), count=1)
        assert position.failed == Totals(amount=Decimal("3.00"), count=1)
        assert position.reversed_ == Totals(amount=Decimal("4.00"), count=1)
        assert position.version == 1

    def test_two_deltas_on_the_same_bucket_accumulate(self) -> None:
        """Because the fold reads its own running value, not `self`, on each iteration.

        The plausible bug is `pending = _add(self.pending, delta)` inside the loop, which would keep
        only the last delta for a bucket. It passes every single-delta test.
        """
        position = Position(key=KEY).with_deltas(
            (
                BucketDelta(bucket=Bucket.PENDING, amount=Decimal("100.00"), count=1),
                BucketDelta(bucket=Bucket.PENDING, amount=Decimal("50.00"), count=1),
            ),
            at=AT,
        )
        assert position.pending == Totals(amount=Decimal("150.00"), count=2)

    def test_the_key_survives_the_fold(self) -> None:
        """`replace` keeps it, and a position whose key changed under it would be another row."""
        position = Position(key=KEY).with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1.00"), count=1),), at=AT
        )
        assert position.key == KEY

    def test_version_counts_observable_changes_not_events(self) -> None:
        """Three events, two of which change nothing: the version ends at 2, not 4.

        This is the assertion a client's caching depends on, and it is the reason the rule is
        implemented in the state rather than in the projector — the projector sees four events and
        cannot tell which of them mattered.
        """
        position = Position(key=KEY)
        received = position.with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=Decimal("10.00"), count=1),), at=AT
        )
        no_ops = (
            BucketDelta(bucket=Bucket.PENDING, amount=Decimal("-10.00"), count=-1),
            BucketDelta(bucket=Bucket.PENDING, amount=Decimal("10.00"), count=1),
        )
        validated = received.with_deltas(no_ops, at=LATER)
        matched = validated.with_deltas(no_ops, at=LATER)
        settled = matched.with_deltas(
            (
                BucketDelta(bucket=Bucket.PENDING, amount=Decimal("-10.00"), count=-1),
                BucketDelta(bucket=Bucket.SETTLED, amount=Decimal("10.00"), count=1),
            ),
            at=LATER,
        )
        assert (received.version, validated.version, matched.version, settled.version) == (
            1,
            1,
            1,
            2,
        )

    def test_exact_arithmetic_survives_a_long_run_of_transitions(self) -> None:
        """A thousand instructions in and out of pending, ending at exactly zero in both fields.

        The point is not performance. It is that the projection's core operation — add, then
        subtract the same amount later — must return to *exactly* the starting state, or a position
        that has been busy all day slowly stops matching the write model. With `Decimal` this is
        exact; the same loop over floats leaves a residue.
        """
        position = Position(key=KEY)
        amount = Decimal("0.07")
        for index in range(1000):
            position = position.with_deltas(
                (BucketDelta(bucket=Bucket.PENDING, amount=amount, count=1),), at=AT
            )
            assert position.version == index + 1
        for _ in range(1000):
            position = position.with_deltas(
                (BucketDelta(bucket=Bucket.PENDING, amount=-amount, count=-1),), at=AT
            )
        assert position.pending == Totals(amount=ZERO, count=0)
        assert str(position.pending.amount) == "0.00"


class TestPositionShape:
    """Structural properties of the state itself."""

    def test_a_fresh_position_is_empty_in_all_four_buckets(self) -> None:
        position = Position(key=KEY)
        for bucket in Bucket:
            assert position.totals_for(bucket) == Totals()
        assert position.version == 0
        assert position.updated_at is None

    def test_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            Position(key=KEY).version = 5  # type: ignore[misc]

    def test_two_positions_with_the_same_contents_are_equal(self) -> None:
        """Dataclass equality over the whole state, which the store's tests rely on for replay."""
        assert Position(key=KEY) == Position(key=KEY)
        assert Position(key=KEY) != Position(key=OTHER_KEY)

    def test_updated_at_is_none_only_before_anything_happens(self) -> None:
        """The one place `None` is meaningful: a position being built from nothing inside a fold.

        Every other position has an `updated_at`, and it is *event* time. A wall-clock value here
        would make a projection an hour behind report every position as freshly updated, which is
        exactly backwards from what an operator needs to see.
        """
        assert Position(key=KEY).updated_at is None
        touched = Position(key=KEY).with_deltas(
            (BucketDelta(bucket=Bucket.PENDING, amount=Decimal("1.00"), count=1),), at=AT
        )
        assert touched.updated_at == AT


class TestInstructionSnapshot:
    """The projection's memory of its own decisions. Small, and the reason replay is idempotent."""

    def test_records_where_the_money_was_put_and_which_event_put_it_there(self) -> None:
        snapshot = make_snapshot(bucket=Bucket.SETTLED, amount="1500.75", last_event_id=7)
        assert snapshot.key == KEY
        assert snapshot.bucket is Bucket.SETTLED
        assert snapshot.amount == Decimal("1500.75")
        assert snapshot.last_event_id == 7

    def test_is_frozen(self) -> None:
        """A snapshot is replaced wholesale by the projection, never edited.

        Mutating one in place would mean the value the projector *read* and the value it is about to
        *write* are the same object, so the "what did I previously do?" question would answer itself
        with the new state — and the undo half of every transition would subtract the wrong amount.
        """
        with pytest.raises(dataclasses.FrozenInstanceError):
            make_snapshot().amount = Decimal("1.00")  # type: ignore[misc]

    def test_stores_the_amount_it_added_rather_than_referring_to_the_event(self) -> None:
        """The field exists so an amendment cannot drift the projection. Pinned as a shape fact.

        If the snapshot held only an event id and the projection re-read the amount from the current
        event, an amended instruction would be subtracted at its *new* amount from a bucket it was
        added to at its *old* one — leaving exactly the amendment delta behind, in the old bucket,
        forever, with nothing recording that it happened.
        """
        fields = {field.name for field in dataclasses.fields(make_snapshot())}
        assert {"amount", "bucket", "key", "last_event_id", "instruction_id"} == fields
