"""The read model's state: what a position is, and what one instruction contributes to it.

WHY THE READ MODEL KEEPS PER-INSTRUCTION STATE IT IS SUPPOSED TO BE AGGREGATING AWAY
---------------------------------------------------------------------------------
This is the single most important idea in the projection, and the one that is easiest to get wrong,
so it is stated before any code.

The obvious design for "total pending exposure per counterparty" is a counter you add to. An
`instruction.received` for 1500 USD arrives, so `pending += 1500`. Then `instruction.settled`
arrives and you want `pending -= 1500; settled += 1500` — and you cannot, because *the settled event
does not know that this instruction was previously counted as pending*. It knows the instruction and
the amount; it does not know what this read model already did with it.

There are three ways out and only one of them survives contact with an at-least-once stream:

  1. Trust the event to carry the previous state. Now the producer's payload is coupled to this
     consumer's internal model, and a second consumer with different buckets is impossible.
  2. Re-read the whole instruction history for that aggregate on every event. Correct, and it makes
     every event an O(history) database read — a projection that gets slower for its whole life.
  3. **Keep a snapshot of where this read model last placed each instruction**, and compute the
     transition as a subtraction from the old bucket and an addition to the new one.

Option 3 is what `InstructionSnapshot` is. It looks redundant — why store per-instruction rows in an
aggregated view? — and it is what makes the aggregation *idempotent*, because a transition expressed
as "move it from wherever I last put it to where this event says it belongs" produces the same
result however many times it is applied. That is the property the whole service rests on: replaying
the entire stream twice must produce byte-identical positions.

A useful way to hold it: the snapshots are the projection's *memory of its own decisions*, not a
copy of the write model. They exist so the projection can undo what it previously did.

WHY A POSITION IS BUCKETS RATHER THAN ONE SIGNED NUMBER
---------------------------------------------------------------------------------
One net number cannot answer the question the view exists to answer. "Counterparty ACME, 4.2m USD"
is useless without knowing whether that is settled money or money still in flight — those are
different risks, they are reported to different people, and collapsing them loses information no
downstream consumer can reconstruct. Four buckets, each with an amount and a count, and every
instruction is in exactly one of them at any moment. That last clause is an invariant, and it is
asserted by a test.
"""

from __future__ import annotations

import enum
import uuid
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal

from .events import (
    EVENT_FAILED,
    EVENT_MATCHED,
    EVENT_RECEIVED,
    EVENT_REVERSED,
    EVENT_SETTLED,
    EVENT_VALIDATED,
)
from .money import ZERO


class Bucket(enum.Enum):
    """Where one instruction's money currently sits, from this read model's point of view.

    Deliberately NOT a copy of the producer's status enum. The producer distinguishes RECEIVED,
    VALIDATED and MATCHED because those are different stages of its own workflow; this view
    collapses all three into PENDING because they are the same *risk*, which is the question it
    answers. A read model that mirrors the write model's states one-for-one is not a read model, it
    is a replica — and replicating is the job of a database, not of a projection.

    The practical consequence of the collapse is visible in `bucket_for`: three event types map to
    one bucket, so two of them are no-ops for the numbers. Handling that cleanly is the reason the
    projection distinguishes "applied" from "applied and nothing changed".
    """

    PENDING = "pending"
    SETTLED = "settled"
    FAILED = "failed"
    REVERSED = "reversed"


#: The buckets an instruction can never leave.
#:
#: This is the projection's own judgement, not the producer's. Once money has settled and then been
#: reversed, or once an instruction has failed, no further event should move it — and if one
#: arrives, something is wrong in a way that must be reported rather than absorbed. See
#: `is_legal_transition`.
TERMINAL_BUCKETS: frozenset[Bucket] = frozenset({Bucket.FAILED, Bucket.REVERSED})


#: Which bucket each event type puts an instruction into.
#:
#: An event type absent from this mapping is not an error — it is an event this view does not care
#: about, which is the normal case on a shared stream. `bucket_for` returns None for those and the
#: projection skips them, counted separately from failures so that "we are ignoring 90% of the
#: topic" is a visible fact rather than a silent one.
_BUCKET_BY_EVENT: dict[str, Bucket] = {
    EVENT_RECEIVED: Bucket.PENDING,
    EVENT_VALIDATED: Bucket.PENDING,
    EVENT_MATCHED: Bucket.PENDING,
    EVENT_SETTLED: Bucket.SETTLED,
    EVENT_FAILED: Bucket.FAILED,
    EVENT_REVERSED: Bucket.REVERSED,
}


def bucket_for(event_type: str) -> Bucket | None:
    """The bucket this event type implies, or None if the event is not for this view."""
    return _BUCKET_BY_EVENT.get(event_type)


def is_legal_transition(current: Bucket, target: Bucket) -> bool:
    """Whether an instruction may move from `current` to `target`.

    The rules, and why each one is a rule rather than a convention:

      - **Out of a terminal bucket: never.** A settled-then-reversed instruction that receives
        another `settled` event is either a producer replaying history it should not, or two
        producers writing the same aggregate. Applying it would move real money in a real report.
      - **Back to PENDING: never.** Money does not become in-flight again after settling. An event
        that says it does is almost always this projection reading an event from *before* its
        checkpoint — which is a bug in the ordering guarantee, not a business fact.
      - **PENDING to anything: yes**, including PENDING to PENDING, which is the common case
        (`validated` and `matched` both mean "still pending").
      - **SETTLED to REVERSED: yes.** That is what a reversal is.
      - **SETTLED to FAILED: no.** Settlement already happened; a failure afterwards is a reversal,
        and the producer has a distinct event for it. Accepting `failed` here would put the money in
        a bucket that means "never settled", which is materially untrue.

    Note what this function is NOT: it is not validation of the producer's workflow. It is
    validation of *this projection's* assumptions, and the distinction matters because the honest
    response to a violation is "record it and keep going", not "reject the event as invalid".
    """
    if current in TERMINAL_BUCKETS:
        return False
    if target is Bucket.PENDING:
        # Only legal if we are already there, i.e. `validated` / `matched` following `received`.
        return current is Bucket.PENDING
    if current is Bucket.SETTLED:
        return target is Bucket.REVERSED
    # `current` is PENDING here, and `target` is one of SETTLED / FAILED / REVERSED — all legal.
    # A PENDING instruction going straight to REVERSED is unusual but not impossible (a cancellation
    # of something that never settled), and refusing it would lose an event to protect a rule this
    # projection has no business enforcing.
    return True


@dataclass(frozen=True)
class PositionKey:
    """What a position is keyed by: one counterparty, one currency.

    Frozen and therefore hashable, which is what lets it be a dict key in the store and a set member
    in the change set the projector hands to the fan-out hub.

    ⚠️ CURRENCY IS PART OF THE KEY, NOT A FIELD ON THE POSITION.
       Summing across currencies requires FX rates, FX rates have timestamps, and a read model that
       quietly adds dollars to yen produces a number that is confidently wrong. Making currency part
       of the identity means the wrong sum is not expressible.
    """

    counterparty: str
    currency: str

    def __str__(self) -> str:
        """`ACME-BANK/USD`. Used in URLs, log lines and SSE event names, so it is defined once here.

        Defined rather than left to the default `PositionKey(counterparty='...', currency='...')`
        because that form appears in log output where it is four times longer than useful, and
        because a stable string form is what makes a position addressable in a URL at all.
        """
        return f"{self.counterparty}/{self.currency}"


@dataclass(frozen=True)
class Totals:
    """One bucket's contents: how much money, and how many instructions.

    The count is not decoration. An amount alone cannot distinguish "one 4.2m trade" from "eight
    hundred small ones", and those are different operational situations even though the exposure is
    identical. It is also the cheapest possible consistency check on the projection itself: the sum
    of the four counts must equal the number of instruction snapshots, which is asserted by a test
    that replays a stream and then compares.
    """

    amount: Decimal = ZERO
    count: int = 0


@dataclass(frozen=True)
class BucketDelta:
    """A single change to one bucket, as produced by one half of a transition.

    A transition always yields two of these — a subtraction from the old bucket and an addition to
    the new one — and they are kept separate rather than pre-combined so that the PENDING→PENDING
    case (which produces `-1500 pending` and `+1500 pending`) is handled by the same code path as
    every other case, rather than by a special case that somebody has to remember exists.
    """

    bucket: Bucket
    amount: Decimal
    count: int


@dataclass(frozen=True)
class Position:
    """One counterparty's exposure in one currency, as this read model currently believes it.

    Frozen, and every operation returns a new instance. The reason is not functional purity for its
    own sake: the fan-out hub holds references to positions it has already published, and a position
    that can be mutated under it would let a subscriber observe a state that was never a consistent
    whole. Immutability makes "the thing I published" and "the thing that is true now" two values
    rather than one aliased object.
    """

    key: PositionKey
    pending: Totals = Totals()
    settled: Totals = Totals()
    failed: Totals = Totals()
    # `reversed` is a builtin, and shadowing builtins in a dataclass field produces code where
    # `reversed(x)` means different things in different scopes. The trailing underscore is the
    # conventional escape and is worth the small ugliness.
    reversed_: Totals = Totals()

    #: Bumped only when the numbers above actually change — see `with_deltas`. This is what a client
    #: compares to decide "is this the same position I already have?", and what the fan-out hub
    #: compares to conflate a burst of updates.
    version: int = 0

    #: EVENT time, not wall-clock time: the `occurred_at` of the event that last changed this
    #: position. A read model's "as of" must come from the log, or a projection that is an hour
    #: behind reports every position as freshly updated, which is exactly backwards from the truth
    #: an operator needs. None only while a position is being built from nothing inside the fold.
    updated_at: datetime | None = None

    def totals_for(self, bucket: Bucket) -> Totals:
        """The totals in one bucket.

        A `match` rather than a dict lookup so that adding a member to `Bucket` without handling it
        here is caught by mypy's exhaustiveness checking rather than by a `KeyError` in production.
        That is the entire reason the four buckets are four fields and not a mapping: the type
        checker can see all of them.
        """
        match bucket:
            case Bucket.PENDING:
                return self.pending
            case Bucket.SETTLED:
                return self.settled
            case Bucket.FAILED:
                return self.failed
            case Bucket.REVERSED:
                return self.reversed_

    def with_deltas(self, deltas: tuple[BucketDelta, ...], *, at: datetime) -> Position:
        """Apply a transition's deltas, bumping the version only if anything actually changed.

        ⚠️ THE "ONLY IF ANYTHING CHANGED" CLAUSE IS THE INTERESTING PART, AND IT IS LOAD-BEARING.

           Three of the six event types this view reads (`received`, `validated`, `matched`) map to
           the same bucket, so `validated` following `received` produces deltas of `-1500 pending`
           and `+1500 pending`, which net to nothing. Bumping the version anyway would be easier and
           would mean:

             - every SSE subscriber gets woken for an update in which nothing they can see is
               different — on a busy stream, most pushes carry no information;
             - the cache is invalidated for nothing, so a hot position is evicted and re-loaded
               several times per instruction;
             - a client polling "has this changed since version N?" is told yes, repeatedly, and
               cannot tell a real change from a workflow step it has no visibility into.

           So a no-op transition returns `self` — the *same object*, not an equal copy — and the
           identity check `result is position` is what the test asserts, because an equal-but-new
           object would still defeat the `is` comparisons the hub relies on to skip work.

        Note that `updated_at` is not touched by a no-op either. If it were, two positions could
        have the same version and different content, and every consumer that treats the version as a
        content hash would be subtly wrong.
        """
        # Local copies of the four buckets, folded over the deltas and compared at the end. Building
        # the result in locals rather than in a series of `replace` calls is what makes the "did
        # anything change?" question answerable with one comparison instead of four.
        pending = self.pending
        settled = self.settled
        failed = self.failed
        reversed_ = self.reversed_

        for delta in deltas:
            match delta.bucket:
                case Bucket.PENDING:
                    pending = _add(pending, delta)
                case Bucket.SETTLED:
                    settled = _add(settled, delta)
                case Bucket.FAILED:
                    failed = _add(failed, delta)
                case Bucket.REVERSED:
                    reversed_ = _add(reversed_, delta)

        if (
            pending == self.pending
            and settled == self.settled
            and failed == self.failed
            and reversed_ == self.reversed_
        ):
            return self

        return replace(
            self,
            pending=pending,
            settled=settled,
            failed=failed,
            reversed_=reversed_,
            version=self.version + 1,
            updated_at=at,
        )


def _add(totals: Totals, delta: BucketDelta) -> Totals:
    """Fold one delta into one bucket's totals.

    Written as a free function rather than a method on `Totals` because it is an implementation
    detail of `with_deltas` and exposing it would invite callers to apply deltas to buckets other
    than the one the delta names — which the `match` above is what prevents.
    """
    return Totals(amount=totals.amount + delta.amount, count=totals.count + delta.count)


@dataclass(frozen=True)
class InstructionSnapshot:
    """Where this read model last placed one instruction, and which event told it to.

    See the module docstring: this is the projection's memory of its own decisions, and it is what
    makes a transition expressible as an undo plus a redo.
    """

    instruction_id: uuid.UUID

    #: The position this instruction currently contributes to. Stored rather than recomputed from
    #: the latest event, because an instruction whose counterparty is corrected has to be *removed
    #: from the old position* — and the only record of which position that was is this field.
    key: PositionKey

    #: The amount this read model last added. Stored for the same reason as `key`: the subtraction
    #: has to undo exactly what the addition did, and the current event may carry a different amount
    #: (an amendment). Subtracting the *new* amount from the *old* bucket is the classic way a
    #: projection drifts by exactly the size of the amendment, forever.
    amount: Decimal

    bucket: Bucket

    #: The `event_id` that produced this snapshot. The deduplication key: an event whose id is not
    #: strictly greater than this has already been applied (or is out of order), and the projector
    #: refuses it. This is per-instruction rather than global on purpose — the stream interleaves
    #: aggregates, so the global sequence has gaps from this instruction's point of view, and only
    #: the per-instruction comparison is meaningful.
    last_event_id: int
