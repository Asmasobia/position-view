"""The projection step: one event plus one snapshot in, a decision out. Pure, and deliberately so.

WHY THIS IS A FUNCTION AND NOT A METHOD ON A PROJECTOR THAT OWNS A DATABASE
---------------------------------------------------------------------------------
Everything difficult about a read model is decided here: is this a duplicate, is it out of order,
does this transition make sense, whose numbers change and by how much. None of those questions
involve storage, and all of them have to be right for every combination of prior state and incoming
event.

A pure function over two values can be tested exhaustively — every bucket against every event type
is twenty-four cases, and they run in milliseconds with no fixtures. The same logic written as a
method that reads and writes rows can only be tested with a database, so in practice it gets tested
for the happy path and one duplicate, and the illegal transitions are discovered in production.

The storage-shaped half of the job (read the snapshot, apply the deltas, advance the checkpoint, all
in one transaction) lives in `projection/`, and it is deliberately dull: it does no arithmetic and
makes no decisions. That split is what the layering test enforces.

WHAT "APPLIED" DOES AND DOES NOT PROMISE
---------------------------------------------------------------------------------
An `Applied` outcome says "these deltas are what this event means". It does *not* say the position
changed — a `validated` event following a `received` produces deltas that net to zero, and
`Position.with_deltas` returns the same object. The distinction is kept because the projector must
advance its checkpoint for an event that changed nothing (or it will re-read it forever) while *not*
notifying subscribers or invalidating caches. Collapsing the two would force one of those two bugs.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass

from .events import AGGREGATE_INSTRUCTION, Envelope
from .positions import (
    BucketDelta,
    InstructionSnapshot,
    PositionKey,
    bucket_for,
    is_legal_transition,
)


class SkipReason(enum.Enum):
    """Why an event produced no change. Every value here is counted and exposed by `/projection`.

    These are separate values rather than one "skipped" because they need separate *reactions*, and
    a single counter would make all five look like the same healthy background noise:

      - `NOT_OUR_AGGREGATE` and `UNKNOWN_EVENT_TYPE` are expected in volume on a shared stream. A
        sudden drop to zero is the interesting signal (someone changed the topic), not a rise.
      - `DUPLICATE` is normal — at-least-once delivery guarantees it — but a *rising rate* means the
        consumer is failing after applying and before committing its offset, which is a real problem
        that only shows up as this counter climbing.
      - `OUT_OF_ORDER` should be flat zero. Anything else means the ordering guarantee this whole
        design rests on has broken, and no amount of retrying will fix it.
      - `ILLEGAL_TRANSITION` should also be flat zero, and means either a producer bug or that this
        projection's model of the workflow is wrong. Both need a human.
    """

    NOT_OUR_AGGREGATE = "not_our_aggregate"
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    DUPLICATE = "duplicate"
    OUT_OF_ORDER = "out_of_order"
    ILLEGAL_TRANSITION = "illegal_transition"


@dataclass(frozen=True)
class PositionChange:
    """The deltas one event contributes to one position.

    An event usually produces one of these. It produces *two* when an instruction's counterparty or
    currency is corrected, because the money has to leave the old position and arrive at the new one
    — see `project` for why that case is handled rather than refused.
    """

    key: PositionKey
    deltas: tuple[BucketDelta, ...]


@dataclass(frozen=True)
class Applied:
    """The event was accepted. Here is the new snapshot and every position it touches."""

    snapshot: InstructionSnapshot
    changes: tuple[PositionChange, ...]

    #: True when this read model had never seen this instruction before.
    #:
    #: Worth surfacing rather than inferring, for one operational reason: a rebuild from an empty
    #: store makes every event a first sighting, which is expected, while a *steady trickle* of
    #: first sightings whose first event is not `instruction.received` means the stream's retention
    #: is shorter than the projection's lag — so positions are being built from partial history and
    #: are quietly incomplete. Nothing else in the system can detect that.
    first_seen: bool


@dataclass(frozen=True)
class Skipped:
    """The event produced no change, and here is precisely why.

    ⚠️ SKIPPED IS NOT AN ERROR, AND THE PROJECTOR STILL ADVANCES ITS CHECKPOINT PAST IT.
       That is the whole reason this is a returned value rather than a raised exception. An
       exception would have to be caught by the loop and turned back into "keep going" anyway, and
       the version of that code which forgets to advance the checkpoint reprocesses the same poison
       event forever — a stalled projection whose only symptom is lag that grows linearly.
    """

    reason: SkipReason

    #: A human-readable detail for the log line, e.g. which transition was refused. Not machine-read
    #: anywhere; the `reason` is the machine-readable part, and mixing the two is how a log message
    #: ends up being parsed by an alert.
    detail: str = ""


Outcome = Applied | Skipped


def project(snapshot: InstructionSnapshot | None, envelope: Envelope) -> Outcome:
    """Decide what one event means for one instruction. The order of the checks is the design.

    Cheapest and most common first, so that a shared stream full of other people's events costs a
    dictionary lookup rather than a transition analysis:

      1. Is this even our aggregate type?
      2. Is this an event type we project?
      3. Have we already applied this event, or a later one?
      4. Does the transition make sense?
      5. Only then: compute the money.

    Steps 3 and 4 are the two that make this a read model rather than a spreadsheet, and step 3 has
    to come first — an out-of-order event might well describe an illegal transition, and reporting
    it as an illegal transition would send somebody looking for a producer bug when the real problem
    is delivery order.
    """
    if envelope.aggregate_type != AGGREGATE_INSTRUCTION:
        return Skipped(
            SkipReason.NOT_OUR_AGGREGATE,
            f"aggregate_type {envelope.aggregate_type!r} is not {AGGREGATE_INSTRUCTION!r}",
        )

    target = bucket_for(envelope.event_type)
    if target is None:
        return Skipped(SkipReason.UNKNOWN_EVENT_TYPE, f"no bucket for {envelope.event_type!r}")

    key = PositionKey(counterparty=envelope.counterparty, currency=envelope.currency)

    if snapshot is None:
        # First sighting. There is nothing to undo, so the change is a single addition, and *any*
        # event type is accepted as an opener — including `settled`, which on a full replay from the
        # beginning of the stream would be impossible but after a retention-driven gap is simply the
        # earliest truth available. Refusing it to protect the workflow model would discard a real
        # instruction in exchange for nothing; `first_seen` records it instead.
        return Applied(
            snapshot=InstructionSnapshot(
                instruction_id=envelope.aggregate_id,
                key=key,
                amount=envelope.amount,
                bucket=target,
                last_event_id=envelope.event_id,
            ),
            changes=(
                PositionChange(
                    key=key,
                    deltas=(BucketDelta(bucket=target, amount=envelope.amount, count=1),),
                ),
            ),
            first_seen=True,
        )

    if envelope.event_id == snapshot.last_event_id:
        # The same event, delivered twice. Normal, expected, and the reason this service can be
        # restarted without reconciling anything.
        return Skipped(
            SkipReason.DUPLICATE,
            f"event {envelope.event_id} was already applied to {snapshot.instruction_id}",
        )
    if envelope.event_id < snapshot.last_event_id:
        # Strictly older than what we have applied. Distinguished from a duplicate because it means
        # the ordering guarantee broke rather than that a message was redelivered.
        return Skipped(
            SkipReason.OUT_OF_ORDER,
            f"event {envelope.event_id} is older than the applied {snapshot.last_event_id}",
        )

    if not is_legal_transition(snapshot.bucket, target):
        return Skipped(
            SkipReason.ILLEGAL_TRANSITION,
            f"{snapshot.bucket.value} -> {target.value} is not a transition this view allows",
        )

    # The transition, expressed as an undo of what this projection previously did followed by a redo
    # of what this event says. Both halves use the values they were recorded with:
    #
    #   - the SUBTRACTION uses `snapshot.amount` and `snapshot.bucket`, because that is what was
    #     added
    #   - the ADDITION uses `envelope.amount` and `target`, because that is what is true now
    #
    # Using the envelope's amount for both is the most common way a projection drifts: an amended
    # instruction leaves behind exactly the difference between the two amounts, in the old bucket,
    # forever, with nothing anywhere recording that it happened.
    removal = BucketDelta(bucket=snapshot.bucket, amount=-snapshot.amount, count=-1)
    addition = BucketDelta(bucket=target, amount=envelope.amount, count=1)

    # Annotated because mypy would otherwise infer the one-element `tuple[PositionChange]` from the
    # first branch and reject the two-element tuple in the second. The annotation is also the honest
    # description of the return: a variable-length tuple, because how many positions one event
    # touches is a property of the event, not of the code path.
    changes: tuple[PositionChange, ...]
    if key == snapshot.key:
        changes = (PositionChange(key=key, deltas=(removal, addition)),)
    else:
        # The instruction moved between positions — its counterparty or currency was corrected. Two
        # position changes, and the order matters not at all for correctness (they touch disjoint
        # state) but does for the notification the projector emits: both keys changed, so both must
        # be published, and a subscriber watching only the old one has to see it go down.
        #
        # Handling this rather than refusing it is a judgement call, recorded in the ADRs. The
        # argument for handling it: a correction is a legitimate business event, and the alternative
        # leaves the money double-counted across two positions with no way to notice.
        changes = (
            PositionChange(key=snapshot.key, deltas=(removal,)),
            PositionChange(key=key, deltas=(addition,)),
        )

    return Applied(
        snapshot=InstructionSnapshot(
            instruction_id=envelope.aggregate_id,
            key=key,
            amount=envelope.amount,
            bucket=target,
            last_event_id=envelope.event_id,
        ),
        changes=changes,
        first_seen=False,
    )
