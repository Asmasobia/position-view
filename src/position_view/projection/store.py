"""Where the projection's state lives, and the single guarantee the interface has to make.

WHY THIS IS A PROTOCOL AND NOT A BASE CLASS
---------------------------------------------------------------------------------
`PositionStore` is a `typing.Protocol`, so `InMemoryPositionStore` does not inherit from it and does
not mention it. That is the point. A structural type says "anything with these methods will do",
which means the Postgres implementation that arrives later has no import of this module and no risk
of picking up a half-useful concrete method it did not want.

The practical difference shows up in the tests. With an ABC, a test double must subclass it, which
means it inherits every method — including the ones the test does not care about — and a new
abstract method added here breaks every double at once, in a way that has nothing to do with the
behaviour under test. With a protocol, a double implements what it needs and mypy checks the fit at
the point of use.

⚠️ THERE IS EXACTLY ONE WRITE METHOD, AND THAT IS THE WHOLE DESIGN OF THIS FILE.
   The interface offers no way to write a single position, no way to save a snapshot, and no way to
   advance the checkpoint on its own. Everything goes through `commit(WriteBatch)`. That is not
   minimalism — it is the atomicity guarantee expressed as a type, so the dangerous version of this
   code cannot be written by accident.

   The dangerous version looks like this:

       await store.save_position(new_position)      # crash here
       await store.save_snapshot(new_snapshot)
       await store.set_checkpoint(envelope.event_id)

   Crash on the first line and the projection has counted money while its memory of the
   instruction still says the money is in the old bucket. The next event for that instruction undoes
   the OLD bucket again and the projection is now wrong in two buckets at once, permanently, with no
   error anywhere. Crash after the third line but before the first and the checkpoint has moved past
   an event that was never applied — the money is simply gone and no replay will find it, because
   replay starts from the checkpoint.

   A single unit of work makes both of those unrepresentable rather than merely unlikely.

WHY THE READS ARE `async` WHEN THE ONLY IMPLEMENTATION HERE IS A DICT
---------------------------------------------------------------------------------
An in-memory dict needs no `await`, and `async def snapshot_for(...)` over a dict lookup looks like
ceremony. It is shaped by the implementation that matters rather than the one that is convenient:
the real store is a database, database calls suspend, and an interface whose reads are synchronous
cannot be implemented by one without either blocking the event loop or being rewritten.

The cost is real and worth naming: async-ness is contagious, so every caller of a read is now a
coroutine, and the pure model layer stays pure precisely because it is on the other side of this
boundary. `project` is a synchronous function of two values; everything that suspends lives here.
"""

from __future__ import annotations

import uuid
from typing import Final, Protocol

from ..model import InstructionSnapshot, Position, PositionKey

#: The checkpoint value meaning "this projection has applied nothing at all".
#:
#: Zero rather than `None`, and this is why `decode_envelope` refuses `event_id` 0 at the boundary:
#: a sentinel that shares its type with real values only works if no real value can equal it. With
#: `None` every comparison in the projector would need a null check, and the version of that code
#: which forgets one treats an empty projection as being caught up.
NO_CHECKPOINT: Final = 0


class StoreError(Exception):
    """The store was asked to do something structurally invalid.

    Distinct from "the database is down", which is whatever the driver raises and is a *transient*
    failure worth retrying. This one means the caller built a batch that does not make sense — two
    changes to the same position in one unit of work, for instance — and retrying it will fail
    identically. The projector does not catch it: a bug in the projector is not something the
    projector should paper over.
    """


class WriteBatch:
    """Everything one event changes, as one unit of work. Deliberately not a dataclass.

    A frozen dataclass would be the obvious choice and would give a free `__init__`, `__eq__` and
    `__repr__`. It is written out by hand because the validation in `__init__` is the reason the
    type exists: a batch is checked for internal consistency at *construction*, so an invalid batch
    cannot be passed around and blamed on the store that eventually refuses it. `__post_init__`
    would do the same job, and this is the same amount of code without the indirection.

    Three fields, and each one is optional in a different sense:

      - `checkpoint` is always present. Every event advances it, including skipped ones — that is
        the rule `Skipped`-as-a-value exists to make easy to get right.
      - `snapshot` is present when the event was applied and absent when it was skipped. A skip
        changes no per-instruction memory.
      - `positions` is empty in two distinct cases, and the distinction matters: a skipped event
        (nothing happened) and a no-op transition (something happened and it changed no number,
        e.g. `validated` following `received`). The second still carries a `snapshot`, because the
        instruction's `last_event_id` moved even though its bucket did not.
    """

    __slots__ = ("checkpoint", "positions", "snapshot")

    def __init__(
        self,
        *,
        checkpoint: int,
        snapshot: InstructionSnapshot | None = None,
        positions: tuple[Position, ...] = (),
    ) -> None:
        if checkpoint < NO_CHECKPOINT:
            raise StoreError(f"checkpoint may not be negative, got {checkpoint}")

        keys = [position.key for position in positions]
        if len(keys) != len(set(keys)):
            # Two changes to the same position inside one batch. Not a store problem: it means the
            # caller folded a cross-position move wrongly, and whichever entry is written second
            # would silently discard the first. Refusing here names the real bug; letting it through
            # produces a position that is short by exactly one delta and no evidence of why.
            duplicates = sorted({str(key) for key in keys if keys.count(key) > 1})
            raise StoreError(f"a batch may touch each position once; repeated: {duplicates}")

        self.checkpoint = checkpoint
        self.snapshot = snapshot
        self.positions = positions

    def __repr__(self) -> str:
        """Named fields, because a batch appears in assertion failures and in a poison-message log.

        The default `<WriteBatch object at 0x...>` would make every failed assertion about a batch
        require a debugger, and the positions are the interesting part.
        """
        return (
            f"WriteBatch(checkpoint={self.checkpoint}, "
            f"snapshot={'-' if self.snapshot is None else self.snapshot.instruction_id}, "
            f"positions={[str(position.key) for position in self.positions]})"
        )


class PositionStore(Protocol):
    """The storage the projector needs, and nothing else.

    Note what is absent: no `delete`, no `truncate`, no way to write a position directly. A read
    model is rebuilt by replaying the stream into an *empty* store, not by editing the one that is
    wrong — and an interface that offers a hand-edit is an interface somebody will hand-edit at 3am,
    leaving state that no replay can reproduce and no test can explain.
    """

    async def checkpoint(self) -> int:
        """The highest `event_id` this projection has considered. `NO_CHECKPOINT` if none.

        "Considered", not "applied": a skipped event advances it too. The consumer resumes from
        here, so anything it does not cover will be delivered again — which is safe, because the
        snapshot deduplicates.
        """
        ...

    async def snapshot_for(self, instruction_id: uuid.UUID) -> InstructionSnapshot | None:
        """This projection's memory of one instruction, or None if it has never seen it."""
        ...

    async def position_for(self, key: PositionKey) -> Position | None:
        """One position, or None if nothing has ever contributed to it.

        None rather than an empty `Position` on purpose. The api layer has to distinguish "this
        counterparty has no exposure" from "this counterparty is not one we have heard of", because
        the first is a legitimate 200 with zeros and the second is a 404 — and a store that
        manufactures empty positions makes every misspelt counterparty look like a real one with
        nothing outstanding. That distinction is also what slice 4's negative caching caches.
        """
        ...

    async def all_positions(self) -> tuple[Position, ...]:
        """Every position, for the invariant tests and for a full-view endpoint.

        Returning a tuple rather than an iterator or a live view is deliberate: an iterator over the
        store's own mapping would raise if the projector committed while a caller was halfway
        through it, and a live view would let a caller observe a batch being applied. A snapshot
        copy costs a list build and removes both.
        """
        ...

    async def snapshot_count(self) -> int:
        """How many instructions this projection remembers.

        Exposed for one reason, and it is the cheapest correctness check in the whole service: every
        remembered instruction is in exactly one bucket, so the sum of the four bucket counts across
        every position must equal this number. Any drift in the transition arithmetic breaks that
        equality, which is why a test replays a stream and compares the two.
        """
        ...

    async def commit(self, batch: WriteBatch) -> None:
        """Apply one batch atomically: all of it, or none of it.

        The implementation is free to choose how — one SQL transaction, one dict swap — but a
        partial application is a contract violation and not a degraded mode. See the module
        docstring for what each half of a partial write costs.
        """
        ...


class InMemoryPositionStore:
    """A `PositionStore` in three dicts. For tests, and for running the service with no database.

    WHY THIS IS SHIPPED IN `src/` RATHER THAN LIVING IN `tests/`
      Two reasons, and the second is the one that matters. First, it makes the service runnable end
      to end with nothing installed, so the HTTP surface in slice 5 can be exercised against a real
      projector rather than a mock. Second, a test double that lives in `tests/` is under no
      obligation to be *correct* — it accumulates whatever behaviour made the last test pass, and by
      the time a real store exists nobody can say which of the double's behaviours were the contract
      and which were accidents. A shipped implementation is held to the same protocol as the real
      one, and the same tests run against both.

    ⚠️ ATOMICITY HERE COMES FROM NOT SUSPENDING, WHICH IS A PROPERTY OF THIS FILE AND NOT OF PYTHON.
       `commit` contains no `await`. A coroutine only yields control at an `await`, so no other task
       can run between the first mutation and the last, and no reader can observe a half-applied
       batch. Add a single `await` inside `commit` — an audit write, a metric push — and the
       guarantee is silently gone while every test still passes, because the tests would have to
       interleave two commits at exactly that point to notice.

       The mutations are also done last, after every check, so a refused batch leaves the store
       untouched. "Validate everything, then mutate everything" is the only ordering that gives
       all-or-nothing without a transaction to roll back.
    """

    def __init__(self) -> None:
        self._positions: dict[PositionKey, Position] = {}
        self._snapshots: dict[uuid.UUID, InstructionSnapshot] = {}
        self._checkpoint: int = NO_CHECKPOINT

        #: How often `commit` was handed a checkpoint that would have moved backwards.
        #:
        #: Counted rather than raised, because a regression is not this layer's problem to diagnose:
        #: the checkpoint is a high-water mark, and clamping it is the correct response. What makes
        #: the counter worth keeping is that a non-zero value here means events are arriving out of
        #: `event_id` order, and the whole resume-from-checkpoint design assumes they do not. See
        #: DECISIONS.md for the walkthrough of what that costs.
        self.checkpoint_regressions = 0

    async def checkpoint(self) -> int:
        return self._checkpoint

    async def snapshot_for(self, instruction_id: uuid.UUID) -> InstructionSnapshot | None:
        return self._snapshots.get(instruction_id)

    async def position_for(self, key: PositionKey) -> Position | None:
        return self._positions.get(key)

    async def all_positions(self) -> tuple[Position, ...]:
        # Sorted by key so that a caller comparing two stores — which is exactly what the replay
        # idempotence test does — compares content and not dict insertion order. Two stores fed the
        # same events in the same order would agree anyway; two stores fed the same events in a
        # *different* order hold identical positions in a different insertion sequence, and without
        # the sort the test would fail for a reason that is not a bug.
        return tuple(self._positions[key] for key in sorted(self._positions, key=str))

    async def snapshot_count(self) -> int:
        return len(self._snapshots)

    async def commit(self, batch: WriteBatch) -> None:
        """Apply a batch. No `await` in here, on purpose — see the class docstring."""
        if batch.checkpoint < self._checkpoint:
            self.checkpoint_regressions += 1

        for position in batch.positions:
            self._positions[position.key] = position
        if batch.snapshot is not None:
            self._snapshots[batch.snapshot.instruction_id] = batch.snapshot

        # `max` rather than assignment. The checkpoint means "everything up to here has been
        # considered", so an event arriving late must not un-consider the ones already handled.
        self._checkpoint = max(self._checkpoint, batch.checkpoint)
