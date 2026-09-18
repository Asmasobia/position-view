"""The store contract: one unit of work, a high-water checkpoint, and no way to write half of it.

WHAT THESE TESTS ARE ACTUALLY DEFENDING
---------------------------------------------------------------------------------
Not "does a dict store things". The dict works. What is worth testing is the set of properties the
projector *assumes* and could not detect the loss of:

  - a position that has never been touched reads as absent, not as zeroed (the api layer's 404)
  - a batch is all-or-nothing, and there is no method that could write part of one
  - the checkpoint never moves backwards, however the events arrive
  - a batch that touches the same position twice is refused rather than silently half-applied

Three of the four are properties of the *interface* rather than of the implementation, so three of
these tests read the source or the protocol rather than calling a method. That is deliberate: a
guarantee that lives in the shape of the type is one a future implementation inherits for free, and
these tests are what stop the shape from being widened by a convenient one-line addition.
"""

from __future__ import annotations

import ast
import inspect
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

from builders import KEY, OCCURRED_AT, OTHER_KEY, instruction_id, make_snapshot
from position_view.model import Bucket, BucketDelta, Position, PositionKey
from position_view.projection import (
    NO_CHECKPOINT,
    InMemoryPositionStore,
    PositionStore,
    StoreError,
    WriteBatch,
)

# Every test in this module is a coroutine, and `asyncio_mode = "strict"` requires the marker.
# Applied once at module level rather than on twenty functions: a per-function marker is a line
# somebody forgets, and a forgotten `@pytest.mark.asyncio` on an async test does not fail — the
# coroutine is never awaited, pytest warns, and the test PASSES having asserted nothing.
pytestmark = pytest.mark.asyncio


#: The complete write-facing and read-facing surface of `PositionStore`.
#:
#: Written out so that adding a method is a decision made here, in a test whose name says why the
#: surface is small, rather than a two-line addition to a protocol nobody re-reads.
EXPECTED_STORE_SURFACE = {
    "all_positions",
    "checkpoint",
    "commit",
    "position_for",
    "snapshot_count",
    "snapshot_for",
}


def _store_source() -> str:
    """The store module's own source text, read once at import time.

    Read here rather than inside the test that parses it, because `pathlib` I/O inside an
    `async def` blocks the event loop and `flake8-async` is right to flag it. Inside a test the
    consequence is nil; obeying the rule anyway is cheaper than teaching every reader which
    blocking calls are the harmless ones, and the identical line in a request handler is a
    stalled server.
    """
    path = inspect.getsourcefile(InMemoryPositionStore)
    assert path is not None, "InMemoryPositionStore has no source file"
    return Path(path).read_text(encoding="utf-8")


_STORE_SOURCE = _store_source()


def _delta(bucket: Bucket, amount: str, count: int) -> BucketDelta:
    """A delta built from a string amount, so no test here constructs a float by accident."""
    return BucketDelta(bucket=bucket, amount=Decimal(amount), count=count)


def _position(key: PositionKey, amount: str, count: int = 1) -> Position:
    """A position with `amount` pending, built through the real fold rather than by hand.

    `Position(key=key, pending=Totals(...))` would be shorter and would let these tests store a
    position whose `version` is 0 while its buckets are non-empty — a state the projector can never
    produce. Going through `with_deltas` means every position in this file is reachable.
    """
    at = datetime.fromisoformat(OCCURRED_AT)
    return Position(key=key).with_deltas((_delta(Bucket.PENDING, amount, count),), at=at)


# ---------------------------------------------------------------------------
# The empty store
# ---------------------------------------------------------------------------


async def test_a_fresh_store_has_considered_nothing() -> None:
    store = InMemoryPositionStore()

    assert await store.checkpoint() == NO_CHECKPOINT
    assert await store.all_positions() == ()
    assert await store.snapshot_count() == 0


async def test_an_untouched_position_is_absent_rather_than_zeroed() -> None:
    """⚠️ THE DISTINCTION THE API LAYER'S 404 IS BUILT ON, AND THE ONE SLICE 4 CACHES NEGATIVELY.

    A store that manufactured `Position(key=key)` on a miss would be more convenient for the
    projector — no None check in the fold — and it would make every misspelt counterparty look like
    a real one with no outstanding exposure. "ACME-BNK has nothing pending" is a far worse answer
    than "there is no such counterparty", because the first is a number somebody can act on.
    """
    store = InMemoryPositionStore()

    assert await store.position_for(KEY) is None


async def test_an_unknown_instruction_has_no_snapshot() -> None:
    """None is what makes `project` treat the event as a first sighting, not a duplicate."""
    store = InMemoryPositionStore()

    assert await store.snapshot_for(instruction_id(1)) is None


# ---------------------------------------------------------------------------
# Committing
# ---------------------------------------------------------------------------


async def test_a_commit_writes_the_position_the_snapshot_and_the_checkpoint() -> None:
    store = InMemoryPositionStore()
    snapshot = make_snapshot(last_event_id=7)

    await store.commit(
        WriteBatch(checkpoint=7, snapshot=snapshot, positions=(_position(KEY, "1500.75"),))
    )

    stored = await store.position_for(KEY)
    assert stored is not None
    assert stored.pending.amount == Decimal("1500.75")
    assert await store.snapshot_for(snapshot.instruction_id) == snapshot
    assert await store.checkpoint() == 7
    assert await store.snapshot_count() == 1


async def test_a_checkpoint_only_batch_is_a_valid_unit_of_work() -> None:
    """The shape a skipped event commits: progress, and nothing else.

    Requiring a write to justify a commit would look tidier and would leave the projector with no
    way to advance past an event it declined — so the poison event is re-read forever and the only
    symptom is lag climbing in a straight line.
    """
    store = InMemoryPositionStore()

    await store.commit(WriteBatch(checkpoint=42))

    assert await store.checkpoint() == 42
    assert await store.all_positions() == ()
    assert await store.snapshot_count() == 0


async def test_a_batch_may_carry_a_snapshot_with_no_position_write() -> None:
    """The no-op transition's shape: the instruction moved on, the numbers did not.

    `validated` following `received` puts the same money back in the same bucket, so no position row
    changes — but the instruction's `last_event_id` must advance or the next event is misread. A
    batch that required at least one position would make this case unrepresentable and force the
    projector to write an unchanged row just to have something to commit.
    """
    store = InMemoryPositionStore()
    snapshot = make_snapshot(last_event_id=3)

    await store.commit(WriteBatch(checkpoint=3, snapshot=snapshot))

    assert await store.snapshot_count() == 1
    assert await store.all_positions() == ()


async def test_both_sides_of_a_cross_position_move_land_in_one_commit() -> None:
    """⚠️ THE ATOMICITY CASE THAT MATTERS MOST, BECAUSE IT IS THE ONE WITH TWO ROWS.

    An instruction whose counterparty is corrected leaves one position and arrives at another.
    Commit those as two units of work and a crash between them leaves the money counted in both — a
    total that is over by exactly one instruction, in a report nobody can reconcile, with no error
    anywhere. One batch makes the interleaving unrepresentable.
    """
    store = InMemoryPositionStore()

    await store.commit(
        WriteBatch(
            checkpoint=9,
            snapshot=make_snapshot(key=OTHER_KEY, last_event_id=9),
            positions=(_position(KEY, "500.00"), _position(OTHER_KEY, "1500.75")),
        )
    )

    assert len(await store.all_positions()) == 2


async def test_a_later_commit_replaces_a_position_rather_than_accumulating() -> None:
    """The store holds state, not a log. The fold happened in the model; this is its result."""
    store = InMemoryPositionStore()

    await store.commit(WriteBatch(checkpoint=1, positions=(_position(KEY, "100.00"),)))
    await store.commit(WriteBatch(checkpoint=2, positions=(_position(KEY, "250.00"),)))

    stored = await store.position_for(KEY)
    assert stored is not None
    assert stored.pending.amount == Decimal("250.00")


# ---------------------------------------------------------------------------
# The checkpoint is a high-water mark
# ---------------------------------------------------------------------------


async def test_the_checkpoint_never_moves_backwards() -> None:
    """⚠️ `max`, NOT ASSIGNMENT, AND THE REASON IS WHAT "CHECKPOINT" MEANS.

    It is not "the last event I handled". It is "every event up to here has been considered", which
    is the only reading that makes resuming from it safe. An event arriving late must not
    un-consider the ones already handled: assignment would rewind the checkpoint to 3, and a restart
    would then re-deliver events 4 through 9 — harmless, since the snapshots deduplicate them, but
    it makes the projection's progress non-monotonic and every lag graph built on it meaningless.
    """
    store = InMemoryPositionStore()

    await store.commit(WriteBatch(checkpoint=9))
    await store.commit(WriteBatch(checkpoint=3))

    assert await store.checkpoint() == 9


async def test_a_backwards_checkpoint_is_counted_because_it_should_never_happen() -> None:
    """Clamping silently would hide the only evidence that delivery order has broken.

    The counter is not an error path — the clamp is the correct response — it is the fact that the
    assumption underneath resume-from-checkpoint is no longer true. A non-zero value here means an
    event was applied *after* the checkpoint had already passed it, and a crash at that moment loses
    it permanently, because replay starts from the checkpoint and will never deliver it again.
    """
    store = InMemoryPositionStore()

    await store.commit(WriteBatch(checkpoint=9))
    assert store.checkpoint_regressions == 0

    await store.commit(WriteBatch(checkpoint=3))
    assert store.checkpoint_regressions == 1


async def test_re_committing_the_same_checkpoint_is_not_a_regression() -> None:
    """Equal is not backwards. Counting it would make every duplicate delivery look like a broken
    guarantee, and duplicates are the normal, healthy case on an at-least-once stream."""
    store = InMemoryPositionStore()

    await store.commit(WriteBatch(checkpoint=5))
    await store.commit(WriteBatch(checkpoint=5))

    assert store.checkpoint_regressions == 0
    assert await store.checkpoint() == 5


# ---------------------------------------------------------------------------
# WriteBatch validates itself
# ---------------------------------------------------------------------------


async def test_a_batch_touching_one_position_twice_is_refused() -> None:
    """⚠️ REFUSED AT CONSTRUCTION, NOT AT THE STORE, BECAUSE THE BUG IS IN THE CALLER.

    Two entries for the same key means the fold produced two changes where it should have produced
    one — a cross-position move whose "old" and "new" keys are equal, most likely. Whichever entry
    is written second wins, so the position ends up short by exactly one delta with nothing to
    indicate it. Raising here names the caller; letting the store overwrite names nobody.
    """
    with pytest.raises(StoreError, match=r"once"):
        WriteBatch(
            checkpoint=1,
            positions=(_position(KEY, "100.00"), _position(KEY, "200.00")),
        )


async def test_the_refusal_names_the_repeated_position() -> None:
    """A message saying "duplicate key" and nothing else sends somebody to a debugger."""
    with pytest.raises(StoreError, match=r"ACME-BANK/USD"):
        WriteBatch(checkpoint=1, positions=(_position(KEY, "1.00"), _position(KEY, "2.00")))


async def test_two_different_positions_in_one_batch_are_fine() -> None:
    """The guard must not catch the case it exists to serve. This is the cross-position move."""
    batch = WriteBatch(
        checkpoint=1, positions=(_position(KEY, "1.00"), _position(OTHER_KEY, "2.00"))
    )

    assert len(batch.positions) == 2


async def test_a_negative_checkpoint_is_refused() -> None:
    """`NO_CHECKPOINT` is zero, so a negative value is not "even emptier" — it is a bug."""
    with pytest.raises(StoreError, match=r"negative"):
        WriteBatch(checkpoint=-1)


async def test_the_batch_repr_names_its_positions() -> None:
    """A batch appears in assertion failures and in poison-message diagnostics.

    The default `<WriteBatch object at 0x7f...>` turns every one of those into a debugger session,
    and the positions are the part anybody looking at a batch actually wants.
    """
    batch = WriteBatch(checkpoint=4, positions=(_position(KEY, "1.00"),))

    assert "checkpoint=4" in repr(batch)
    assert "ACME-BANK/USD" in repr(batch)


# ---------------------------------------------------------------------------
# The guarantees that live in the shape rather than in the behaviour
# ---------------------------------------------------------------------------


async def test_the_protocol_offers_exactly_one_way_to_write() -> None:
    """⚠️ THE ATOMICITY GUARANTEE, EXPRESSED AS A TYPE AND PINNED BY A TEST.

    `commit` is the only mutating method. Add `save_position` for the convenience of a rebuild
    script and the dangerous sequence becomes writable:

        await store.save_position(p)   # crash here
        await store.save_snapshot(s)

    which leaves the projection counting money in a position while its memory of the instruction
    still says the money is elsewhere. The next event undoes the old bucket a second time and the
    projection is wrong in two buckets at once, permanently, with no error raised.

    This test is the review comment that would otherwise have to be made by a person who happened to
    be paying attention.
    """
    surface = {name for name in vars(PositionStore) if not name.startswith("_")}

    assert surface == EXPECTED_STORE_SURFACE, (
        "the PositionStore surface changed. A new *read* is fine — add it to "
        "EXPECTED_STORE_SURFACE. A new *write* means atomicity is no longer structural."
    )


async def test_the_in_memory_store_satisfies_the_protocol() -> None:
    """A real assignment, so mypy checks the fit, plus a runtime sweep for the readable failure.

    The annotation alone would be enough for `mypy --strict` and would produce nothing at all when
    the suite is run by somebody who has not installed mypy. The loop below is what turns
    "structurally incompatible" into a message naming the missing method.
    """
    store: PositionStore = InMemoryPositionStore()

    for name in EXPECTED_STORE_SURFACE:
        assert callable(getattr(store, name, None)), f"InMemoryPositionStore is missing {name}()"


async def test_commit_does_not_suspend_which_is_what_makes_it_atomic() -> None:
    """⚠️ THE IN-MEMORY STORE'S ATOMICITY IS "NO `await` IN `commit`", AND NOTHING ELSE ENFORCES IT.

    A coroutine yields control only at an `await`. With none in `commit`, no other task can run
    between the first mutation and the last, so no reader can observe a half-applied batch — the
    same guarantee a SQL transaction gives, obtained from the event loop's cooperative scheduling
    instead.

    Add one `await` — an audit write, a metric push, an `asyncio.sleep(0)` someone inserted to
    "yield fairly" — and the guarantee is gone while every other test in this file still passes,
    because catching it requires two commits interleaved at exactly that suspension point. So the
    property is checked where it actually lives: in the source.

    Parsing rather than reading `inspect.getsource` line by line, because `await` also appears
    inside strings and comments, and a substring search would be defeated by this very docstring.
    """
    tree = ast.parse(_STORE_SOURCE)

    commits = [
        node
        for klass in ast.walk(tree)
        if isinstance(klass, ast.ClassDef) and klass.name == "InMemoryPositionStore"
        for node in klass.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "commit"
    ]
    # Guards the assertion below against vacuity: a rename would otherwise leave an empty list and a
    # cheerfully passing test for a rule that was never checked.
    assert len(commits) == 1, "InMemoryPositionStore.commit not found — has it been renamed?"

    suspensions = [node for node in ast.walk(commits[0]) if isinstance(node, ast.Await)]
    assert suspensions == [], (
        "InMemoryPositionStore.commit now suspends, so a reader can observe a half-applied batch. "
        "Either remove the await or give the store a real lock."
    )


async def test_all_positions_returns_a_copy_and_not_a_live_view() -> None:
    """A caller iterating the store while the projector commits must not see the write land.

    The stronger version of this — an iterator over the store's own mapping — would raise
    `RuntimeError: dictionary changed size during iteration` in production and never in a test,
    because a test rarely commits while reading. A tuple snapshot costs one list build per call and
    removes the whole class of problem.
    """
    store = InMemoryPositionStore()
    await store.commit(WriteBatch(checkpoint=1, positions=(_position(KEY, "1.00"),)))

    before = await store.all_positions()
    await store.commit(WriteBatch(checkpoint=2, positions=(_position(OTHER_KEY, "2.00"),)))

    assert len(before) == 1
    assert len(await store.all_positions()) == 2


async def test_all_positions_is_ordered_by_key() -> None:
    """Insertion order would make the replay-idempotence comparison depend on arrival order.

    Two stores fed the same events in a different order hold identical positions in a different dict
    sequence. Without the sort, comparing them tuple-to-tuple fails for a reason that is not a bug —
    and the obvious "fix" is to weaken the comparison, which is how an idempotence test stops
    testing idempotence.
    """
    store = InMemoryPositionStore()

    await store.commit(WriteBatch(checkpoint=1, positions=(_position(OTHER_KEY, "2.00"),)))
    await store.commit(WriteBatch(checkpoint=2, positions=(_position(KEY, "1.00"),)))

    assert [str(position.key) for position in await store.all_positions()] == [
        "ACME-BANK/USD",
        "BETA-TRUST/USD",
    ]
