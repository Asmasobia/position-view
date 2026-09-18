# position-view

A CQRS read model over a settlement event stream, built to demonstrate the one thing that read-model
tutorials leave out: **a projection that aggregates a state machine has to remember the per-item state
it is aggregating away.**

The interesting question is not "how do I subscribe to a topic". It is this:

> An `instruction.settled` event arrives carrying `2000.00`. Your projection currently counts that
> instruction as pending. How much do you subtract from the pending bucket?

If the answer is "2000.00", the projection is wrong, and it is wrong in a way no test of a single event
will find — because the instruction may have been amended since it was counted, and **the event does not
know what this consumer previously counted.** The residue is exactly the amendment delta, it stays
forever, and nothing downstream can detect it: `pending = 500.00` is a perfectly plausible number.

The fix is one small piece of state and two different sources of truth per transition. It is written up
in full in [ADR-007](DECISIONS.md#adr-007--the-projection-stores-the-per-aggregate-state-it-is-aggregating-away),
and pinned by a test that its own docstring labels the most important one in the repository.

## Status

**Slices 1 and 2 of 5 complete** — the pure read-model core, plus the projector that owns a unit of
work and a checkpoint. The service now folds a real stream idempotently. No HTTP and no fan-out yet.

| | Slice | State |
|---|---|---|
| 1 | Event contract, money, bucket state machine, pure `project` | ✅ done — 382 tests, 41/41 mutants killed |
| 2 | Projector + store protocol: atomic apply-and-checkpoint, replay | ✅ done — 444 tests, 32/32 mutants killed |
| 3 | Subscription fan-out: bounded per-subscriber queues, conflation | ⬜ planned |
| 4 | Read-through cache: single-flight, negative caching, invalidation | ⬜ planned |
| 5 | HTTP surface: FastAPI, read-your-writes, SSE, health and lag | ⬜ planned |

Both slices so far have no runtime dependencies at all — the whole thing is importable and testable
with nothing installed but the toolchain, and it will run end to end against `InMemoryPositionStore`.
FastAPI arrives in slice 5, and `tests/test_layering.py` fails the build if it drifts inward before
then.

### What slice 2 added

- **`PositionStore`, a protocol with exactly one write method.** `commit(WriteBatch)`, and nothing
  else. Atomicity is expressed as a type rather than as a rule, so the three-call sequence that leaves
  money counted in one bucket and remembered in another is unwritable rather than merely discouraged
  ([ADR-020](DECISIONS.md#adr-020--the-store-protocol-has-exactly-one-write-method)).
- **A checkpoint that is a high-water mark**, not "the last event handled", with a counter for the
  batch that would have moved it backwards — because clamping is right and clamping *silently* destroys
  the only evidence that delivery order broke
  ([ADR-024](DECISIONS.md#adr-024--the-checkpoint-is-a-high-water-mark-and-a-regression-is-counted-rather-than-raised)).
- **One lock across read → decide → write → notify.** With an async store, applying an event is
  read-modify-write across suspension points, and the lost update it invites is silent in both
  directions: money missing, or one instruction counted twice
  ([ADR-022](DECISIONS.md#adr-022--one-asynciolock-is-held-across-read--project--commit--notify)).
- **Counters that only move after the commit returns**, so a failing database produces a flat line
  instead of a rising one
  ([ADR-025](DECISIONS.md#adr-025--every-counter-is-incremented-after-the-commit-returns-and-a-refusal-is-the-exception)).
- **Two raw facts instead of a metric called "lag"**, since `now - last_event_at` is staleness and
  grows by a second per second on an idle stream
  ([ADR-028](DECISIONS.md#adr-028--the-projection-publishes-staleness-and-a-watermark-never-a-number-called-lag)).

## Quick start

Requires Python 3.12+.

```bash
git clone <this repo> && cd position-view
python -m pip install -e ".[dev]"      # pytest, pytest-asyncio, ruff, mypy, httpx
python -m pytest                       # 444 tests, well under a second — none of them sleep
python -m ruff check . && python -m ruff format --check .
python -m mypy --strict src tests
```

There is nothing to serve yet. Slice 5 adds the HTTP surface and a `[serve]` extra with `uvicorn`.

## The shape

```
model/        pure: values, the state machine, the projection step. Standard library only.
projection/   the store protocol and the loop that owns a unit of work and a checkpoint.
push/         the fan-out hub: snapshot then stream, per subscriber.  (slice 3)
cache/        read-through, single-flight, invalidated by version.    (slice 4)
api/          FastAPI, SSE, health. The only place a framework appears. (slice 5)
```

The arrow runs one way, left to right, and it is enforced by a test that parses the AST rather than by
a paragraph asking nicely — see [ADR-018](DECISIONS.md#adr-018--the-architecture-is-enforced-by-a-test-that-parses-the-source).
`model/` may not import a framework, a driver, a clock, or randomness; `api/` may import everything.

### What a position is

One position per `(counterparty, currency)` pair — currency is part of the **identity**, not a field,
so "add dollars to yen" is not expressible rather than merely discouraged
([ADR-006](DECISIONS.md#adr-006--currency-is-part-of-the-position-key-not-a-field-on-the-position)).
Each position holds four buckets — pending, settled, failed, reversed — and each bucket holds both an
amount and a count, because one 4.2m trade and eight hundred small ones are the same exposure and very
different situations.

### The five reasons an event is declined

`project` returns `Applied | Skipped`, never raises for these, and the caller advances its checkpoint
either way:

| Reason | What it means | Who cares |
|---|---|---|
| `NOT_OUR_AGGREGATE` | A `payment.*` event on a shared topic | nobody |
| `UNKNOWN_EVENT_TYPE` | An instruction event this view does not read | nobody |
| `DUPLICATE` | At-least-once delivery working as designed | nobody |
| `OUT_OF_ORDER` | `event_id` went backwards — a guarantee broke | **a human** |
| `ILLEGAL_TRANSITION` | The write model emitted a sequence this view calls impossible | **a human** |

The order those checks run in is a design decision, not an accident: ordering is checked *before*
legality, so a redelivered `settled` for an already-settled instruction is reported as `DUPLICATE`
(healthy) rather than as `ILLEGAL_TRANSITION` (sends someone to read the producer's state machine).
Same event, same drop, two very different mornings —
[ADR-011](DECISIONS.md#adr-011--the-order-of-checks-inside-project-is-part-of-the-design).

## Design decisions

[DECISIONS.md](DECISIONS.md) has one ADR per decision a reviewer might reasonably have made
differently, each with the rejected alternative and the cost accepted. The ones worth reading first:

- **[ADR-007](DECISIONS.md)** — the projection stores the per-aggregate state it aggregates away. The
  reason this repository exists.
- **[ADR-003](DECISIONS.md)** / **[ADR-005](DECISIONS.md)** — ordering and dedup come from `event_id`,
  never a timestamp; and the precondition that makes that valid holds for *one* producer, which is
  enforced as an outcome rather than assumed.
- **[ADR-004](DECISIONS.md)** — money is `Decimal` at a fixed exponent, and normalisation **refuses to
  round**. Includes the verified stdlib fact that `Decimal.quantize` rounds silently by default, which
  is why the refusal needs a `Context` that traps `Inexact`.
- **[ADR-009](DECISIONS.md)** — a version bump means the content changed, so a no-op transition returns
  the *same object*. Everything in slices 3–5 is built on that.
- **[ADR-008](DECISIONS.md)** — "skipped" is a returned value, because the version of the exception-based
  loop that forgets to advance the checkpoint reprocesses the same event forever.
- **[ADR-020](DECISIONS.md)** — the store has one write method, so a partial write is not a bug to avoid
  but a state the interface cannot express. Includes what each half of a partial write actually costs.
- **[ADR-023](DECISIONS.md)** — the one that is worth reading even if you never look at this code again:
  a concurrency test against an in-memory double proves nothing, because awaiting a coroutine that
  never blocks is not a suspension point. Found by mutation testing, not by review.
- **[ADR-026](DECISIONS.md)** — an undecodable message does not advance the checkpoint, and the choice
  between dead-lettering it and stalling the partition is handed back to the caller rather than made
  silently by a `try/except` that logs.

## Verification

| Gate | Result |
|---|---|
| `pytest` | 444 passed, **0 skipped**, ~0.6s |
| `ruff check .` | clean (`E W F I B UP ASYNC RUF`, line length 100) |
| `ruff format --check .` | clean |
| `mypy --strict src tests` | clean, including `warn_unreachable` |
| Mutation matrix, slice 1 | **41 / 41 killed**, no survivors |
| Mutation matrix, slice 2 | **32 / 32 killed**, no survivors — one on the second attempt, see below |

### The mutation matrix

A passing test suite proves the tests run, not that they check anything. So each slice is verified by
introducing changes to the source — each one a mistake a careless author could plausibly have made —
and confirming the suite fails for every one of them. Slice 1's 41 mutations were single-token
arithmetic and validation swaps. Among them:

| Mutation | First test that catches it | Total failures |
|---|---|---:|
| The undo half uses the **event's** amount instead of the stored one | `test_the_undo_half_uses_the_stored_amount_not_the_events` | 3 |
| The undo half uses the event's **bucket** instead of the stored one | `test_a_settlement_moves_money_between_buckets` | 5 |
| `Inexact` is not trapped, so `quantize` rounds silently | `test_excess_precision_is_refused_rather_than_rounded` | 5 |
| `Rounded` *is* trapped, so `"1500.0000"` is wrongly refused | `test_normalisation_is_about_spelling_not_value` | 3 |
| A no-op returns an equal copy instead of the **same object** | `test_a_net_zero_transition_returns_the_same_object` | 3 |
| The ordering check becomes inclusive, so a duplicate reports as out-of-order | `test_an_older_event_is_out_of_order_and_not_a_duplicate` | 4 |
| `event_id` 0 is accepted, colliding with the empty-checkpoint sentinel | `test_zero_and_negatives_are_refused[0]` | 1 |
| A cross-position move emits one change instead of two | `test_a_corrected_counterparty_produces_two_changes` | 5 |

Slice 2's claims are of a different kind — atomicity, ordering, counter honesty, notification order —
so its 32 mutations are mostly *structural*: the changes somebody makes while tidying up.

| Mutation | What it breaks | Caught by |
|---|---|---|
| `async with self._lock` deleted | Concurrent events on one position lose updates silently | `test_the_lock_is_what_makes_a_suspending_store_safe` |
| The counters move **before** `commit` | A retrying consumer makes a failing database look busy | `test_a_failed_commit_leaves_no_trace_in_the_store_or_the_counters` |
| The listener is called **before** `commit` | Subscribers are told about a version that then fails to exist | the listener's own `durable is position` assertion |
| `max(...)` becomes assignment on the checkpoint | A late event rewinds progress; every lag graph is meaningless | `test_the_checkpoint_never_moves_backwards` |
| `<` becomes `<=` on the regression check | Every ordinary duplicate reports a broken ordering guarantee | `test_re_committing_the_same_checkpoint_is_not_a_regression` |
| A skipped event commits no checkpoint | A poison event is re-read forever, and lag climbs in a straight line | `test_a_skipped_event_still_advances_the_checkpoint` |
| `is not` becomes always-true in the fold | An unchanged row is rewritten and every subscriber woken for nothing | `test_a_no_op_transition_advances_the_snapshot_and_writes_no_position` |
| The duplicate-key guard never fires | Half a cross-position move is discarded with no evidence | `test_a_batch_touching_one_position_twice_is_refused` |
| A miss returns an empty `Position` instead of `None` | Every misspelt counterparty looks real, with nothing outstanding | `test_an_untouched_position_is_absent_rather_than_zeroed` |

**One of those 32 survived the first run, and it was the most important one.** Deleting the lock left
all 441 tests green — including the two named "concurrent". The reason is a fact about asyncio worth
carrying to other codebases: **awaiting a coroutine is not a suspension point.** `await` on a coroutine
that never itself blocks runs start to finish on the current task without a trip through the event loop,
and `InMemoryPositionStore` is three dict operations with no `await` in them. So no test using it could
possibly observe the lock — those two were pinning the fold under `asyncio.gather`, which is worth
pinning and is not what they claimed. The fix was a second double whose every method yields
(`asyncio.sleep(0)`, the minimal faithful model of a database round trip) and three tests over it; the
mutation is now killed, and the whole episode is written up in
[ADR-023](DECISIONS.md#adr-023--a-test-double-that-suspends-because-the-shipped-store-does-not).

That is the argument for mutation testing in one paragraph. Review would not have caught it — the tests
looked right, were named right, and passed.

The matrix is a throwaway script rather than a committed tool: it answers "are these tests
load-bearing?" once per slice, and a committed mutation runner would be a second thing to maintain for
a question that is only asked at the end of a slice.

**One thing worth passing on, because the first run of the matrix was wrong.** CPython validates a
`.pyc` against the source's `(mtime, size)`. Almost every mutation here is a single-token swap — `< 1`
for `< 0`, `snapshot.amount` for `envelope.amount` — which leaves the file *exactly the same length*.
Restoring the original within the same clock second therefore leaves bytecode compiled from the
**mutated** source looking valid. The symptom was a test failing on a provably clean tree; the real
danger is the other direction, since a mutant judged against the previous run's bytecode can be
reported as killed when it actually survived. The numbers above were produced with `python -B`,
`PYTHONDONTWRITEBYTECODE=1`, and `__pycache__` purged before every run.


Test count by file:

| File | Tests | What it pins |
|---|---:|---|
| `test_events.py` | 127 | The wire contract: what is accepted, what is dropped, what is refused |
| `test_project.py` | 84 | The transition arithmetic, dedup, check order, and a stream fold |
| `test_positions.py` | 72 | The 16-pair transition table, the version rule, the identity contract |
| `test_money.py` | 61 | Exactness, refusals, and the stdlib behaviour the module works around |
| `test_layering.py` | 44 | The dependency arrow and the purity of `model/` |
| `test_projector.py` | 35 | Atomicity, the lock, counter honesty, notification order, replay |
| `test_store.py` | 21 | One unit of work, the high-water checkpoint, and the shape of the protocol |

**Zero skips is a deliberate property, not a coincidence.** An earlier version of the invariant tests
reported "83 passed, 34 skipped" because it called `pytest.skip` on a condition computed by the function
under test — so a rule regression would have converted real assertions into skips and stayed green. That
was replaced with an explicit data table plus a completeness cross-check
([ADR-017](DECISIONS.md#adr-017--no-test-may-skip-on-a-condition-computed-by-the-code-under-test)).

Three facts in this repository were found by tests rather than by reasoning, and all three are recorded
where they were fixed rather than tidied away:

- `Decimal.quantize` rounds silently under the default context. The module's docstring had claimed the
  opposite; the implementation was rewritten around a trapping `Context`. This was a genuine money
  correctness bug.
- `1 == 1.0` and `1 == True` are both true, so a bare `!=` accepted `{"schema_version": 1.0}` and
  `{"schema_version": true}` as compatible versions. The source was fixed and the test that caught it
  is named after having caught it.
- Awaiting a coroutine that never blocks does not yield to the event loop, so two "concurrency" tests
  written against the in-memory store could not see the lock they were named after. The source was
  right; the tests were not. A suspending double fixed them.

## Not what this is

Not a settlement engine — it consumes events, it does not produce them, and it deliberately holds no
opinion about whether a settlement was correct. Not a general-purpose CQRS framework: there is one
aggregate type, one read model, and every decision is made concretely for that case, because the
concrete version is the one where the reasoning is visible.

## Licence

MIT — see [LICENSE](LICENSE).
