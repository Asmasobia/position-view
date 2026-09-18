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

**All five slices complete.** The pure read-model core, the projector that owns a unit of work and a
checkpoint, the fan-out hub that carries changes to live subscribers, a read-through cache kept
coherent by the change stream rather than by a timeout, and an HTTP surface with read-your-writes and
a live SSE feed. The service folds a real stream idempotently, pushes the result to any number of
watchers without one of them being able to slow it down, serves repeat reads — including reads for
counterparties that do not exist — without touching the store, and answers "have you caught up with my
write yet?" with either the fresh answer or a 503, never with a stale 200.

| | Slice | State |
|---|---|---|
| 1 | Event contract, money, bucket state machine, pure `project` | ✅ done — 382 tests, 41/41 mutants killed |
| 2 | Projector + store protocol: atomic apply-and-checkpoint, replay | ✅ done — 444 tests, 32/32 mutants killed |
| 3 | Subscription fan-out: conflating per-subscriber buffers, bounded | ✅ done — 502 tests, 43/43 mutants killed |
| 4 | Read-through cache: single-flight, negative caching, invalidation | ✅ done — 573 tests, 57/57 mutants killed |
| 5 | HTTP surface: FastAPI, read-your-writes, SSE, health and lag | ✅ done — 679 tests, 97/97 mutants killed |

The first four slices have no runtime dependencies at all — that part is importable and testable with
nothing installed but the toolchain, and it runs end to end against `InMemoryPositionStore`. FastAPI
appears in slice 5 and only inside `api/`, which `tests/test_layering.py` enforces by parsing the
source: `model/` importing a framework fails the build, not a review.

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

### What slice 3 added

- **A fan-out whose `publish` contains no `await` — enforced by parsing the source.** It runs under the
  projector's lock, so an `await` on a subscriber's socket there stops the entire projection until that
  one client's TCP buffer drains. Every behavioural test in the file would still pass with the bug in
  place, because in-process doubles never block, so the guarantee is checked structurally
  ([ADR-030](DECISIONS.md#adr-030--the-fan-outs-publish-contains-no-await-and-a-test-parses-the-source-to-keep-it-so)).
- **A conflating buffer per subscriber, not a queue.** Keyed by position, so a newer version replaces an
  older one and a subscriber's memory is bounded by *how many positions it watches* rather than by the
  event rate. A thousand updates to one position leave one entry. Only legitimate because the payload is
  versioned state and not an event stream
  ([ADR-031](DECISIONS.md#adr-031--each-subscriber-gets-a-conflating-buffer-keyed-by-position-not-a-queue-of-events)).
- **Overflow disconnects instead of degrading.** A trimmed buffer is delivered as though it were
  complete, which freezes the client's view of the dropped positions with nothing to indicate it — wrong
  and confident. A client that knows it lost data re-snapshots and is right again
  ([ADR-032](DECISIONS.md#adr-032--overflow-disconnects-the-subscriber-rather-than-delivering-a-subset)).
- **The snapshot-then-stream handover, done in the only order that works.** Subscribe first (so nothing
  slips through the gap), then snapshot, then `mark_delivered(snapshot)` to suppress what the client
  already has. Both obvious orderings are broken — one by a gap, one by duplicates — and there is a
  negative-control test asserting the duplicate appears when the call is removed
  ([ADR-033](DECISIONS.md#adr-033--a-subscription-is-registered-before-the-snapshot-is-read-and-mark_delivered-closes-the-gap)).
- **`keys=None` means every position; `keys=[]` means none.** An empty list is what a caller produces by
  accident, and the accident-shaped value gets the restrictive reading
  ([ADR-035](DECISIONS.md#adr-035--keysnone-means-every-position-and-keys-means-none)).

### What slice 4 added

- **A cache that deliberately cannot be substituted for the store, pinned by a test.** Five passthrough
  methods would make `CachedPositions` satisfy `PositionStore`, and then `Projector(CachedPositions(store))`
  compiles — a projector folding deltas onto a stale base and committing the result as truth. No counter
  moves, no exception is raised, and the projector's lock cannot help, because the stale value did not
  come from inside it
  ([ADR-038](DECISIONS.md#adr-038--the-cache-is-deliberately-not-a-positionstore-and-a-test-pins-its-public-surface)).
- **Single-flight built on a task plus `asyncio.shield`.** Fifty simultaneous misses on one key become
  one store read. The `shield` is the part worth copying: with a bare `await` on the shared load, one
  HTTP client disconnecting cancels the load for every other waiter — intermittent, load-dependent,
  concentrated on the most popular keys, and invisible to any sequential test
  ([ADR-040](DECISIONS.md#adr-040--single-flight-is-a-task-register-plus-asyncioshield-not-a-bare-await)).
- **Absences are cached, which makes `dict.get` unusable for detecting a miss.** Without negative
  caching, the cheapest way to load this service is to ask about counterparties that do not exist.
  Caching `None` as a *value* fixes that and creates a trap that reads as tidy code: `.get(key)` returns
  `None` for both "cached absence" and "not cached", so it silently disables the feature while the
  counters keep incrementing
  ([ADR-041](DECISIONS.md#adr-041--an-absence-is-cached-as-a-value-which-makes-dictget-unusable-for-miss-detection)).
- **No clock, no TTL — coherence comes from the change stream.** The projector already publishes every
  changed position with its new value, so entries are updated in place and compared by `version`. A TTL
  is what you use when you cannot know when the value changed; given a precise signal it is strictly
  worse on both axes at once — staler than necessary *and* more store reads than necessary
  ([ADR-043](DECISIONS.md#adr-043--invalidation-comes-from-the-change-stream-by-version-there-is-no-clock-and-no-ttl)).
- **A refresh updates, never inserts, and never promotes.** A cache is populated by reads and updated by
  writes; never populated by writes. Inserting on notification hands the event stream control of what
  the cache holds, and promoting on notification makes the hit rate degrade as the *stream* gets busier
  rather than as the *traffic* changes
  ([ADR-044](DECISIONS.md#adr-044--a-refresh-updates-in-place-never-inserts-and-never-promotes)).
- **The classic read-through race, closed twice over.** A change landing while a load is in flight is
  *held* and version-compared against the store's answer, so neither interleaving loses; and a
  generation counter — captured when the load is *created*, because `create_task` schedules without
  starting — stops a load in flight from refilling a cache that has since been cleared
  ([ADR-045](DECISIONS.md#adr-045--the-read-throughinvalidate-race-is-closed-by-a-held-change-map-and-a-generation-counter)).
- **A failed read is never cached, and the `except` clause is narrow.** `except Exception: return None`
  would turn a five-second database wobble into a 404 with real exposure behind it, while the error rate
  *fell*. `BaseException` would make every orderly shutdown look like an outage
  ([ADR-046](DECISIONS.md#adr-046--a-failed-read-is-never-cached-and-the-except-clause-is-narrow-on-purpose)).
- **`combine_listeners`, which runs every listener even when one raises.** Two things now want the same
  change notification. A bare loop stops at the first failure — so a transient error in the cache costs
  every live subscriber that update *permanently*, because the retry returns `DUPLICATE` and the
  notification is never regenerated
  ([ADR-047](DECISIONS.md#adr-047--combine_listeners-runs-every-listener-then-raises)).

### What slice 5 added

- **Read-your-writes as a query parameter, and a timeout that is a 503 rather than a stale 200.**
  `POST /events` returns the checkpoint it reached; a client passes that back as `?await_event_id=`
  and the read suspends on a future until the projection passes it. A stale answer with a "may be
  behind" header would be rendered by the client and believed by the person watching, who would then
  reissue the instruction they had just issued — which is how one stale read becomes a duplicate
  payment. A 503 is a thing a client can retry, log and alert on
  ([ADR-050](DECISIONS.md#adr-050--freshness-is-a-query-parameter-served-by-a-gate-of-futures-and-a-timeout-is-a-503)),
  with a server-side ceiling on the wait because a caller-chosen timeout is otherwise a denial of
  service written in a URL
  ([ADR-059](DECISIONS.md#adr-059--the-freshness-timeout-has-a-server-side-ceiling)).
- **Pydantic validates HTTP; the event decoder validates events.** `POST /events` takes
  `dict[str, Any]` and hands it straight to `decode_envelope`. A pydantic model in front would be a
  second definition of a contract that already has 127 tests, and the one rejecting a payload first
  would be the one with none — reporting the refusal in pydantic's vocabulary rather than in the words
  the projection's own logs use
  ([ADR-051](DECISIONS.md#adr-051--pydantic-validates-what-belongs-to-http-the-event-decoder-validates-events)).
- **Money leaves as a JSON string, formatted fixed-point.** A JSON number is a double in every
  mainstream parser, so serialising an amount as one would hand away the exactness the whole model was
  built to protect, in the layer least likely to be reviewed for arithmetic. `f"{d:f}"` rather than
  `str(d)`, because `str(Decimal("1E+3"))` is `"1E+3"`
  ([ADR-052](DECISIONS.md#adr-052--money-crosses-the-wire-as-a-json-string-never-as-a-json-number)).
- **`/healthz` checks nothing, and there is deliberately no `/readyz`.** A health check that verifies
  its dependencies turns one slow store into every replica failing its probe at the same moment and
  being restarted together — an outage manufactured out of a degradation. And readiness cannot be
  computed here at all: real lag needs the producer's head event id, which only the broker knows, so a
  probe would have to invent a threshold that marks a quiet, perfectly caught-up projection as unready
  ([ADR-053](DECISIONS.md#adr-053--healthz-is-liveness-only-and-there-is-deliberately-no-readyz)).
- **The SSE handover happens inside the generator, where the cleanup is guaranteed.** Doing it in the
  route function would allow a clean 500 on a store failure — and would also leak a subscription for
  every client that disconnects between the response being built and the stream being read. A leak
  that grows with client churn is worse than a degraded error path, so a mid-handover failure is an
  `error` frame on a 200 that has already begun
  ([ADR-054](DECISIONS.md#adr-054--the-subscribesnapshot-handover-happens-inside-the-sse-generator)).
- **The stream is at `/stream/positions`, not `/positions/stream`.** The prettier URL is one
  counterparty named `stream` away from being unreachable, and the failure would present as a 404 for
  a position that exists
  ([ADR-055](DECISIONS.md#adr-055--the-live-stream-is-at-streampositions-not-positionsstream)).
- **The diagnostics publish their owners' field names verbatim, and the OpenAPI cost is asserted.**
  A pydantic mirror of `ProjectionStats` would be a second place to rename a metric, which is how a
  dashboard breaks for a reason nobody can find. The trade — three endpoints with no named schema — is
  pinned by a test, so it stays a decision instead of decaying into an oversight
  ([ADR-057](DECISIONS.md#adr-057--the-diagnostics-return-as_dict-verbatim-and-are-not-described-in-the-schema)).
- **A missing position is a 404; a counterparty with nothing outstanding is an empty list.** "No
  exposure" and "exposure of zero" are different facts and only one of them is known, so a misspelt
  counterparty must not render as a row of zeros — but a collection that exists and is empty is not a
  missing resource
  ([ADR-058](DECISIONS.md#adr-058--a-missing-position-is-a-404-a-counterparty-with-no-positions-is-an-empty-list)).

Two type-checker facts are recorded in the tests rather than suppressed, because both look like test
bugs and are not: starlette annotates `HTTPException.detail` as `str` while this API always puts the
error envelope there (one `cast` helper, not a blanket `type: ignore`), and mypy narrows an attribute
expression without widening it again at the end of an `async with` — so asserting `not
subscription.is_closed` inside a block makes the assertion *after* the block unreachable under
`warn_unreachable`. Reading the flag through a function call is the honest fix.

## Quick start

Requires Python 3.12+.

```bash
git clone <this repo> && cd position-view
python -m pip install -e ".[dev]"      # pytest, pytest-asyncio, ruff, mypy, httpx, fastapi, uvicorn
python -m pytest                       # 679 tests, about five seconds — none of them sleep on a clock
python -m ruff check . && python -m ruff format --check .
python -m mypy --strict src tests
```

To run it:

```bash
python -m pip install -e ".[serve]"
python -m uvicorn position_view.api:create_app --factory --port 8000
```

`--factory` because there is no module-level `app` to import — the application is built by a function
that takes its `Service`, which is what keeps the test wiring and the production wiring the same code
path ([ADR-049](DECISIONS.md#adr-049--the-application-is-built-by-create_appservice-not-defined-at-import-time)).
Then, in two terminals:

```bash
curl -N http://localhost:8000/stream/positions            # snapshot, then every change, then keep-alives

curl -X POST http://localhost:8000/events -H 'content-type: application/json' -d '{
  "event_id": 1, "schema_version": 1, "event_type": "instruction.received",
  "aggregate_type": "instruction", "aggregate_id": "8f3c...", "occurred_at": "2026-03-01T09:30:00Z",
  "data": {"counterparty": "ACME-BANK", "currency": "USD", "amount": "1500.75"}}'

# read your own write: the checkpoint that POST returned, passed back
curl 'http://localhost:8000/positions/ACME-BANK/USD?await_event_id=1'
curl http://localhost:8000/projection    # what the projection has done, and the two numbers a lag alert needs
```

`uvicorn` is in the `dev` extra as well as `serve`, deliberately: one test needs a real socket, and the
alternative — `pytest.importorskip` — is a test that reports green while checking nothing.

## The shape

```
model/        pure: values, the state machine, the projection step. Standard library only.
projection/   the store protocol and the loop that owns a unit of work and a checkpoint.
push/         the fan-out hub: a conflating buffer per subscriber, bounded.
cache/        read-through, single-flight, invalidated by version — never a store.
api/          FastAPI, SSE, health. The only place a framework appears.
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
- **[ADR-031](DECISIONS.md)** — the fan-out buffer conflates by position instead of queueing events, so a
  subscriber's memory is bounded by what it watches rather than by how busy the stream is. Includes why a
  bounded `asyncio.Queue` is the worst of the available options.
- **[ADR-033](DECISIONS.md)** — the snapshot-to-stream handover, which is the race push APIs get wrong
  most often. Both obvious orderings lose or duplicate data; the fix is one ordering plus one call.
- **[ADR-038](DECISIONS.md)** — the cache is *not* a store, and a test enforces it, because the refactor
  that makes it one is small, pleasant to review, and silently wrong about money.
- **[ADR-041](DECISIONS.md)** / **[ADR-042](DECISIONS.md)** — caching an absence is what makes a
  nonexistent-key flood cheap to serve, and is also what makes an unbounded cache a memory leak with a
  remote trigger. The two decisions only make sense together.
- **[ADR-045](DECISIONS.md)** — the read-through/invalidate race, including the asyncio detail that made
  the first implementation wrong: `create_task` schedules a coroutine, it does not start it, so state
  captured on the coroutine's first line is captured *later* than it looks.
- **[ADR-047](DECISIONS.md)** — why a listener fan-out must not stop at the first failure, worked through
  to the actual consequence: not a logged error, but one subscriber permanently missing an update.
- **[ADR-050](DECISIONS.md)** — the answer to the standard complaint about CQRS: a read model that
  offers no way to say *how fresh* an answer must be is the reason "eventually consistent" feels like a
  broken screen. A gate of futures, and a 503 rather than a stale 200.
- **[ADR-051](DECISIONS.md)** — validation belongs to whichever layer owns the contract, which is why
  the one endpoint in this service with no request schema is the one whose payload matters most.
- **[ADR-053](DECISIONS.md)** — the health check that verifies nothing, and the readiness probe that
  does not exist because this service cannot honestly compute one.
- **[ADR-056](DECISIONS.md)** — the test-strategy ADR: `httpx.ASGITransport` buffers the entire response
  body, so the obvious way to test an endless stream does not fail, it *hangs*. Includes why one real
  socket is worth its tenth of a second.

## Verification

| Gate | Result |
|---|---|
| `pytest` | 679 passed, **0 skipped**, ~5s |
| `ruff check .` | clean (`E W F I B UP ASYNC RUF`, line length 100) |
| `ruff format --check .` | clean |
| `mypy --strict src tests` | clean, including `warn_unreachable` |
| Mutation matrix, slice 1 | **41 / 41 killed**, no survivors |
| Mutation matrix, slice 2 | **32 / 32 killed**, no survivors — one on the second attempt, see below |
| Mutation matrix, slice 3 | **43 / 43 killed**, no survivors, first run |
| Mutation matrix, slice 4 | **57 / 57 killed**, no survivors — after three tests were added and one production line deleted, see below |
| Mutation matrix, slice 5 | **97 / 97 killed**, no survivors — after two tests were added and one redundant condition deleted, see below |

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

Slice 3's claims are mostly about *absence* — no `await` in the publish path, no unbounded buffer, no
version delivered twice, no leaked subscription — so almost all 43 of its mutations are deletions or
one-token relaxations: the changes somebody makes while removing what looks like defensive clutter.

| Mutation | What it breaks | Caught by |
|---|---|---|
| `mark_delivered` records nothing | Every client is resent the whole snapshot as though it were news | `test_the_handover_has_neither_a_gap_nor_a_duplicate` |
| The delivered watermark is assigned rather than `max`'d | A late snapshot re-opens a version the client already applied | `test_mark_delivered_never_moves_the_watermark_backwards` |
| Suppression becomes strict (`<` for `<=`) | Every duplicate in an at-least-once stream reaches the client | `test_the_same_version_twice_is_suppressed` |
| Nothing is ever suppressed | A late publish moves the client's view of a position backwards | `test_a_version_already_delivered_is_not_delivered_again` |
| The bound becomes `>=` | A subscriber is cut off for holding exactly what it was allowed | `test_the_bound_is_inclusive` |
| The bound never fires | A wildcard subscriber that stops reading grows without limit | `test_a_wildcard_subscriber_that_stops_reading_eventually_overflows` |
| The buffer is read without being cleared | Every position ever seen is held for the life of the connection | `test_a_newer_version_replaces_a_buffered_older_one` |
| `drain` stops looping | A suppressed-only wakeup resolves it with an empty batch, which slice 5 would write down a socket | `test_drain_never_returns_an_empty_batch` |
| An overflow ends `updates()` quietly | Losing data becomes indistinguishable from a closed tab | `test_updates_propagates_an_overflow_instead_of_ending_quietly` |
| The context manager cleans up only on `Exception` | Every cancelled connection — which is all of them — leaks a subscription | `test_the_context_manager_unsubscribes_on_cancellation` |
| `keys=[]` is promoted to a wildcard | A client that asked for nothing is streamed the whole book | `test_an_empty_key_set_receives_nothing_rather_than_everything` |

**Two of slice 3's mutations make the suite hang rather than fail**, and that is worth writing down
because it changes the tool. Dropping `_wakeup.clear()` turns `drain` into a tight loop that never
yields; dropping `_wakeup.set()` from `close` parks a reader on an event nobody will ever set. Neither
produces a failing assertion — the process simply stops making progress, and a matrix that waits for a
return code waits forever. A hang *is* a failure, so the runner gives each suite run a subprocess
timeout and counts a timeout as killed. Without that, the two most subtle mutations in the slice could
not have been measured at all.

Slice 4 is a cache, and a cache is the easiest thing in a service to get wrong without anybody
noticing, **because every way it can be wrong still returns a plausible value.** Drop single-flight and
it still answers. Stop caching absences and it still answers. Refill after a clear, promote on refresh,
count a teardown as a failure — each of those is a green suite and a service that behaves differently
under load than it does under test. So 57 mutations, almost all of them *relaxations*: one comparison
widened, one counter silenced, one guard inverted.

| Mutation | What it breaks | Caught by |
|---|---|---|
| `asyncio.shield` dropped, the waiter awaits the shared load | One client disconnecting cancels the load for every other waiter on that key | `test_cancelling_one_waiter_does_not_cancel_the_shared_load` |
| The miss check becomes `dict.get(key) is not None` | Negative caching silently stops working while its counters keep moving | `test_an_absent_position_is_cached_as_an_absence` |
| The in-flight register is never consulted | A stampede on one key reaches the store in full | `test_fifty_concurrent_misses_produce_one_store_read` |
| The generation is captured inside `_load` instead of at `create_task` | A `clear()` in the schedule-to-start window is missed, and the load refills the cache it was meant to be discarded by | `test_a_load_in_flight_when_the_cache_is_cleared_does_not_refill_it` |
| A held change always wins, without the version comparison | The ordinary interleaving — commit before the store read — pushes the cache *backwards* | `test_a_held_change_older_than_the_load_does_not_win` |
| The held-change comparison is inverted | A change arriving during a load is lost until the next change to that position | `test_a_change_arriving_during_a_load_is_not_lost` |
| The `finally` no longer clears `_in_flight` | One transient error poisons one counterparty permanently, with a healthy store | `test_a_failed_load_does_not_prevent_the_next_one` |
| A failed read is answered as an absence | A database wobble becomes a 404 for a counterparty with real exposure, while the error rate *falls* | `test_a_store_failure_is_not_cached_as_an_absence` |
| `except Exception` widens to `BaseException` | Every orderly shutdown adds to the store-failure count, so a deploy looks like an outage | `test_a_base_exception_is_not_counted_as_a_store_failure` |
| The done-callback is dropped | asyncio logs "Task exception was never retrieved" at collection time, with a traceback and no request context | `test_an_abandoned_failed_load_does_not_spam_the_event_loops_logger` |
| The cold-drop check is inverted | The event stream populates the cache and evicts the working set the API is serving | `test_a_change_for_an_uncached_key_is_dropped_rather_than_inserted` |
| A refresh promotes the entry | Recency comes to mean "recently changed", so the hit rate degrades as the stream gets busier | `test_a_refresh_does_not_promote_an_entry` |
| The staleness comparison relaxes to `<` | A redelivered notification is counted as a change, hiding the counter that watches for reordering | `test_a_change_at_the_cached_version_is_refused` |
| The listener fan-out re-raises instead of collecting | A transient failure in the cache costs every live subscriber that update, permanently | `test_combine_listeners_runs_the_rest_when_one_raises` |
| An untouched cache reports a 0% hit rate | A cache with no traffic and a cache that has stopped hitting look identical | `test_the_hit_rate_is_none_before_any_traffic` |
| `on_change` gains an `await` | The projector's lock is held across a yield to the event loop | `test_on_change_never_suspends` |

**Three of those 57 are only killed because the matrix was audited before it was run**, and that is the
part worth passing on: writing the mutation list is itself a review of the tests, and it is cheaper to
read the list and ask "what would catch this?" than to run 57 suites and read the survivors.

Two mutations had no test at all when they were written down — the missing done-callback, and the
widened `except` clause. The first was resolved by a throwaway probe before any test was written, since
"asyncio logs a line when an exception is never retrieved" is a claim about garbage-collection timing and
a test that cannot actually observe it would be worse than no test. It can be observed: replace the
loop's exception handler, cancel the only waiter, drop the reference, `gc.collect()`. The second needed a
`BaseException` that pytest does not treat as a request to abandon the session — a real
`KeyboardInterrupt` aborts the run instead of asserting anything — so the file defines a one-line
`Teardown(BaseException)` for it.

**The third was resolved by deleting a line of production code rather than by testing it.** `_remember`
had a `move_to_end` guarding the "promote an existing entry" case, and that case cannot arise: a hit
returns early, and single-flight means one load per key, so the method is only ever reached for a key
that is absent. No test could distinguish the line from its own deletion — which makes it a line that
has quietly stopped meaning anything, and a future reader would reasonably take its presence as evidence
that promotion-on-insert matters here. The comment that replaced it says where the promotion that *does*
matter lives, and where the deliberate lack of one lives.

Slice 5 is the HTTP surface, and almost every way it can be wrong still returns `200 OK`. A stale
answer is a valid response. A bucket wired to the wrong field is a valid response. A stream that never
flushes looks, from inside the process, exactly like a stream that does. So its 97 mutations are mostly
about *meaning* rather than about failure: the same shape carrying a different fact.

| Mutation | What it breaks | Caught by |
|---|---|---|
| Money is serialised as a JSON number | Every browser client parses amounts as doubles, and the exactness the model was built for is gone | `test_money_crosses_the_wire_as_a_string_and_never_as_a_number` |
| An amount is rendered with `str()` instead of fixed-point | A value with a large exponent goes out as `1.5E+3`, and one client one day reads it as 1.5 | `test_a_large_amount_renders_as_digits_and_not_in_exponent_form` |
| `failed` is filled from `settled` | A failed instruction reads as a completed one — the most expensive wrong answer this payload can give | `test_each_bucket_reaches_the_field_that_names_it` |
| A timed-out freshness wait reports success | Exactly the stale 200 the parameter exists to prevent, with no way for a client to detect it | `test_a_checkpoint_that_never_arrives_is_a_503_with_retry_after` |
| The gate is advanced **before** the event is applied | A released reader is answered from the cache as it was before its own write | `test_ingest_advances_the_gate_after_the_listeners_have_run` |
| The abandoned waiter is not removed from the gate | Every hopeless request leaks a future, so the leak is proportional to how far behind the projection is | `test_a_timed_out_read_leaves_no_waiter_behind` |
| Startup does not seed the gate from the store | Every restart blocks freshness reads on data the store already holds | `test_startup_seeds_the_gate_from_the_store` |
| The cache is dropped from the projector's fan-out | A read straight after a write is served the version before it, and nothing anywhere disagrees | `test_one_ingest_reaches_both_the_cache_and_the_subscribers` |
| The projector reads *through* the cache | The fold computes from a possibly stale base and commits the result as truth | `test_the_cache_is_never_handed_to_the_projector_as_a_store` |
| Shutdown does not close the hub | Every reader task is still parked when the server stops, so graceful shutdown always ends in a kill | `test_the_lifespan_closes_every_subscription_on_the_way_down` |
| `mark_delivered` is dropped | A change committed inside the snapshot window is re-sent as an update, so a client doubles every position on connect | `test_a_change_committed_during_the_snapshot_is_not_re_sent` |
| A frame is not terminated by a blank line | No SSE client can tell where a frame ends; the stream parses as one endless frame | `test_a_frame_is_an_event_line_a_data_line_and_a_blank_line` |
| The heartbeat becomes a named event instead of a comment | Every client logs an unknown-event warning every 15 seconds, forever | `test_a_comment_frame_carries_no_event_name` |
| The drain is unbounded, with no heartbeat | An idle connection is reaped by a proxy and the client is never told — the "dashboard was stale overnight" bug | `test_an_idle_stream_sends_a_keep_alive_comment` |
| An overflow ends the stream silently | Losing data becomes indistinguishable from a normal close, so a client re-renders a frozen view | `test_a_subscriber_that_falls_behind_is_told_before_the_stream_ends` |
| The snapshot is sent under the update event name | A client merges the snapshot into stale state instead of replacing it | `test_the_first_frame_is_the_retry_hint_and_the_second_is_the_snapshot` |
| A readiness probe appears | An invented staleness threshold takes a caught-up projection out of rotation on a quiet stream | `test_there_is_no_readiness_probe` |
| A skip is reported as a refusal | At-least-once delivery reads as a stream of 400s, and a consumer retrying on 4xx loops forever | `test_a_duplicate_is_a_200_with_a_reason` |
| The reported checkpoint is a constant | The freshness token is useless, and the read-your-writes parameter quietly stops working | `test_the_checkpoint_from_a_post_is_usable_as_a_freshness_token` |
| The stream moves under `/positions` | One counterparty named `stream` from being unreachable, presenting as a 404 for a position that exists | `test_the_stream_path_is_not_shadowed_by_a_counterparty` |

**Three of those 97 survived the first run, and only two of them were test gaps.** The first was
`failed` filled from `settled`: the suite's one full-body assertion reads a position with a single
non-zero bucket, so three of the four fields were `0.00` and any mix-up *between those three* was
invisible. Fixed by one test with four distinct amounts and four distinct counts.

The second is the more interesting one, because there was already a test named for exactly the
behaviour the mutation broke. `test_the_snapshot_is_not_re_sent_as_an_update` ingests its event *before*
the stream starts — so by the time the subscription exists there is nothing buffered, `mark_delivered`
has nothing to suppress, and deleting the call changes nothing that test can see. The window
[ADR-033](DECISIONS.md#adr-033--a-subscription-is-registered-before-the-snapshot-is-read-and-mark_delivered-closes-the-gap)
is about is the one where a change lands *between* subscribing and snapshotting, and reaching it needs
the frame order: `retry` is yielded after the subscription is created and before the snapshot is read,
so a generator suspended on that first yield is sitting inside the gap. Ingest there and the change is
both buffered and included in the snapshot; what must follow is silence, not a repeat. A test named
after a property is not the same thing as a test that reaches it.

**The third survivor was not a test gap at all, and the honest response was to delete a line.**
`parse_position_key` guarded with `if not separator or not counterparty.strip():` — and with
`rpartition`, a string containing no slash comes back as `("", "", raw)`, so the counterparty is always
empty in exactly the case the first clause was watching for. No input can reach that clause on its own,
so no test can possibly fail when it is removed: an *equivalent* mutant, not an unkilled one. Dead logic
that reads as a safeguard is worse than no safeguard, because the next reader trusts it. The clause is
gone, the comment in its place explains why the check is unnecessary rather than missing, and the
mutation was replaced with one on the same line that a test does catch.

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
| `test_api.py` | 96 | Routes, freshness, ingest, SSE framing, the diagnostics, and the composition |
| `test_project.py` | 84 | The transition arithmetic, dedup, check order, and a stream fold |
| `test_positions.py` | 72 | The 16-pair transition table, the version rule, the identity contract |
| `test_cache.py` | 67 | Single-flight, negative caching, the LRU bound, the invalidate races, the fan-out helper |
| `test_layering.py` | 62 | The dependency arrow, the purity of `model/`, and that FastAPI stays inside `api/` |
| `test_money.py` | 61 | Exactness, refusals, and the stdlib behaviour the module works around |
| `test_hub.py` | 54 | Conflation, the handover, the bound, and that `publish` cannot block |
| `test_projector.py` | 35 | Atomicity, the lock, counter honesty, notification order, replay |
| `test_store.py` | 21 | One unit of work, the high-water checkpoint, and the shape of the protocol |

**Zero skips is a deliberate property, not a coincidence.** An earlier version of the invariant tests
reported "83 passed, 34 skipped" because it called `pytest.skip` on a condition computed by the function
under test — so a rule regression would have converted real assertions into skips and stayed green. That
was replaced with an explicit data table plus a completeness cross-check
([ADR-017](DECISIONS.md#adr-017--no-test-may-skip-on-a-condition-computed-by-the-code-under-test)).

Six facts in this repository were found by tests rather than by reasoning, and all six are recorded
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
- `asyncio.create_task` schedules a coroutine without starting it, so the cache's generation counter —
  read on `_load`'s first line — was captured several event-loop ticks *after* the decision to load. A
  `clear()` landing in that window was invisible to the load, which then refilled the cache it was
  supposed to have been discarded by. Two tests failed on their first run and both were right: the
  generation is now captured synchronously and passed in as a parameter.
- `httpx.ASGITransport` buffers the whole response body: it collects every `http.response.body`
  message, waits for the application to finish, and only then hands the joined bytes over. An event
  stream never finishes, so requesting one through it does not fail — it hangs until somebody cancels
  the suite, which is the worst behaviour a test can have. That is why the SSE tests drive the
  generator directly and exactly one test starts a real server on a real port
  ([ADR-056](DECISIONS.md#adr-056--sse-is-tested-three-ways-because-httpxasgitransport-buffers)).
- `asyncio.wait_for(coro, timeout=0)` never runs the coroutine at all. It wraps it in a task, finds a
  task that has not been scheduled to be not done, cancels it, and raises `TimeoutError`. So a
  zero-length heartbeat interval can only ever produce keep-alives, and a test that expected `timeout=0`
  to mean "check for a batch, then time out" was asserting something the fast path makes impossible.
  Three tests are shaped by it: the ones asserting the timeout *branch* use zero because it is exact and
  clock-free, and the one asserting that cancelling a drain cannot lose a batch uses ten milliseconds,
  because that test needs the drain to genuinely start.

## Not what this is

Not a settlement engine — it consumes events, it does not produce them, and it deliberately holds no
opinion about whether a settlement was correct. Not a general-purpose CQRS framework: there is one
aggregate type, one read model, and every decision is made concretely for that case, because the
concrete version is the one where the reasoning is visible.

## Licence

MIT — see [LICENSE](LICENSE).
