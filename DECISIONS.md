# Architecture decision records

One entry per decision that a reviewer might reasonably have made differently. The value of an ADR is
in the *rejected* alternative and the cost accepted — a record that only states what was built is a
changelog. Entries are append-only; a reversal gets a new ADR that supersedes the old one rather than
an edit, because the reasoning that turned out to be wrong is the useful part.

---

## ADR-001 — The read model re-declares the event contract instead of importing it

**Status:** accepted (slice 1)

**Context.** The events this service consumes are published by a settlement engine that already
defines its own `Envelope` dataclass. Importing that class — as a shared package, a git submodule, or
a generated client — would delete this project's entire `events.py`.

**Decision.** `position_view.model.events` hand-writes the decoder and names only the fields this
service reads. There is no build-time dependency on the producer in either direction.

**Why.**

1. **A shared class is a shared build.** With a common package, the read model can no longer be
   released, rolled back, or even imported without the write model's package at a compatible version.
   That converts an asynchronous, decoupled integration into a synchronous, coupled one at exactly the
   point where the decoupling was the reason for splitting the services.
2. **A shared class hides breakage rather than surfacing it.** If the producer renames `counterparty`,
   an imported dataclass keeps compiling and this service starts reading `None`. A hand-written decoder
   raises `MalformedEventError` on the first message, at the boundary, naming the field.
3. **The contract is the wire format, not a Python type.** Writing the decoder by hand keeps that
   honest: `events.py` is a statement about JSON, and it would still be correct if the producer were
   rewritten in Go.

**Cost accepted.** Two files describe one message shape, so a genuinely additive producer change has
to be applied twice. This is mitigated by narrowness: the decoder reads eight fields and drops the
rest, so most producer changes require no edit here at all.

**Rejected alternative.** A generated client from a shared schema (Avro, protobuf, JSON Schema). It is
the right answer at the point where there are five consumers and a registry to enforce compatibility.
With one consumer it adds a code-generation step, a schema repository and a build ordering constraint
in exchange for removing forty lines of `isinstance` checks that are also the project's documentation
of what a valid message is.

---

## ADR-002 — The model layer has no runtime dependencies and performs no I/O

**Status:** accepted (slice 1)

**Context.** This service will end up with FastAPI, an SSE implementation, a store and a subscription
hub. The natural shape is to build the projection directly against those: a projector that writes to
the database as it folds, a `Position` that knows how to serialise itself to JSON, an error that is
already an `HTTPException`.

**Decision.** `position_view/model/` imports only the standard library, touches no clock, no
randomness, no filesystem and no network. Everything outward-facing lives in `projection/`, `push/`,
`cache/` and `api/`, and the dependency arrow runs one way:

```
model  ->  projection  ->  push  ->  cache  ->  api
```

**Why.**

1. **Testability, concretely.** The whole transition matrix — every prior bucket against every event
   type — runs in under half a second with no fixtures, no database and no event loop. The most
   important test in the repository (`test_the_undo_half_uses_the_stored_amount_not_the_events`) is
   three function calls. If `project` wrote to storage, that test would need a storage double, and a
   storage double is a place for the test to be wrong in the same direction as the code.
2. **The interesting behaviour is not HTTP-shaped or SQL-shaped.** A bucket transition is arithmetic;
   deduplication is an integer comparison. Coupling either to a request or a cursor mixes the logic
   being demonstrated with the transport carrying it, and a reader can no longer tell which is which.
3. **`git clone && pytest` works with nothing installed but the toolchain.** A suite that needs a
   package index is a suite that stops running the moment the network is restricted, and "the tests
   don't run here" is how a suite quietly dies.

**Cost accepted.** The boundary has to be crossed explicitly: something outside the core must read the
clock for lag metrics, own the transaction, and turn `MalformedEventError` into a retry decision. That
adapter code is real work that a directly-coupled design would not need.

**Rejected alternative.** Enforcing the rule by convention and a paragraph in the README. Rejected
because this property is never lost deliberately — it goes one convenient import at a time, and each
one passes review. `tests/test_layering.py` parses the AST and fails the build instead (ADR-018).

---

## ADR-003 — Ordering and deduplication come from `event_id`, never from a timestamp

**Status:** accepted (slice 1)

**Context.** The projection must be idempotent, because the transport is at-least-once: the same event
will be delivered twice, and applying `settled` twice would add the amount to the settled total twice.
Something has to answer "have I already seen this?".

**Decision.** Each event carries `event_id`, the producer's outbox row id: assigned by a single writer,
monotonically increasing, unique forever. Each instruction's snapshot stores `last_event_id`. An event
with `event_id == last_event_id` is a duplicate; `<` is an ordering violation; `>` is applied.

**Why.** The alternatives all fail on a real case:

| Candidate | How it fails |
|---|---|
| `occurred_at` | Two events can share a timestamp; clocks move backwards; a backlogged relay republishes old events with old timestamps *in the right order*, and a timestamp comparison would drop all of them. |
| Arrival order | At-least-once delivery redelivers, which is the entire problem being solved. |
| A hash of the payload | Answers "seen this exact message?" but not "is this newer?", so it needs unbounded storage of every hash ever seen and still cannot order. |
| A per-aggregate version from the producer | Works, and is strictly better where it exists — but it is a field the producer would have to add, and it cannot order events *across* aggregates for the checkpoint. |

Comparing integers also makes the state small: one number per instruction, not a set of message ids.

**Cost accepted.** This service's correctness now depends on a property of someone else's database
column. That dependency is stated in ADR-005 and the failure is made observable rather than assumed
away.

---

## ADR-004 — Money is `Decimal` at a fixed two-place exponent, and rounding is refused

**Status:** accepted (slice 1)

**Context.** A position is a running total added to and subtracted from thousands of times — the worst
case for binary floating point, because every operation contributes error in an unpredictable
direction and the errors do not cancel. Separately, amounts arrive as JSON, where they may be spelled
`"1500"`, `"1500.0"`, `"1500.00"` or (badly) `1500.0`.

**Decision.** Three rules:

1. Amounts are `Decimal`, parsed from a **string** or an **integer**. A JSON float is refused with a
   message saying what to send instead.
2. Every amount is normalised to exponent `0.01` at the boundary, so the three spellings above become
   one value.
3. Normalisation **refuses to round**. `"1500.005"` is a refusal, not a silent `1500.00`.

**Why.** `0.1 + 0.2 == 0.30000000000000004` is not a curiosity — it is the reason a float-based read
model eventually reports `-0.000000000000002` for an account that is exactly flat, and the reason
`position == 0` is false forever. `Decimal` is exact for decimal fractions, so a value added and then
subtracted returns to exactly where it started. That is not a nicety here: a bucket transition *is* a
subtraction followed by an addition (ADR-007), so exactness under add-then-subtract is the property the
projection is built on.

Refusing to round matters because the alternative is invisible. Rounding `"1500.005"` makes the read
model disagree with the write model by half a cent per event, forever, with no error anywhere. A
precision mismatch between two services is a contract problem, and the honest response is to say so at
the boundary.

**Cost accepted, and it is a real one.** Two decimal places is wrong for some currencies: JPY has zero
minor units, BHD and KWD have three. A genuinely multi-currency ledger needs the exponent to be a
property of the currency, which means a currency table, which means this module needs a dependency it
deliberately does not have (ADR-002). For JPY the consequence is cosmetic — `¥1500.00` is odd but the
trailing zeros are exact, so no arithmetic is wrong. For a three-place currency it is **not** cosmetic:
a genuine `0.001` would be refused as excess precision. That is the correct failure (loud, at the
boundary) rather than the wrong one (silent truncation), but it is a limitation, not a feature.

**Also accepted:** an upper bound of `1e15` on a single amount. Not because a larger trade is
impossible, but because `Decimal` has no natural size limit, so a producer bug sending `"1e999"`
produces a position that is technically correct, formats to a kilobyte of digits, and is
indistinguishable in a dashboard from a real number. A bound turns that into one refused event instead
of a permanently poisoned position — and a poisoned position cannot be un-poisoned without replaying
the whole stream.

**Verified rather than assumed.** `Decimal.quantize` **does not** refuse to round by default; it
rounds silently under the default context. The refusal in `quantize` comes from running in a `Context`
that traps `Inexact`. Two adjacent facts were checked at the same time: `Rounded` must *not* be
trapped (it fires for discarded trailing zeros, so trapping it would refuse `"1500.0000"`), and
`Context(traps=[...])` *replaces* the default trap set rather than extending it, so
`InvalidOperation`, `DivisionByZero` and `Overflow` have to be restated or an over-large value returns
NaN instead of raising. `test_default_quantize_would_have_rounded_silently` pins the stdlib behaviour
so that this reasoning cannot rot.

---

## ADR-005 — `event_id` monotonicity holds for one producer, and the limit is enforced, not assumed

**Status:** accepted (slice 1)

**Context.** ADR-003 rests on `event_id` increasing forever. That holds for one producer writing one
outbox table. It does not hold if the write model is sharded, if a second publisher is added, if the
outbox is truncated and the sequence restarted, or if events are replayed from a backup taken before
the current sequence position.

**Decision.** The assumption is documented as a precondition, and the projection treats a *regression*
in `event_id` as a first-class outcome (`SkipReason.OUT_OF_ORDER`) rather than as an impossible state.
The event is skipped, the reason is returned to the caller, and the checkpoint still advances.

**Why.** The dangerous property of this assumption is that breaking it is silent. A second producer
would emit ids that collide with the first, and the projection would drop real events as duplicates
while reporting perfect health. Making regression an explicit, countable outcome converts "the numbers
are quietly wrong" into "a counter is non-zero", which is the difference between a bug found in a
morning and a bug found in a quarterly reconciliation.

**Cost accepted.** A genuine ordering violation is *recorded and dropped*, not repaired. Repairing it
would mean buffering events and reordering them, which requires a bound on how out-of-order the stream
can be — a number nobody can supply honestly. Dropping with a loud reason is the smaller wrong answer.

**Rejected alternative.** Raising on a regression. It would stall the partition on a condition that
this service cannot fix, and stalling on data you cannot process is how one bad event becomes an
outage.

**Explicitly not checked: gaps.** `event_id` is a *global* sequence shared with events for other
aggregate types, so a gap in the ids seen for one instruction is the normal case, not a lost message.
Alerting on gaps would produce continuous false alarms; detecting genuinely lost events needs the
producer's own sequence-per-aggregate or an end-to-end reconciliation, both out of scope here.

---

## ADR-006 — Currency is part of the position key, not a field on the position

**Status:** accepted (slice 1)

**Context.** A position identifies "how much exposure to whom". The obvious shape is
`Position(counterparty, currency, pending, settled, ...)`, where currency is one attribute among many.

**Decision.** `PositionKey(counterparty, currency)` is the identity. There is exactly one position per
(counterparty, currency) pair, and its string form — `ACME-BANK/USD` — is the addressable name used in
URLs, SSE event names and log lines.

**Why.** Summing across currencies requires FX rates; FX rates have timestamps and sources; a read
model that quietly adds dollars to yen produces a number that is confidently wrong and has no error
attached. Making currency part of the identity means **the wrong sum is not expressible** — there is no
code path that can produce it, so there is no code path that needs a test for it. Constraints that
remove a class of bug are worth more than validation that detects it.

**Cost accepted.** A client that genuinely wants "total exposure to ACME-BANK across currencies" has to
ask for each currency and convert, using rates this service does not hold. That is more work for the
client and it is the correct place for the work, because the client knows which rate, as of when, and
for what purpose.

---

## ADR-007 — The projection stores the per-aggregate state it is aggregating away

**Status:** accepted (slice 1)

**Context.** This is the decision the entire repository exists to demonstrate, so it is worth stating
the naive version first, because it looks right.

A position holds four buckets: pending, settled, failed, reversed. An `instruction.settled` event
arrives. The obvious handler is *"add the event's amount to `settled`, subtract it from `pending`"*.

That handler is wrong, and it is wrong for a reason no test of a single event will find.

**The failure.** Suppose instruction X is received at `1500.00`, so pending is `1500.00`. It is then
amended to `2000.00` — the producer emits `instruction.matched` with the new amount, and pending is now
`2000.00`. Then it settles, and the `settled` event carries `2000.00`.

The naive handler subtracts `2000.00` from pending and adds `2000.00` to settled. Pending is now
correct by accident. Now consider the amendment going the other way, or an event whose amount differs
from what was last counted for any reason: **the event does not know what this projection previously
counted.** Subtracting the envelope's amount from the bucket it was never added to leaves a residue in
pending equal to the amendment delta — permanently, invisibly, and un-recoverably without a full
replay.

**Decision.** The projection stores an `InstructionSnapshot` per instruction: its `last_event_id`, the
bucket it is currently counted in, the amount it is currently counted *at*, and its position key. A
transition is then two halves with two different sources of truth:

* **Undo** — subtract the **stored** amount from the **stored** bucket.
* **Redo** — add the **envelope's** amount to the bucket the **envelope's** event type implies.

**Why.** An aggregating read model over a state machine cannot be a function of the current event
alone. The information "I previously counted 1500 in pending" exists nowhere in the event stream at the
point the `settled` event is handled — the producer has no idea what this consumer did. Either the
consumer remembers, or it re-reads the whole history for that instruction on every event. Remembering
is one small row.

**Cost accepted.** More state, and a second thing that must be written atomically with the position
(see slice 2). The snapshot is also the only per-instruction data in an otherwise per-position store,
which makes the store's shape less tidy than it looks in a diagram.

**Rejected alternative.** Storing only aggregate totals and recomputing from the stream on drift.
Rejected because "on drift" requires detecting drift, and the whole failure mode above is undetectable
from the totals alone: `pending = 500.00` is a perfectly plausible number.

**This is pinned by `test_the_undo_half_uses_the_stored_amount_not_the_events`**, which is labelled in
its own docstring as the most important test in the repository. It fails if either half of the rule is
sourced from the wrong place, and it is the test to read first when changing anything in `project.py`.

---

## ADR-008 — "Skipped" is a returned value, not a raised exception

**Status:** accepted (slice 1)

**Context.** `project` has five reasons to decline an event: it belongs to another aggregate type, its
event type is not one this view reads, it is a duplicate, it arrived out of order, or the transition it
implies is illegal. Some of those are routine and some indicate a bug elsewhere.

**Decision.** `project` returns `Applied | Skipped`. `Skipped` carries a `SkipReason` enum and an
optional human-readable detail. Nothing in the model layer raises for these cases, and no reason is
logged from inside the core.

**Why.**

1. **The projector must advance its checkpoint past a skipped event.** That is the crux. An exception
   would have to be caught by the loop and turned back into "keep going" anyway — and the version of
   that code which forgets to advance the checkpoint reprocesses the same event forever, producing a
   stalled projection whose only symptom is lag growing linearly. Making the outcome a value means the
   caller cannot accidentally omit the case; `match` on a union is exhaustive and mypy says so.
2. **Skipping is the common path, not the error path.** On a shared stream most events belong to other
   aggregates. Exceptions for the majority case make flame graphs and logs useless.
3. **The reason belongs to the caller, not the core.** Whether `ILLEGAL_TRANSITION` is a warning, a
   metric, or a page is an operational judgement that differs per environment. The core states the
   fact; the caller decides how loud it is. This is also why `logging` is on the model layer's
   forbidden-import list (ADR-002) — a core that logs has already made that decision for everyone.

**Cost accepted.** Every caller must handle both branches, which is more code at each call site than
letting an exception fly past. That is the intended trade: the compiler asks the question every time.

**Rejected alternative.** Returning `None` for "nothing happened". It loses the reason, and the reason
is the entire operational value — `DUPLICATE` is healthy, `OUT_OF_ORDER` means a guarantee broke, and
one of those needs a human.

---

## ADR-009 — A version bump means the observable content changed, and a no-op returns the same object

**Status:** accepted (slice 1)

**Context.** Three of the six event types this view reads (`received`, `validated`, `matched`) map to
the same bucket, `PENDING`. So on a busy stream, most events change no total at all: the undo and redo
halves cancel exactly.

**Decision.** `Position.with_deltas` compares the folded result against the current one and, if
nothing observable changed, returns **`self`** — the identical object, asserted in tests with `is`, not
`==`. `version` is not incremented and `updated_at` is not touched.

**Why.** Everything downstream is built on "version changed means content changed":

* the SSE fan-out publishes on version change, so a version bump per event would push three identical
  frames per instruction to every subscriber;
* the cache invalidates on version change, so a bumped version discards a still-valid entry;
* clients poll with a version they already hold, and a version that changes without content makes
  every conditional request a full response.

Leaving `updated_at` alone is part of the same decision: if a no-op refreshed the timestamp, `version`
would stop being a content identifier and become an activity counter, and "has this position changed
since I last looked?" would have no answer.

**Cost accepted.** The comparison is real work on every event, and the identity contract is a subtle
thing for a future contributor to preserve — an innocent-looking `return replace(self, version=...)`
breaks it. The `is` assertion in the test is what makes the breakage loud.

---

## ADR-010 — `updated_at` is event time, and the core never reads a clock

**Status:** accepted (slice 1)

**Context.** A position needs a "last changed" timestamp. `datetime.now(UTC)` inside the projection is
the most natural line of code in the repository.

**Decision.** `updated_at` is set from the envelope's `occurred_at`. The model layer calls no clock at
all; `tests/test_layering.py` fails the build if it does.

**Why.** Wall-clock time is wrong twice over. It makes `project` non-deterministic, so "folding the
stream twice gives the same answer" stops being true and the replay-idempotence test has to be deleted
— the test that justifies the whole design. And it makes the timestamp meaningless during a backlog: a
projection an hour behind would report every position as freshly updated, which is the exact opposite
of what an operator needs at that moment.

Event time also makes replay produce byte-identical output, which is what allows a rebuilt projection
to be diffed against the live one.

**Cost accepted.** `updated_at` is not "when this row was written", so it cannot answer "is the
projection stuck?". That question needs a separate, honestly-named lag measurement — the difference
between the newest `occurred_at` seen and now — computed outside the core, where reading a clock is
allowed.

**Also accepted:** naive timestamps are refused at the boundary rather than assumed to be UTC.
Assuming is how a lag measurement ends up off by the producer's UTC offset: a number that looks
plausible, changes twice a year, and is never questioned because it is only ever a few hours.

---

## ADR-011 — The order of checks inside `project` is part of the design

**Status:** accepted (slice 1)

**Context.** `project` performs five checks. Any order produces the same accept/reject decision for a
given event, so the ordering looks arbitrary.

**Decision.** The order is fixed and documented:

1. aggregate type — is this event about an instruction at all?
2. event type — is it one this view reads?
3. deduplication and ordering — have we seen it, or something newer?
4. transition legality — does this move make sense from the bucket we are in?
5. money — apply the arithmetic.

**Why.** The order does not change the outcome; it changes *which reason an operator is given*, and the
reason is what determines where they go looking.

The load-bearing pair is 3 before 4. A duplicate `settled` for an already-settled instruction is both
a duplicate and an illegal transition. Checking legality first reports `ILLEGAL_TRANSITION`, which
means "the producer emitted a nonsensical sequence" and sends someone to read the write model's state
machine. Checking order first reports `DUPLICATE`, which means "at-least-once delivery worked as
designed" and needs nobody. The same event, the same drop, two very different mornings.

The cheap filters come first for the ordinary reason: on a shared stream most events fail check 1, and
there is no point parsing money for an event that belongs to another service.

**Cost accepted.** The order is a behavioural contract that no type can enforce, so it has to be held
in place by tests. `TestCheckOrder` exists for exactly that and asserts the specific reason returned
for events that violate two rules at once.

---

## ADR-012 — Four explicit bucket fields, not a mapping keyed by bucket

**Status:** accepted (slice 1)

**Context.** `Position` holds four `Totals`. The compact spelling is
`totals: dict[Bucket, Totals]` (or `Mapping`), which makes `with_deltas` a one-line loop and adding a
fifth bucket a one-line change.

**Decision.** Four named fields — `pending`, `settled`, `failed`, `reversed_` — and a `match` over the
bucket enum in `totals_for`.

**Why.**

1. **Exhaustiveness is checked.** With a `match` over four literal enum members and no wildcard, mypy
   reports a missing case when a fifth `Bucket` is added. With a dict, a new member is a `KeyError` at
   runtime on whichever event first touches it — in production, on one counterparty.
2. **A missing key has no good answer.** `totals.get(bucket, Totals())` silently invents a zero for a
   bucket that was never initialised, which is indistinguishable from a real zero. `totals[bucket]`
   raises from deep inside the arithmetic. Named fields cannot be absent.
3. **The shape is the schema.** The API response and any stored row have four known columns; a dict
   would have to be projected onto them anyway, in a second place that could disagree.

**Cost accepted.** `with_deltas` folds over four locals and compares four values rather than iterating
a mapping, and adding a bucket touches several places. Given that adding a bucket is a change to what
this service *means*, being made to visit each place is appropriate.

**Rejected alternative.** `dataclasses.replace` in a loop over deltas, one call per delta. It reads
well but makes "did anything change?" a four-way comparison at the end anyway (ADR-009), and allocates
an intermediate `Position` per delta for no benefit.

---

## ADR-013 — An illegal transition is recorded and dropped, not rejected

**Status:** accepted (slice 1)

**Context.** `is_legal_transition` encodes this projection's assumptions: nothing leaves a terminal
bucket, `PENDING → PENDING` is normal, `SETTLED → SETTLED` is not. When an event violates one, the
options are to drop it, to apply it anyway, to raise, or to route it to a dead-letter queue.

**Decision.** Return `Skipped(ILLEGAL_TRANSITION, detail=...)`, leave the position untouched, and let
the caller decide what noise to make. The checkpoint advances.

**Why.** The function is not validating the producer's workflow — it is validating *this projection's
assumptions*, and that distinction decides the response. Applying the event anyway would corrupt
totals in a way nothing downstream can detect (a second `settled` double-counts the amount). Raising
would stall the partition on data this service cannot fix. A dead-letter queue is the right answer at
the point where somebody is on the hook to drain it; without that, it is a queue that fills up and
gets deleted, and it costs a topic, a consumer and an alert to build.

**Cost accepted.** The offending event is gone. If the write model really did emit a legitimate
sequence this table calls illegal, the fix is a code change here plus a replay — which is precisely why
the reason is returned rather than swallowed: the counter is the signal that the table, not the
producer, might be wrong.

---

## ADR-014 — Explicit type checks at the boundary, no coercion

**Status:** accepted (slice 1)

**Context.** `decode_envelope` receives whatever `json.loads` produced. The short version is
`int(payload["event_id"])` and `str(payload["event_type"])`.

**Decision.** Every field is `isinstance`-checked and refused on mismatch. `bool` is excluded
explicitly wherever an `int` is expected, because `bool` is a subclass of `int` in Python.

**Why.** `int("12")`, `int(12.9)` and `int(True)` all succeed, and each represents a different producer
bug that coercion would absorb. `12.9 → 12` is the worst: it silently makes two distinct events share a
deduplication key, so one of them is dropped as a duplicate and the position is wrong with no error
anywhere.

The `bool` exclusion is not pedantry. `{"event_id": true}` is a realistic serialiser bug — a field that
used to be a flag — and without the check every such event deduplicates against event id 1.

**Verified rather than assumed.** `schema_version` needs the same treatment for a reason that only
showed up when a test failed: `1 == 1.0` and `1 == True` are both `True`, so a bare
`!= SUPPORTED_SCHEMA_VERSION` accepted `{"schema_version": 1.0}` and `{"schema_version": true}` as
compatible. This was a real gap in the source, found by the test
`test_a_numerically_equal_non_integer_is_refused`, which is named after the fact that it caught it.

**Cost accepted.** Roughly forty lines of validation that a coercing decoder would not need. Those
lines are also the only complete, executable description of what a valid message is, so they are doing
two jobs.

**Also accepted:** whitespace around a numeric amount is tolerated, because `Decimal(" 1500.00 ")`
strips it and the value is unambiguous — while `"1,500.00"` is refused, because a thousands separator
raises a locale question (is `1,500` fifteen hundred or one and a half?) that a consumer must not guess
at. That asymmetry was checked empirically, not assumed, and is pinned by
`test_surrounding_whitespace_is_tolerated`.

---

## ADR-015 — Unknown fields and unknown event types are dropped in silence; a bad *shape* is refused

**Status:** accepted (slice 1)

**Context.** Two different things can be unfamiliar about a message: a field the producer added that
this service does not read, and a message that is structurally broken. Treating them the same way is
the easy mistake in either direction.

**Decision.**

* An unrecognised **field** is ignored without comment. A producer that adds `settlement_venue` must
  not be able to break a consumer that does not want it.
* An unrecognised **event type** or **aggregate type** returns `Skipped`, not an error. On a shared
  stream this is the majority of traffic.
* A **malformed shape** — missing `event_id`, a non-integer where an integer belongs, an unparseable
  timestamp — raises `MalformedEventError`, which is a distinct type precisely so a caller can classify
  it as a **permanent** failure. The same bytes will not become valid in thirty seconds; retrying is
  waste, and retrying forever blocks every well-formed message behind it.
* `schema_version` is the one place where "we have not verified this" is said out loud, and an
  unfamiliar version stops the message — including a *higher* one.

**Why refusing a higher version is not over-caution.** Surely v2 is v1 plus fields? Sometimes. And
sometimes v2 is where `amount` became a minor-unit integer, in which case optimistically reading it as
a decimal turns 1500 dollars into 15 dollars in a report somebody trades on. The version field exists
so that this decision is explicit, and for money the honest default is to stop.

**Cost accepted.** A purely additive v2 requires a one-line deploy here before this service will read
it, which is coordination that a version-tolerant consumer would avoid. Accepted because the failure
mode of guessing is a wrong number, not an outage.

---

## ADR-016 — Test data comes from explicit builders, not from fixtures

**Status:** accepted (slice 1)

**Context.** The suite constructs a lot of envelopes and snapshots. pytest's answer is `conftest.py`
plus fixtures requested by parameter name.

**Decision.** `tests/builders.py` — an ordinary module, imported explicitly — provides
`envelope_payload(**overrides)`, `make_envelope(**overrides)` and `make_snapshot(...)`, each with a
complete valid default and keyword overrides. An `OMIT` sentinel expresses "leave this field out
entirely", which `None` cannot, because `None` is itself a value under test.

**Why.**

1. **A test should read as one thing.** `make_envelope(event_id=6)` says the event id is the whole
   point of the test. A fixture parameter says "something valid was injected from another file", and
   the reader has to go and find out what.
2. **`conftest.py` is implicit.** Nothing in the test file mentions it, so a reader cannot follow the
   dependency, and every test in the directory silently depends on it.
3. **Mutation is a shared-state trap.** A test that receives a fixture-built dict and mutates it can
   affect a later test, depending on fixture scope. Builders return a fresh object per call.

**Cost accepted.** An explicit `from builders import ...` in each test module, and the builders module
must be kept honest — `test_the_default_builder_payload_is_valid` exists so that a change to the
decoder's requirements cannot leave every test constructing an invalid message that happens to fail
for the reason the test expected.

---

## ADR-017 — No test may skip on a condition computed by the code under test

**Status:** accepted (slice 1)

**Context.** The invariant tests iterate all 24 (prior bucket × event type) pairs and assert
properties that only hold for legal transitions. The first version filtered with
`pytest.skip("illegal transitions produce no deltas")`, using `is_legal_transition` to decide.

**Decision.** The legal pairs are a hand-written data table, `LEGAL_PAIRS`, and
`test_the_legal_pairs_table_is_complete` cross-checks the table against `is_legal_transition` for all
24 combinations. Zero skips in the suite.

**Why.** The suite reported **"83 passed, 34 skipped"**, and that report was a lie waiting to happen. A
change that wrongly made a legal transition illegal would have converted a real assertion into a skip
— and stayed green. A test that can silently stop testing is worse than no test, because it also
occupies the space where a real test would have been noticed as missing.

Written as data, the same regression turns those rows into failures.

**Cost accepted.** The table is maintained by hand and will need an entry when the rules change. The
completeness cross-check is what makes that safe: a stale table fails loudly at one obvious place with
a message naming the pair, rather than degrading quietly across 24 tests.

---

## ADR-018 — The architecture is enforced by a test that parses the source

**Status:** accepted (slice 1)

**Context.** ADR-002 states a dependency arrow. A README paragraph does not enforce it.

**Decision.** `tests/test_layering.py` parses every module in the package with `ast` and asserts: no
module imports a layer further out; `model/` imports only the standard library; `model/` imports
nothing from a forbidden list (`logging`, `asyncio`, `threading`, `random`, `secrets`, `socket`,
`sqlite3`); `model/` calls no clock and no randomness; a new sub-package must be registered in the
`LAYERS` list before it can be used.

**Why `ast` and not importing the modules.** Importing gives a *runtime* answer that depends on what
the test process already loaded, and it misses the interesting case entirely: a forbidden import inside
a function body never runs, so it never appears in `sys.modules`, so it looks clean. Parsing sees the
source, which is what review is about.

`sys.stdlib_module_names` is the authority for "is this third party", rather than a hand-written
allow-list, so the test does not need editing when the core starts using another standard module. The
forbidden list exists on top of it because some standard modules — `logging`, `asyncio` — are as
disqualifying as any dependency.

**Cost accepted.** The AST checks match syntax, so they can be evaded (`importlib.import_module`, a
clock read through an alias the matcher does not know). The test is a ratchet against accidental drift,
not a sandbox against a determined author. `test_the_package_root_is_where_this_test_thinks_it_is`
guards the one failure that would make all of it vacuous — a path typo producing zero files to check.

---

## ADR-019 — An instruction that changes counterparty or currency moves between positions

**Status:** accepted (slice 1)

**Context.** `PositionKey` is `(counterparty, currency)` (ADR-006), and both are read off each event
rather than fixed at first sighting. So an event can legitimately carry a *different* key from the one
the instruction was last counted under — a corrected counterparty, or a currency that was wrong on the
original booking. The options are to refuse the event, to ignore the new key, or to handle the move.

**Decision.** `project` returns **two** `PositionChange` values: the undo delta against the *stored*
key, and the redo delta against the *new* key. When the key is unchanged, it returns one change
carrying both deltas.

**Why.** The alternatives leave money double-counted with nothing recording it. Ignoring the new key
keeps the instruction in the wrong position forever, and the wrong position is a plausible number.
Refusing the event means the correction never lands, so the old position stays wrong *and* the new one
never appears — and the next event for that instruction then looks like an ordering anomaly rather than
an obvious symptom. Emitting both halves is the only option under which both positions are right
afterwards.

The two changes touch disjoint state, so their order does not matter for correctness. It matters for
notification: both keys changed, so both must be published, and a subscriber watching only the old
counterparty has to see the exposure go *down*. That is why the return type is a tuple of changes
rather than one change with a nullable second key — the projector's fan-out iterates it without needing
to know that a move is special.

**Cost accepted.** Every caller handles a one-or-two-change result, and the atomicity requirement in
slice 2 gets harder: two position rows plus the snapshot must be written in one transaction, or a crash
between them leaves the amount counted twice. That constraint is stated where the store protocol is
defined rather than discovered later.

**Rejected alternative.** Treating the key as immutable after first sighting and routing a mismatch to
`ILLEGAL_TRANSITION`. Tempting, because the projection gets simpler and the counter would at least be
non-zero. Rejected because a booking correction is an ordinary business event, not a producer bug, and
classifying it as a bug trains people to ignore the counter that also reports real bugs.

---

## ADR-020 — The store protocol has exactly one write method

**Status:** accepted (slice 2)

**Context.** Applying one event changes three things: zero to two position rows, one instruction
snapshot, and the checkpoint. The obvious interface offers a method for each — `save_position`,
`save_snapshot`, `set_checkpoint` — and each one is individually useful, most obviously to a rebuild
script that wants to write positions without inventing snapshots.

**Decision.** `PositionStore` exposes five reads and **one** write: `commit(WriteBatch)`. There is no
`save_position`, no `save_snapshot`, no `set_checkpoint`, no `delete` and no `truncate`. A test named
`test_the_protocol_offers_exactly_one_way_to_write` pins the method set, so widening it is a decision
somebody has to make in a test rather than a two-line addition to a protocol nobody re-reads.

**Why.** Atomicity stops being a rule people remember and becomes a property of the type. The
dangerous version of the code is not merely discouraged, it is unwritable:

```
await store.save_position(new_position)      # crash here
await store.save_snapshot(new_snapshot)
await store.set_checkpoint(envelope.event_id)
```

Crash on the first line and the projection has counted the money in the new bucket while its memory of
the instruction still says the money is in the old one — so the next event for that instruction undoes
the *old* bucket a second time and the projection is wrong in two buckets at once, permanently, with
nothing raised. Crash after the third line but before the first and the checkpoint has moved past an
event that was never applied; the money is gone and no replay will find it, because replay starts from
the checkpoint. Both are silent, and both reconcile to a number nobody can explain.

A guarantee that lives in the shape of the interface is also one the Postgres implementation inherits
for free: it has no way to offer a partial write, so it cannot be *used* to make one.

**Cost accepted.** Callers that genuinely want one thing must build a batch for it, and a rebuild
script cannot bulk-load positions without going through the projector. The second half of that is
arguably a feature — see ADR-029 — but it is a real constraint and it will be felt the first time
somebody wants a fast import path.

**Rejected alternative.** Several write methods plus a `transaction()` context manager, which is what
most ORMs encourage. Rejected because the guarantee then depends on every call site remembering to open
the transaction, the version that forgets is indistinguishable at a glance from the version that does
not, and the failure only appears under a crash at one specific instruction.

---

## ADR-021 — The store's reads are `async` even though the shipped implementation is a dict

**Status:** accepted (slice 2)

**Context.** `InMemoryPositionStore` is three dicts. `async def snapshot_for(...)` over a dict lookup
is ceremony: it needs an event loop, it makes every caller a coroutine, and it buys nothing today.

**Decision.** Every method on `PositionStore` is `async`, including the reads.

**Why.** An interface should be shaped by the dependency that matters rather than the one that is
convenient. The real store is a database, a database call suspends, and a protocol with synchronous
reads cannot be implemented by one without either blocking the event loop or being rewritten along with
every caller. Choosing the honest signature now costs a keyword; discovering it later costs a
refactor of the projector, the cache and the HTTP layer at once.

**Cost accepted.** Async-ness is contagious. Everything downstream of a read is now a coroutine, and
that is the direct cause of ADR-022 — a read-modify-write cycle with suspension points in the middle
needs a lock, which a synchronous store would not have needed. The pure model layer stays synchronous
precisely because it sits on the other side of this boundary: `project` is a function of two values,
and everything that suspends lives in the projection package.

**Also accepted:** a subtler cost that only surfaced under mutation testing. Because the in-memory
store never actually suspends, tests written against it cannot observe the lock at all — see ADR-023.

---

## ADR-022 — One `asyncio.Lock` is held across read → project → commit → notify

**Status:** accepted (slice 2)

**Context.** `Projector.apply` reads the snapshot, decides, writes, and notifies. With an async store
(ADR-021) there are suspension points between those steps, so two events for the same position can
interleave. This is the classic lost update, and its consequences here are silent in both directions:

- two events for *different* instructions on the same position: both tasks read the same position,
  each computes "version 1 with my amount in it", the last commit wins, and one instruction's money is
  simply absent. `snapshot_count` still counts both instructions, so the count invariant is the only
  thing in the service that would ever reveal it.
- two deliveries of the *same* event: both tasks read "no snapshot yet", both treat themselves as a
  first sighting, and one instruction is counted twice. The total is too high, which is the version
  somebody eventually notices, because it can exceed the book.

**Decision.** A single `asyncio.Lock` is acquired at the top of `apply` and held until after the change
listener has returned. Decoding happens *outside* the lock, because it is pure and CPU-bound and cannot
touch the store.

**Why.** The projection is single-writer by design, and this is the cheapest possible way to say so.
The alternative — per-position locking — is a real optimisation and the wrong one to make first: a
cross-position move touches two keys, so it would need both locks, which introduces lock ordering,
which introduces deadlock, in exchange for throughput nobody has measured a need for.

Holding the lock across the *notification* as well is deliberate, and it is not about the store. It is
about subscribers seeing versions in order. Release before notifying and two tasks can publish version
6 after version 7; a subscriber that discards anything not newer than the last version it saw would
ignore every subsequent update for that position, forever, from a race that leaves no trace.

**Cost accepted.** The projector applies one event at a time, so throughput is bounded by the slowest
store round trip and a slow change listener blocks ingestion. The second half is the load-bearing
constraint on slice 3: subscriber delivery must not be allowed to block the projection, which is why
the fan-out hub gets bounded per-subscriber queues rather than awaiting each subscriber's socket.

**Rejected alternative.** No lock, on the grounds that a single consumer task delivers events
sequentially anyway. Rejected because it makes a correctness property depend on a deployment detail
nobody has written down: the day somebody runs two consumer tasks to double throughput, or replays a
backlog with `asyncio.gather`, the projection silently starts losing money.

---

## ADR-023 — A test double that suspends, because the shipped store does not

**Status:** accepted (slice 2)

**Context.** Two concurrency tests were written for ADR-022, driving fifty events through
`asyncio.gather` against `InMemoryPositionStore` and asserting that nothing is lost or duplicated.
The slice-2 mutation matrix then deleted `async with self._lock` from `apply` — and the entire suite,
including both of those tests, stayed green.

**Decision.** The projection tests carry a second double, `_SuspendingStore`, which delegates to the
in-memory store and awaits `asyncio.sleep(0)` before every operation. The lock's guarantee is pinned by
two tests that use it. The original two are kept, with their docstrings corrected to state what they
actually check.

**Why.** The survivor exposed a wrong belief, not a missing assertion. **Awaiting a coroutine is not a
suspension point.** `await` on a coroutine that never itself blocks runs it start to finish on the
current task without a trip through the event loop, so `apply` over three dict operations is
effectively one synchronous block. There is no interleaving for a lost update to happen in, and
therefore no way for any test using that store to notice the lock is gone. The tests were asserting
that the fold is correct under `gather`, which is worth pinning and is not what their names claimed.

`asyncio.sleep(0)` is the minimal faithful model of the real dependency: one yield, which is what a
network round trip costs a task in scheduling terms. `commit` yields *before* mutating, mirroring a
round trip that reaches the database and then applies the whole transaction — so the batch stays atomic
while the call as a whole is interruptible, which is exactly the shape the projector must be safe
against.

**Cost accepted.** A second double to keep in step with the protocol, and a test suite where two
scenarios appear twice. Both doubles are therefore assigned to a `PositionStore`-annotated variable in
a test of their own, so `mypy --strict` fails if either drifts from the contract — a double that has
drifted is worse than no double, because it keeps passing after the thing it was written to catch has
stopped being checked.

**Also accepted:** the general lesson, which is why this is an ADR rather than a commit message. A
concurrency test is only as real as its slowest dependency. Against an in-memory double, "concurrent"
tests frequently prove nothing at all, and they are reassuring in exactly the way a bad test is
reassuring. This one was caught by mutation testing and would not have been caught by review.

---

## ADR-024 — The checkpoint is a high-water mark, and a regression is counted rather than raised

**Status:** accepted (slice 2)

**Context.** `commit` receives the `event_id` of the event it is applying. The natural implementation
is assignment: `self._checkpoint = batch.checkpoint`.

**Decision.** `self._checkpoint = max(self._checkpoint, batch.checkpoint)`, and a `commit` that would
have moved the checkpoint backwards increments a `checkpoint_regressions` counter. An equal checkpoint
is not a regression.

**Why.** "Checkpoint" does not mean "the last event I handled". It means "every event up to here has
been considered", which is the only reading that makes resuming from it safe. Assignment would let a
late event rewind the checkpoint, so a restart re-delivers everything in between — harmless in itself,
because the snapshots deduplicate, but it makes the projection's progress non-monotonic and every lag
graph built on it meaningless.

The counter matters more than the clamp. Clamping is the correct response; clamping *silently* destroys
the only evidence that delivery order has broken. A non-zero value means events are arriving out of
`event_id` order, which is the assumption the entire resume-from-checkpoint design rests on — and a
crash at that moment loses the late event permanently, because replay starts from the checkpoint and
will never deliver it again. That is a data-loss window, and it is worth a metric.

Equality is excluded because duplicates are the normal, healthy case on an at-least-once stream.
Counting them would make the metric fire constantly and nobody would look at it again.

**Cost accepted.** The counter is a plain attribute on the in-memory store rather than part of the
protocol, so a Postgres implementation has to remember to expose an equivalent. Putting it in the
protocol would have been the alternative; it was rejected because the protocol is the atomicity
contract (ADR-020) and mixing observability into it makes the surface test in that ADR much weaker.

**Rejected alternative.** Raising on a regression. Rejected because the clamp is correct and the batch
is otherwise perfectly valid — refusing it would turn a monitoring signal into lost data.

---

## ADR-025 — Every counter is incremented after the commit returns, and a refusal is the exception

**Status:** accepted (slice 2)

**Context.** `ProjectionStats` is what `/projection` serves and what a dashboard is built on. The
counters could be incremented as the event is handled, which reads naturally top to bottom, or only
after the write is durable.

**Decision.** Nothing in `ProjectionStats` moves until `commit` has returned — including
`events_seen`. The one deliberate asymmetry: a message that fails to decode bumps `events_seen` and
`refused` immediately, before returning `Refused`.

**Why.** Counting first gets every number wrong in the same direction at exactly the moment the numbers
matter. A commit that raises is retried by the consumer, so an event counted before the write is
counted once per attempt: a database wobble shows up as a *rise* in applied events, and the one metric
that should reveal a stuck projection instead reports a busy one. Counting after means a failing store
produces a flat line, which is what an operator expects to see.

The asymmetry is not an oversight. A refusal will never be retried — the message is undecodable, so
re-reading it produces the same refusal forever — which means counting it immediately cannot
double-count anything. And `events_seen` has to include refusals because it is the denominator: a
refusal rate is only meaningful against everything that arrived.

**Cost accepted.** The counter updates sit in a block after the commit rather than next to the code
they describe, which reads slightly worse and is easy to "tidy" back. That is why the mutation matrix
includes moving them, and why the comment above the block says what it is protecting.

---

## ADR-026 — A refusal does not advance the checkpoint, and the caller is handed the choice

**Status:** accepted (slice 2)

**Context.** `ingest` decodes a raw payload. A payload that cannot be decoded cannot be applied, and
the tidy handling is catch-log-advance-continue: skip the poison message and keep going.

**Decision.** `ingest` returns a `Refused` value carrying the decoder's message, and does **not**
commit. Nothing in the projector decides what happens next.

**Why.** The checkpoint is an `event_id`, and the reason a message was refused may be that its
`event_id` was missing, non-integer, or zero. There is often no number to advance *to*. Inventing one
— `checkpoint + 1`, say — would guess at the position of an event whose identity is unknown, and if
the guess is low the projection re-reads it forever, while if it is high the projection has silently
skipped events it never saw.

Beyond that, catch-log-advance is a data-loss switch wearing the clothes of error handling. It reads
like robustness and it means "acknowledge the message and lose it". The honest alternatives are to
dead-letter the message and advance, or to stall the partition until somebody looks — and which one is
correct depends on whether the stream is a payment feed or a metrics feed, which is knowledge the
projection does not have. So it reports the fact and returns.

`Refused` is a distinct type rather than an exception for the same reason `Skipped` is (ADR-008): the
caller has to handle it, and a return type says so where a raised exception is an invitation to a bare
`except`.

**Cost accepted.** Every caller of `ingest` must handle three outcomes rather than two, and slice 5's
consumer has to make the dead-letter decision explicitly instead of inheriting a default.

---

## ADR-027 — Subscribers are notified after the commit, and a listener failure propagates

**Status:** accepted (slice 2)

**Context.** `Projector` takes an optional `on_change` callback, which slice 3's fan-out hub will
attach to. It can be called before or after the commit, and its exceptions can be swallowed or allowed
to escape.

**Decision.** The listener is called after `commit` returns, only when at least one position row
actually changed, and an exception from it propagates.

**Why.** Compare the worst cases. Notify-second's is a missed notification, which
reconnect-and-resnapshot repairs, because a subscriber's recovery path is to fetch the current state.
Notify-first's is published state that never existed: the commit then fails, the subscriber has been
told about version 7, and on reconnect it finds version 6 — which it cannot distinguish from a
rollback it should care about. Nothing repairs that.

Only-when-changed follows from ADR-009. A no-op transition (`validated` after `received`) returns the
same `Position` object, so `_build_changed_positions` drops it with an `is not` check and the batch
carries a snapshot and zero rows. Waking every subscriber for a payload containing no visible
difference is not merely wasteful — with slice 4's cache in place it evicts and reloads a hot position
several times per instruction.

Letting the exception escape is safe *because* the projection is idempotent, and that is the whole
argument. The commit already happened, so the consumer's retry re-delivers the event, `project`
returns `DUPLICATE`, and the checkpoint stays where it is. "Let it fail" here is a consequence of the
design rather than an absence of one.

**Cost accepted.** One broken subscriber can stall ingestion, since the listener is called under the
lock (ADR-022). That is the explicit reason slice 3's hub must not await a subscriber's socket: it puts
the event on a bounded queue and returns, and a subscriber that cannot keep up is disconnected rather
than allowed to become back-pressure on the projection.

---

## ADR-028 — The projection publishes staleness and a watermark, never a number called "lag"

**Status:** accepted (slice 2)

**Context.** The obvious health metric for a read model is lag, and the obvious way to compute it is
`now - last_event_at`.

**Decision.** `ProjectionStats` exposes `last_event_id` and `last_event_at` as raw facts and computes
nothing from them. There is no `lag` field, and the projector reads no clock.

**Why.** `now - last_event_at` is *staleness*, and on an idle stream it grows by one second per second.
A quiet Sunday and a dead consumer produce the same rising line, so the metric pages somebody for
silence — and after that happens twice the alert is disabled, which is worse than never having had it.

Real lag is `producer_head_event_id - last_event_id`, and this service cannot compute it, because it
does not know the producer's head. Only the broker does. Publishing an honest pair of facts lets
whatever *does* know join them; publishing a plausible number named "lag" guarantees somebody builds
a dashboard on it and trusts it.

This also keeps the promise made in ADR-010 for the model layer: `updated_at` is event time, and
nothing in the projection path reads a wall clock. A projection that reads a clock produces different
output on replay, which breaks the one property the whole design exists to have.

**Cost accepted.** `/projection` serves two numbers a caller has to interpret rather than one that
looks like an answer, and "how far behind are we?" has no single-field answer in this service.

---

## ADR-029 — A rebuild replays into an empty store, which is why there is no `truncate`

**Status:** accepted (slice 2)

**Context.** Read models get rebuilt: the fold changes, a bug is fixed, a bucket is added. The
mechanism has to be decided before somebody needs it at 3am.

**Decision.** A rebuild constructs a **new, empty** store and replays the stream into it, then swaps.
`PositionStore` offers no `truncate`, no `delete`, and no direct position write, so there is no
in-place rebuild path to reach for.

**Why.** Replaying a full stream into a *populated* store does not produce the same state — and the
failure mode is a good deal more confusing than "wrong numbers". For every instruction, the stored
snapshot's `last_event_id` equals its final event, so on replay only that last event compares equal
and returns `DUPLICATE`; every earlier event compares *less than* and returns `OUT_OF_ORDER`. So a
rebuild-in-place quietly lights up the counter that exists to mean "the ordering guarantee has broken"
(ADR-024) and drives `checkpoint_regressions` at the same time, in the middle of an operation somebody
is performing precisely because they already do not trust the data.

The replay-idempotence test asserts exactly this and documents the reason in its docstring, rather than
weakening the assertion to `DUPLICATE`-or-`OUT_OF_ORDER` — the distinction is real and the test is
where it is written down.

Withholding `truncate` is the same argument as ADR-020, applied to operations instead of correctness.
An interface that offers a hand-edit is one somebody hand-edits under pressure, leaving state that no
replay reproduces and no test explains.

**Cost accepted.** A rebuild needs enough memory or storage for two copies of the read model, and the
swap has to be arranged by the caller. In exchange the old copy keeps serving traffic until the new one
is complete, and a failed rebuild changes nothing at all — which is a better operational story than
in-place truncation, where a rebuild that dies halfway leaves a live read model containing part of the
stream.

**Rejected alternative.** A `truncate()` on the protocol guarded by a comment. Rejected on the same
grounds as the extra write methods: a guarantee that depends on a comment is not a guarantee, and the
surface test in ADR-020 exists to keep this one from being added quietly.

---

## ADR-030 — The fan-out's `publish` contains no `await`, and a test parses the source to keep it so

**Status:** accepted (slice 3)

**Context.** The projector calls one listener when positions change (ADR-027), under the lock that
serialises the whole projection (ADR-022). Slice 3 turns that one call into N live subscribers. The
three-line version of that is `for subscriber in subscribers: await subscriber.send(...)`.

**Decision.** `SubscriptionHub.publish` is `async` in signature and suspends nowhere in its body. It
drops each changed position into a per-subscriber buffer and returns. The sending happens later, on
each subscriber's own task. A test parses `hub.py` with `ast` and fails if an `ast.Await` appears
anywhere inside `publish`.

**Why.** An `await` in that loop is an await on a socket, held under the projection's lock. One
subscriber on a slow link, one browser tab the OS has suspended, one laptop whose lid closed without
closing the TCP connection — and the projection stops applying events until the write buffer drains,
which can be minutes. Every other subscriber then falls behind too, so the symptom presented to an
operator is "the whole read model is stuck" and the cause is one client nobody can identify from the
inside.

The structural test rather than a comment, because this is the failure that testing cannot see. Every
behavioural test in `test_hub.py` would still pass with `await subscriber.send(...)` in the loop:
in-process doubles never actually block, which is the same lesson ADR-023 paid for once already. The
guarantee is a property of the source, so the source is what gets checked.

Parsed rather than substring-searched for a specific reason: the word `await` appears throughout the
module's own prose, including in the paragraph that explains this rule, so `"await" in source` would be
defeated by its own documentation.

The absence of an `await` buys a second property for free. Nothing can interleave between the first
iteration of the fan-out loop and the last, so `_subscriptions` cannot be mutated mid-fan-out and needs
no lock — which is also why `subscribe` and `unsubscribe` are synchronous rather than `async`.

**Cost accepted.** `publish` cannot do anything that requires I/O, so metrics, logging and anything
else useful have to be either synchronous or moved onto the subscriber's task. Every future maintainer
who wants to add "just one await" here has to argue with a test.

**Rejected alternative.** A comment reading "do not await here". Rejected on ADR-018's grounds: a
constraint that only a reader enforces is not enforced, and this one is silent when broken.

---

## ADR-031 — Each subscriber gets a conflating buffer keyed by position, not a queue of events

**Status:** accepted (slice 3)

**Context.** If `publish` cannot send, it has to buffer, and the obvious buffer is
`asyncio.Queue(maxsize=N)` per subscriber.

**Decision.** The buffer is a `dict[PositionKey, Position]`. A second update for a position *replaces*
the buffered one rather than queueing behind it. The replacement is counted as `conflated`.

**Why.** A queue is a backlog of events; what is being published is *state*. A subscriber that is 500
updates behind on a position does not want the 500 intermediate versions — every one of them is
superseded by the time it arrives, and each costs memory to hold, bandwidth to send and CPU on the
client to apply. It wants the current version.

The decisive argument is what the two shapes are bounded *by*. A queue's occupancy is bounded by the
event rate, so a subscriber can be made to fall behind by volume alone, and a burst on one instruction
can evict a client watching an entirely different counterparty. A conflating buffer's occupancy is
bounded by the number of distinct positions the subscriber watches — so a client watching one
counterparty holds at most one entry no matter how many events arrive. A thousand publishes for one
position leave one entry.

A bounded queue also has no acceptable behaviour when it fills. Blocking the producer is forbidden
(ADR-030). Dropping is the only alternative, and `asyncio.Queue` drops the *newest* item, which is the
worst of every option: the subscriber keeps a stale backlog and loses precisely the update it needed.

This is only legitimate because of ADR-009 and ADR-022 together. `Position.version` moves only when
observable content changes, and the projector publishes under a lock, so per-position publish order is
version order — which makes "the last one wins" identical to "the newest one wins". If the payload were
an event stream rather than versioned state, conflation would be data loss, and this decision would be
wrong.

**Cost accepted.** A subscriber cannot see intermediate states, so the hub cannot be used to drive
anything that needs every transition — an audit trail, or a client animating each change. Those want
the event stream, which is upstream of this service and still available.

---

## ADR-032 — Overflow disconnects the subscriber rather than delivering a subset

**Status:** accepted (slice 3)

**Context.** Conflation bounds a buffer by *distinct positions watched*, which is unbounded for a
subscriber that asked for all of them. A wildcard subscriber that stops reading will eventually hold an
entry per position in the book.

**Decision.** Past `max_pending` distinct positions, the subscription is marked `overflowed`, its
buffer is discarded in full, and the next drain raises `SubscriberOverflow`. The flag is never cleared:
overflow is terminal, not a throttle. The hub counts the transition once per subscriber, in
`overflow_disconnects`.

**Why.** The tempting alternative is to keep what fits and carry on. That sends the client a subset of
the changes with nothing indicating anything was dropped, so its view of the dropped positions freezes
at an old version while the others keep updating — which is indistinguishable from those positions
having genuinely stopped changing, a real and interesting state. The client is then wrong and
confident, which is the worst state this service can put a consumer in, and it is exactly the
failure mode ADR-007 exists to prevent one layer down.

Failing loudly is cheap by comparison. A client that knows it has lost data re-snapshots and is correct
again, and SSE reconnects on its own, so the recovery path is one the client already has. Discarding
the buffer rather than trimming it is part of the same argument: a partial buffer is *delivered as
though it were complete*, so keeping it converts a detected loss back into a silent one.

Terminal rather than resumable for the same reason. Letting an overflowed subscription start buffering
again would hand the client a stream with a hole in it and no marker where the hole is.

Worth stating what this does *not* do: a subscriber watching a fixed set of keys with `max_pending` at
or above that count cannot overflow, however long it stops reading. That is what makes disconnection an
acceptable response — the only clients it can reach are wildcard subscribers that have stopped
reading, which is either a client bug or a sign the wildcard subscription was the wrong shape for what
they are doing.

**Cost accepted.** `max_pending` is a guess. It is deliberately a guess with a stated shape — more
positions than a human dashboard shows at once, far fewer than a whole book — rather than a tuned
number, because the value only matters for the clients that should be disconnected anyway.

---

## ADR-033 — A subscription is registered before the snapshot is read, and `mark_delivered` closes the gap

**Status:** accepted (slice 3)

**Context.** A client needs the current state and then every change after it. Both obvious orderings
are wrong, and this is the race that push APIs get wrong most often.

  - Snapshot, then subscribe: every change between the two is lost permanently, and the client's view
    of those positions is frozen with nothing to indicate it.
  - Subscribe, then snapshot: nothing is lost, but the client receives updates its snapshot already
    contains and cannot tell a stale duplicate from a new change.

**Decision.** Subscribe first, read the snapshot, then call `Subscription.mark_delivered(snapshot)`
before streaming. `mark_delivered` raises a per-position watermark of the highest version already sent;
a drain then drops every buffered position whose version is not *strictly* greater. The watermark moves
with `max`, never by assignment.

**Why.** Registering first means the buffer starts filling immediately, so no change can slip through
the gap — that closes the first failure. `mark_delivered` then makes the resulting overlap harmless
rather than visible, which closes the second. Together the client receives every version exactly once,
in order, with no gap and no duplicate, and neither half works without the other.

The watermark is a `version` and not a timestamp, and that is load-bearing: two changes within the same
clock tick would be indistinguishable, so the second would be suppressed as a duplicate. It rests on
ADR-009 — a version bump means the content changed — and on ADR-022, which is what makes per-key
version order the same as per-key publish order.

`max` rather than assignment because two overlapping snapshot reads can complete out of order, and
lowering the watermark would resend a version the client had already applied.

The test suite carries an explicit negative control: the same sequence *without* `mark_delivered`,
asserting that version 1 is then delivered twice. Without it, deleting the call from the hub would
leave every other assertion in the file still passing.

**Cost accepted.** The connection handler has to run three steps in the right order, and getting them
wrong is not a type error. The context manager and the ordering test are the mitigation; a single
`subscribe_and_snapshot` helper would be better still and belongs in slice 5, where the store read
lives.

---

## ADR-034 — The wakeup is an `asyncio.Event`, and `drain` never returns an empty batch

**Status:** accepted (slice 3)

**Context.** The buffer holds the data, so the subscriber's task needs a way to learn there is
something in it. The choices are a queue (which would carry the data too), a condition variable, or an
event.

**Decision.** One `asyncio.Event` per subscription, set by `offer` and cleared by a drain. `drain`
loops: it waits, takes, and only returns when the batch is non-empty. `drain_nowait` is the
non-blocking half and *may* return an empty tuple.

**Why.** An event carries no payload and needs no bound, which is exactly right when the payload is
already in the buffer — it is a doorbell, and a doorbell rung twice before anyone answers is one visit.
That collapse is a feature: it is what makes a burst of publishes cost one wakeup rather than N.

The loop is not defensive padding; the empty wakeup is reachable in two ordinary ways. A publish whose
version the snapshot already covered rings the bell and yields nothing (ADR-033). Two publishes in
quick succession ring it twice and the first drain takes both. Without the loop, `drain` returns `()`,
and slice 5's `async for` writes an empty frame down a socket — or worse, a caller reads `()` as
end-of-stream.

`drain_nowait` exists as well as `drain` because the tests need to assert "nothing was delivered"
without waiting for a timeout, and asserting on the absence of an event is otherwise a choice between a
sleep and a flaky test. It is also the shape slice 5 wants for the initial catch-up read.

**Cost accepted.** Two drain methods with subtly different contracts — one may return empty, one may
not. The names carry the difference and the docstrings state it, but a caller can still pick the wrong
one.

---

## ADR-035 — `keys=None` means every position and `keys=[]` means none

**Status:** accepted (slice 3)

**Context.** A subscriber either watches specific positions or all of them, and the filter has to
express both.

**Decision.** `None` means every position. A set — including the empty set — means exactly those keys.
An empty key set therefore receives nothing.

**Why.** An empty list is what a caller produces *by accident*: a filter that matched nothing, a query
parameter that parsed to an empty list, a lookup that returned no keys. Treating that as a wildcard
would stream an entire book to a client that asked for nothing — both an exposure and, per ADR-032, the
only subscription shape that can overflow. `None` has to be passed deliberately and cannot arrive from
an empty collection.

The general form of the rule is worth keeping: when a permissive and a restrictive reading of the same
value are both plausible, the accident-shaped value should get the restrictive one.

Filtering lives on the subscription rather than in a key-to-subscribers index on the hub. With a
handful of subscribers a membership test per subscriber beats maintaining an index across every
subscribe and unsubscribe, and the fan-out loop stays honest — it visits every subscriber exactly once
and cannot skip one because an index was stale.

**Cost accepted.** Fan-out is O(subscribers × positions per batch). At thousands of subscribers the
index becomes the right trade, and this is the decision to revisit first.

---

## ADR-036 — Overflow raises and closing returns, so a stream's two endings are distinguishable

**Status:** accepted (slice 3)

**Context.** A subscription's stream ends in two ways: the client disconnected, or the client fell too
far behind and lost data. Both terminate the iteration.

**Decision.** `Subscription.updates()` returns normally when the subscription is closed and propagates
`SubscriberOverflow` when it overflowed. `drain_nowait` raises `SubscriptionClosed` rather than
returning `()` on a closed subscription.

**Why.** If both endings returned, the handler could not tell them apart, and the only correct response
to the second — tell the client to re-snapshot — would never happen. A data-loss event would be
indistinguishable from a browser tab closing, which is the most common event in the system.

Same argument for `SubscriptionClosed` versus an empty tuple: "the stream has ended" and "there is
nothing new right now" are different facts, and a caller that confuses them either spins or hangs. This
is the mirror image of ADR-008 — there, a *routine* outcome was made a returned value rather than an
exception; here an *unrecoverable* one is made an exception rather than a returned value. The principle
is the same in both directions: the type should tell the caller whether carrying on is an option.

**Cost accepted.** Two exception types in a module whose whole job is to not raise. Both are raised on
the subscriber's own task, never on the projection's, so neither can reach the projector.

---

## ADR-037 — The hub holds `Position` objects, and a subscription is a context manager

**Status:** accepted (slice 3)

**Context.** Two smaller decisions, both about where a responsibility sits.

**Decision.** The buffer holds `Position` model objects, not serialised frames. `SubscriptionHub`
offers a `subscription()` context manager that unsubscribes in a `finally`, and `publish` prunes
subscriptions that were closed without being unsubscribed.

**Why — the model object.** Serialising once in the hub would be cheaper than once per subscriber, and
it is still wrong: it would force a wire format on a layer that `test_layering.py` allows no
third-party imports, and slice 4's cache and slice 5's serialiser both want the object. Conflation also
needs `key` and `version`, which a frame would have to carry separately or be re-parsed for.

**Why — the context manager.** The caller is a connection handler, and a connection handler almost
never ends by reaching its last line. It ends with a `CancelledError` when the client disconnects, or
with an exception from a socket write. `finally` is the only construct that survives both — a
`try/except Exception` would miss cancellation, which is how *every* real SSE connection ends. Putting
it in the hub means every call site gets it rather than the one that remembered.

A leaked subscription is not a small leak: it keeps buffering forever, holding `Position` objects alive
for positions nobody is watching and being visited on every publish. The prune in `publish` is the
second line of defence, for a subscription closed directly without going through `unsubscribe`. It is
guarded by an `any(...)` check so the common case — nothing closed — costs a scan rather than a rebuilt
list.

**Cost accepted.** The prune scans the subscriber list on every publish. It is O(subscribers) against a
fan-out that is already O(subscribers × positions), so it changes nothing asymptotically, and it is the
same list the loop below it walks anyway.

---

## ADR-038 — The cache is deliberately not a `PositionStore`, and a test pins its public surface

**Status:** accepted (slice 4)

**Context.** `CachedPositions` wraps a `PositionStore` and serves reads from memory. It would take five
passthrough methods — `commit`, `all_positions`, `snapshot_for`, `checkpoint`, `snapshot_count` — to
make it satisfy the `PositionStore` protocol, and doing so reads like an improvement: the api layer
could then accept a single type and be handed either the store or the cached store, and the wiring
would get shorter.

**Decision.** The cache exposes `position_for`, `refresh`, `on_change`, `invalidate`, `clear` and its
report, and nothing else. It cannot be substituted for a store. `tests/test_cache.py` asserts the exact
set of public attribute names, so adding one means arguing with a failing test rather than getting a
green build.

**Why.** The moment the cache is substitutable, this line compiles:

```python
projector = Projector(CachedPositions(store))
```

and the projection's read-modify-write cycle starts reading cached positions.
`_build_changed_positions` reads a position, applies the event's deltas to it, and commits the result.
Read a version that is one change out of date and the committed position is short by exactly that
change — and then that wrong value is the store's truth, is served to every reader, and is reproduced
identically on every replay. Nothing detects it: no counter moves, no exception is raised, the event is
`APPLIED`, and the projector's `asyncio.Lock` cannot help because the lock serialises access to the
*store* and the stale value did not come from there.

That is a silent money bug arriving via a refactor that looks like tidying up, which is the worst
combination available: the change is small, the diff is pleasant to review, and the symptom appears
days later as a reconciliation break with no obvious cause.

**Cost accepted.** The api layer takes two collaborators where it could have taken one, and a
`PositionStore` and a `CachedPositions` cannot be swapped in a test. That is the point: they are not
interchangeable, and a type system that said they were would be lying.

**Rejected alternative.** Implement the protocol and document the danger in a comment. Comments do not
fail builds, and the reader who would benefit most from this one is the reader who is deleting
duplication and has not read it. A structural test is the only form of the warning that survives.

---

## ADR-039 — Exactly one store method is cached, and the other four are named as omissions

**Status:** accepted (slice 4)

**Context.** `PositionStore` has five read/write methods. A cache could plausibly front several of
them.

**Decision.** Only `position_for` is cached. The omissions are listed in the module docstring with the
reason for each.

**Why.**

- `all_positions` is invalidated by any change to any position. On a live stream that is a cache with a
  near-100% miss rate: memory cost, coherence risk, no benefit.
- `snapshot_for` has exactly one caller, the projector, and per ADR-038 the projector must never read
  through a cache.
- `checkpoint` is the number that answers "have you caught up yet?" for slice 5's read-your-writes
  parameter. Caching it is a contradiction in terms — a stale checkpoint makes the freshness check
  itself unreliable, which is worse than not having one.
- `commit` is a write, and a write-through cache would put the cache on the path that has to be atomic.

**Cost accepted.** A future "list all positions" endpoint gets no help from this cache and will need its
own answer (pagination, or a periodically rebuilt materialised list). Naming that now is cheaper than
discovering it after a cache has been wired into the wrong place.

---

## ADR-040 — Single-flight is a task register plus `asyncio.shield`, not a bare `await`

**Status:** accepted (slice 4)

**Context.** A hot position is evicted or has never been read, fifty requests miss at the same moment,
and fifty identical reads reach the store. The load the cache exists to prevent arrives all at once, at
exactly the moment the position is most popular. The fix is single-flight: one load per key, every other
caller waits on it.

**Decision.** `_in_flight: dict[PositionKey, asyncio.Task]`. The first miss creates a task; every
concurrent miss for the same key awaits `asyncio.shield(load)`; the task pops its own key in a
`finally`.

**Why `create_task` and not "the first caller does the work inline".** A leader-does-the-work design
needs a `Future` per key, manual `set_result`/`set_exception` propagation to the waiters, and its own
answer to "what happens when the leader is cancelled" — which is that the followers wait forever unless
the leader has a `finally` that resolves the future with something. A task is the stdlib's own version
of that machinery, already correct.

**Why `shield` and not `await load`.** ⚠️ Cancelling a task cancels the future it is awaiting. With a
bare `await load`, one HTTP client disconnecting — the single most common event in a web service —
cancels the *shared* load, and every other waiter on that key receives a `CancelledError` for a request
it had nothing to do with. The symptom is intermittent, load-dependent, concentrated on the most
popular keys, and completely invisible to any sequential test: one impatient browser tab failing forty
unrelated requests. `shield` gives each waiter a cancellable wrapper around an uncancellable load.

A second consequence, and a good one: because the load is nobody's child, it finishes even if every
caller has left, so the work the departed caller paid for still warms the cache.

**Why the task is referenced from `_in_flight`.** An `asyncio.Task` with no strong reference can be
garbage-collected mid-flight. The register is that reference; it is not only bookkeeping.

**Cost accepted.** A task allocation per cache miss, and a done-callback per task (ADR-046). Both are
cheap next to the store read they de-duplicate, and both cost nothing on a hit.

---

## ADR-041 — An absence is cached as a value, which makes `dict.get` unusable for miss detection

**Status:** accepted (slice 4)

**Context.** `position_for` returns `None` for a counterparty nothing has contributed to, and slice 5
turns that into a 404. A cache that stores only hits therefore forwards every 404 to the store.

**Decision.** `None` is cached as a value. Every miss check is an explicit membership test —
`if key in self._entries` — never `self._entries.get(key)`. Hits and negative hits are counted
separately.

**Why.** Without negative caching, the cheapest way to put load on this service is to ask about
counterparties that do not exist: no cache in the way, one store read per request, and nothing in the
metrics that looks like an attack rather than traffic. Caching the absence makes the second such
request as cheap as the second request for anything else.

⚠️ **The consequence is a trap that reads as tidy code.** `self._entries.get(key)` returns `None` for
"cached absence" and `None` for "not cached at all", so a `.get`-based miss check silently disables
negative caching while the counters keep incrementing as though it worked. It is the kind of change a
linter would suggest. Hence the explicit membership tests, a comment at each one, and a mutation in the
matrix that swaps one in.

Two counters rather than one because they describe different callers: a high `hits` is a cache doing its
job, a high `negative_hits` is a client polling for something that has not happened or somebody
enumerating names. Summed, neither is visible.

**Cost accepted.** The key space becomes "strings a caller can send" rather than "positions that
exist", which is what forces ADR-042's bound. An absence cached just before the first event for that
counterparty is served until the change notification arrives — bounded by the notification, not by a
timeout, and version 0 as the sentinel is what makes the upgrade automatic.

---

## ADR-042 — The cache is bounded and evicts least-recently-*requested*

**Status:** accepted (slice 4)

**Context.** With absences cached (ADR-041), an unbounded map is a memory leak with a remote trigger:
any caller can add entries to it by asking about names nobody has ever used.

**Decision.** `DEFAULT_MAX_ENTRIES = 4096`, LRU eviction on insert, `max_entries < 1` refused at
construction. Absences count against the bound like anything else. A read promotes; a refresh
(ADR-044) does not.

**Why 4096, and why the test asserts a floor rather than the number.** It is a judgement, not a
measurement: comfortably more than the working set of a dashboard or a handful of API consumers,
comfortably less than a whole book, and any value that is not "unlimited" answers the security question.
The test pins that the constructor default *is* the exported constant and that the constant is at least
1024 — because an assertion of "the default is the constant" is satisfied by a constant of 8, and a
cache that evicts an ordinary working set on every read looks like a cache that does not work rather
than like a failure.

**Why a zero bound is refused rather than accepted.** "Caching disabled" is expressed by not
constructing a cache. A `max_entries=0` that silently missed on every read would be a configuration
typo that manifests as a performance incident with no error anywhere.

**Cost accepted.** `OrderedDict` bookkeeping on every hit, and a working set larger than the bound
thrashes. Both are visible: `evictions` climbing alongside a falling `hit_rate` is the signal to raise
the number.

---

## ADR-043 — Invalidation comes from the change stream by version; there is no clock and no TTL

**Status:** accepted (slice 4)

**Context.** The default answer for cache coherence is a TTL. This service has something better
available: the projector already publishes every changed position to a `ChangeListener` (ADR-021), with
the new value attached.

**Decision.** No TTL, no clock, no `time` import in the package. `CachedPositions.on_change` is a
`ChangeListener`; entries are updated when a change arrives, compared by `version`. A cached absence
compares as `UNCACHED_VERSION = 0`.

**Why.** A TTL is what you use when you cannot know when the underlying value changed. Given a precise
signal, a TTL is a decision to serve a known-wrong answer for up to `ttl` seconds — strictly worse on
both axes at once: staler than necessary *and* more store reads than necessary. It also introduces the
only thing this repository has managed to avoid entirely so far, which is a test that depends on time
passing.

**Why version 0 is a safe sentinel.** `_empty_position` starts at version 0 and `with_deltas` bumps
before anything is committed, so version 0 is never published. Every real change is therefore strictly
newer than a cached absence, and "this counterparty does not exist" upgrades to "here it is" with no
special case.

**Why the comparison exists at all, given that publish order is version order.** Per-key ordering holds
only because the projector notifies while holding its lock (ADR-022) — a fact the cache cannot verify
from where it sits. A cache that trusts an invariant it cannot check is a cache that starts going
backwards silently on the day the invariant moves; `refreshes_dropped_stale` is the alarm for that day,
and it should read zero forever.

**Cost accepted.** The cache is only as correct as the notification path. If a change is committed
without notifying — an offline rebuild, a manual repair, a direct write to the store — the cache does
not find out, because there is no timeout to save it. That is a real operational hazard, and it is why
`invalidate` and `clear` exist as an explicit rescue path (ADR-045).

---

## ADR-044 — A refresh updates in place, never inserts, and never promotes

**Status:** accepted (slice 4)

**Context.** A change notification arrives for a position. Three plausible behaviours: drop the entry,
update it, or insert it if absent.

**Decision.** If the key is cached and the incoming version is strictly newer, the entry is overwritten
in place. If the key is not cached, the notification is dropped and counted
(`refreshes_dropped_cold`). Either way the entry's position in the LRU order is unchanged.

**Why update rather than drop.** Dropping creates a miss at the exact moment the position is being
written to, which is when it is hottest. A busy position would stampede on every change, and
single-flight would spend its life absorbing traffic the cache created for itself. The notification
already carries the new value, so there is nothing to reload.

**Why never insert.** ⚠️ *A cache is populated by reads and updated by writes; it is never populated by
writes.* Inserting looks like free warming — the value is right there — and it hands the event stream
control of what the cache holds. An hour of activity on positions nobody queries would evict the working
set the API is actually serving. Since a live stream touches every position while the cache holds only
what was read, most notifications land in the cold-drop branch, and that is correct rather than
wasteful.

**Why never promote.** Recency has to mean "recently *requested*", not "recently changed". Promoting on
refresh would keep a churning, unread position resident forever while evicting a position read every
second for being calm — so the hit rate would degrade as the *stream* got busier rather than as the
*traffic* changed, which inverts the signal an operator would use to size the cache.

**Cost accepted.** A position that changes constantly and is read rarely is re-read on each of those
rare reads. That is the intended trade: reads pay for reads.

---

## ADR-045 — The read-through/invalidate race is closed by a held-change map and a generation counter

**Status:** accepted (slice 4)

**Context.** Every read-through cache with an invalidation signal has the same race, and it is the
classic bug in the pattern:

1. a load reads version 6 from the store,
2. a change to version 7 lands while the read is still in flight,
3. the refresh finds nothing cached and correctly declines to insert (ADR-044),
4. the load completes and stores version 6.

The cache is now stale with no signal that it is, and stays stale until the next change to that
position — for a quiet position, indefinitely.

**Decision.** Two mechanisms, for two different failures.

- `_changed_while_loading`: a refresh for a key with a load in flight is held rather than dropped, and
  at the end of the load the newer of (held change, store result) wins. One slot per key, keeping the
  newest, cleared in the load's `finally`.
- `_generation`: an integer bumped by `clear` and by `invalidate`. `position_for` captures it when it
  decides to load and passes it in; if it has changed by the time the load finishes, the value is
  returned to the caller but not stored (`loads_discarded`).

**Why the held change is compared and not preferred.** Both interleavings are legitimate: the commit may
land before the store read or after it. If before, the read already returned version 7 and the
notification is a duplicate — so "the held change wins" would push the cache *backwards* on an ordinary
interleaving. Version comparison is right in both orders, which means the code does not depend on
knowing which one happened.

**Why the generation is captured in `position_for` and not on `_load`'s first line.** ⚠️
`asyncio.create_task` *schedules* a coroutine; it does not start it. A generation read inside `_load`
therefore happens some number of event-loop ticks after the decision to load, and a `clear()` landing in
that window would be captured as the load's own starting generation — so the load would consider itself
current and refill the cache it was supposed to be discarded by. This was found by a test that cleared
the cache immediately after observing `in_flight == 1`, which is exactly the window; the matrix now
carries a mutation that reopens it.

**Why one global generation rather than one per key.** A single-key `invalidate` also discards other
keys' loads in flight. That is wasteful — they are re-read on the next request — and never wrong, and
invalidation is an operational rescue path rather than a hot one. A per-key counter would be more
precise and would add a second unbounded-by-caller-input map to keep bounded, on a path that gets no
traffic.

**Cost accepted.** Two extra pieces of state whose whole purpose is the window around one `await`, and
neither is observable except with a store double that can be held mid-read. That is what
`ReadCountingStore`'s gate is for, and it is why ADR-023's "a double that never suspends measures
nothing" is quoted at the top of `test_cache.py`.

---

## ADR-046 — A failed read is never cached, and the `except` clause is narrow on purpose

**Status:** accepted (slice 4)

**Context.** The store read inside `_load` can raise. The tempting shortcut is one line:
`except Exception: return None`.

**Decision.** A failure is counted (`load_failures`) and re-raised to every waiter. Nothing is cached,
positively or negatively. `_in_flight` and `_changed_while_loading` are cleared in a `finally`. The
clause catches `Exception`, not `BaseException`. Loads that fail with nobody left to hear about it have
their exception retrieved by a done-callback.

**Why nothing is cached.** ⚠️ `except: return None` converts a five-second database wobble into a 404
that lasts as long as the entry. A client asking about a counterparty with real exposure is told, with
a perfectly ordinary and unalarming status code, that it does not exist — and the error rate *drops*
while the service is lying. Failing the request is worse for one caller and honest for all of them, and
the retry lands on a cache that has learnt nothing false.

**Why the `finally`.** Without it, the in-flight register keeps a finished, failed task, and every later
request for that key awaits it and receives the same exception — forever, for that one key, with the
store perfectly healthy. One transient error would permanently poison one counterparty.

**Why `Exception` and not `BaseException`.** A `CancelledError` is this task being torn down, not the
store being broken. Counting it as a store failure would make every orderly shutdown add to the failure
count, so a routine deploy would look on the dashboard exactly like a database outage. The test raises a
purpose-built `BaseException` subclass rather than a real `KeyboardInterrupt`, because pytest reads a
`KeyboardInterrupt` as a request to abandon the session.

**Why the done-callback.** When a load fails and every waiter has already gone away, nobody retrieves
the exception, so asyncio logs "Task exception was never retrieved" when the task is collected. The
failure is already counted and the caller that cared has already had it raised, so the line is noise —
but it is *alarming* noise, arriving at collection time rather than failure time, with a traceback and
no request context, and the cost of noise like that is paid on the day it hides something real. The
callback is a module-level function rather than a closure so that it does not keep the cache alive, and
it guards on `not task.cancelled()` because asking a cancelled task for its exception raises.

**Cost accepted.** A store outage becomes a wave of 5xx rather than a wave of 404s, and the cache offers
no protection during it. That is the correct failure mode for a service whose answers are money.

---

## ADR-047 — `combine_listeners` runs every listener, then raises

**Status:** accepted (slice 4)

**Context.** Slice 4 is the first point at which two things want the same change notification: the cache
wants to refresh its entries, and the hub wants to wake its subscribers. `Projector` takes exactly one
`ChangeListener`.

**Decision.** A `combine_listeners(*listeners)` helper in `projection/`, beside the `ChangeListener`
type. It awaits each listener in order, collects failures rather than stopping, and afterwards re-raises
a single failure unwrapped or raises an `ExceptionGroup`. `Projector` still takes one listener.

**Why not a `list[ChangeListener]` parameter on the projector.** That would move two policy decisions —
what order, and what to do when one fails — into the projector, which has no information with which to
make either. Composed at the call site, both are the caller's and both are visible in one line.

**Why every listener runs.** ⚠️ The obvious implementation is a bare loop, which stops at the first
failure. With the wiring above that means: the cache refresh raises, so `hub.publish` never runs, so no
subscriber is told about the change. The projector propagates, the consumer retries the event, and
`project` returns `DUPLICATE` — correctly, because the commit already happened — so the notification is
never regenerated. One transient failure in the cache costs every live subscriber that update,
permanently. Running all of them keeps a failure in one consumer from being data loss in another.

**Why a single failure is re-raised unwrapped.** So that a caller can still catch the type it expected,
and so that a traceback is not one frame deeper for having gone through a fan-out. One failure is the
common case and should not be harder to handle than no fan-out at all.

**Why `Exception` and not `BaseException`.** "Collect it and carry on" is right for a listener that
failed and exactly wrong for a task that has been told to stop.

**On the ordering.** The cache is listed first by convention, so that a read taken after a notification
cannot return an older version than the notification carried. Today that ordering is *not*
load-bearing, and the docstring says so rather than claiming a hazard that does not exist: neither
listener suspends (ADR-030 for the hub, the same rule for `on_change`), so nothing can interleave
between them and no caller can observe the intermediate state. The convention costs nothing and is the
order that stays correct on the day one of them grows an `await`.

**Cost accepted.** A caller that wants fail-fast fan-out cannot get it from this helper. Nobody wants
that here, and the reason is written down.

---

## ADR-048 — `hit_rate` is `None` before any traffic, and the report's key set never changes

**Status:** accepted (slice 4)

**Context.** `as_dict()` feeds slice 5's `/cache` endpoint, which feeds a dashboard.

**Decision.** `hit_rate` returns `None` when no request has been served, not `0.0`. Every counter
appears in the report on every call, including the zeros. A negative hit counts as a hit for the rate.
`__repr__` renders a null rate as `-`.

**Why null rather than zero.** A cache with no traffic and a cache that is failing to hit both report 0%
if the default is zero, and the second one is an incident. A null renders as a gap, which is what "no
data" should look like — ADR-028's argument applied to a different number.

**Why every key, every time.** An absent key makes a dashboard panel disappear exactly when the thing it
watches stops happening, which is indistinguishable from the panel being broken. Same reason
`ProjectionStats` pre-populates every skip reason.

**Why a negative hit counts as a hit.** A served 404 is a store read avoided, which is precisely what
the rate measures. It is also reported separately, so the two can be told apart.

**Cost accepted.** Consumers must handle a nullable number. That is one `if` in the dashboard query
against a metric that cannot mislead.

---

## ADR-049 — The application is built by `create_app(service)`, not defined at import time

**Status:** accepted (slice 5)

**Context.** Every FastAPI tutorial opens with `app = FastAPI()` at module level, and every route is
registered against that global. This service needs an app that can be handed a different store, and a
test suite in which one test cannot see another's positions.

**Decision.** `create_app(service: Service | None = None) -> FastAPI` builds and returns the app. The
`Service` it is given is the composition root; omitted, it builds an in-memory one. Nothing is
constructed at import time. Deployment runs `uvicorn position_view.api:create_app --factory`.

**Why not a module-level app.** A module-level app makes the application a side effect of an import,
which has two consequences that only look small. The store is created when the module is first
imported, so every test in the process shares one — and a suite where tests can see each other's data
is a suite that passes or fails depending on file ordering, which is the failure mode that makes
people stop trusting tests rather than fix them. And there is no way to substitute a store without an
environment variable and an `if`, so the production wiring and the test wiring diverge at exactly the
point where they most need to be the same.

**Why not `Depends` with `app.state`.** FastAPI's own idiom for injecting a collaborator is a
dependency reading `request.app.state.service`, overridden in tests with
`app.dependency_overrides`. It exists for a real reason and this service does not need it, because
the substitution happens one level up in the `Service` the factory receives. What the closure buys
instead is types: `app.state` is an untyped namespace, so `request.app.state.service` is `Any` and
`mypy --strict` cannot tell you that a renamed method broke a route. The closed-over variable is a
`Service` the whole way through. Given the choice between the framework's extension point and a
version the type checker can see, with no requirement for the extension point, this takes the types.

**Consequence worth stating.** There is no test-only branch anywhere in `app.py`. The default
argument is not a test hook — it is what makes `--factory` a one-liner — and it is the same code path
production takes.

**Cost accepted.** A reader looking for `app` will not find one, and any tool that expects an
importable ASGI callable needs `--factory` or a two-line wrapper. That is a one-time cost in a
deployment command, paid once, against a class of test flakiness that is paid forever.

---

## ADR-050 — Freshness is a query parameter served by a gate of futures, and a timeout is a 503

**Status:** accepted (slice 5)

**Context.** This is a CQRS read model. The write side accepts an instruction and returns; this side
finds out when the event arrives. So the client's very next read — the page refresh straight after the
button click — is the one read most likely to be stale, and "eventually consistent" is no comfort at
all to the person looking at a screen that has not changed. This is the standard complaint about CQRS
and it is not inherent to the pattern: it is what happens when the read side offers no way to say
*how* fresh an answer has to be.

**Decision.** Both read routes accept `?await_event_id=` and `?timeout_ms=`. `POST /events` returns
the `checkpoint` the projection reached, which is what a client passes back. A `CheckpointGate` holds
one `asyncio.Future` per waiting request; `Service.ingest` calls `gate.advance(checkpoint)` after the
projector has committed and the listeners have run. If the point is already passed, the read proceeds
without suspending at all. If the wait times out, the response is **503 with `Retry-After`**, never a
stale 200.

**Why a future per waiter rather than a polling loop.** `while await store.checkpoint() < event_id:
await sleep(0.005)` works and has three costs that all get worse exactly when the service is busy: a
store read per waiter per tick; a latency floor of half a tick even when the data is already there;
and a `sleep` in the request path, which puts a clock in every test that touches freshness, so the
suite becomes either slow or flaky. Nothing polls here — `advance` is called once per ingested event
by the code that ingested it.

**Why the counter is monotonic (`max`, not assignment).** `advance` is called with whatever the store
reports after an event, and two concurrent ingests can produce those reports out of order. A plain
assignment would let the gate go backwards, and a reader already released by the higher value would be
re-blocked by the lower one — a request that hangs for a reason no log line could explain.

**Why 503 and not a stale 200 with a header.** The tempting alternative is to answer anyway and add
"this may be behind", because an answer feels more useful than a failure. It is not: a client that
asked for its own write back and receives an older version with a 200 will render it, and the person
watching sees the number they just changed unchanged, and tries again — which is how one stale read
becomes a duplicate instruction. A 503 is a thing a client can retry, log and alert on. A stale 200 is
a thing it cannot even detect. 503 rather than 504 because nothing upstream timed out; this service is
temporarily unable to handle the request, which is precisely what 503 means.

**The hazard, and why it is survivable.** Some load balancers eject a backend that returns 5xx.
Ejection is driven by the health check, and `/healthz` deliberately knows nothing about the
projection's progress (ADR-053), so a burst of freshness timeouts cannot take a healthy node out of
rotation.

**Cost accepted.** A request can now occupy a connection and a worker slot while waiting. That is
what ADR-059's ceiling is for.

---

## ADR-051 — Pydantic validates what belongs to HTTP; the event decoder validates events

**Status:** accepted (slice 5)

**Context.** `POST /events` is the consumer seam. The obvious FastAPI shape is a pydantic model of the
envelope, which would give free validation, a documented request schema and typed access to fields.

**Decision.** The body is annotated `dict[str, Any]` and handed straight to `decode_envelope`. Path
parameters, query parameters and every *response* are pydantic-validated as usual. `Refused` becomes a
400 carrying the decoder's own message; `Skipped` becomes a **200** with a reason.

**Why not a pydantic model in front.** `decode_envelope` already *is* this service's wire contract. It
is where `schema_version` is checked, where `1 == 1.0 == True` was found to be a real bug (ADR-013),
where money is refused rather than rounded, and it carries 127 tests. A pydantic model would be a
second, subtly different definition of the same contract — and the one that rejected a payload first
would be the one with no tests behind it. Worse, its error shape would be pydantic's rather than the
decoder's, so the reason a real event was refused would be reported in a vocabulary that appears
nowhere in the projection's own logs. That is the single most expensive kind of divergence at 3am.

**Why `dict[str, Any]` is not "no validation".** A JSON array still gets a 422, because "the body is
an object" is a fact about the transport and FastAPI owns the transport. What is inside the object
belongs to the model.

**Why a skip is a 200.** A duplicate is at-least-once delivery working as designed. A 4xx would make
every redelivery look like a client bug, and a consumer that retried on 4xx would loop forever. The
`reason` field is what lets an operator tell "the broker sent it twice" from "nothing happened".

**Cost accepted.** `/events` has no request schema in the OpenAPI document, so a generated client gets
no type for the body. The consumers that matter are a broker consumer and an operator with `curl`,
neither of which reads the generated client, and `model/envelope.py` is where the shape is documented.

---

## ADR-052 — Money crosses the wire as a JSON string, never as a JSON number

**Status:** accepted (slice 5)

**Context.** The model refuses to be approximately right about money: `parse_amount` traps `Inexact`
rather than rounding. Serialisation is the last place that guarantee can be given away.

**Decision.** Every amount is a `str` in every response and every SSE frame, formatted with `f"{d:f}"`.
The instruction count next to it stays a JSON number.

**Why.** A JSON number is a double in every mainstream parser. `JSON.parse('{"amount": 1500.75}')` in
a browser produces an IEEE 754 binary float, and the client is then one addition away from
`1500.7500000000001` — or, on a large book, away from losing a cent in a place nobody can trace. The
service would have spent four modules being exact and handed it away in the layer least likely to be
reviewed for arithmetic.

**Why `f"{d:f}"` and not `str(d)`.** `str(Decimal("1E+3"))` is `"1E+3"`. Values with a large exponent
arrive naturally from arithmetic on quantized decimals, so this is not a hypothetical shape, and a
client parsing `"1.5E+3"` as money is a client that will one day parse it wrong. Fixed-point
formatting has no exponential form to fall into.

**Why the count stays a number.** It is an integer count of instructions, it cannot be fractional, and
no parser will mangle it. Making it a string too would be consistency for its own sake at the cost of
every consumer having to parse something that was never at risk.

**Cost accepted.** Every consumer has to parse the string, and a naive one will call `parseFloat` and
be back where it started. That is a consumer bug this API cannot prevent — but a string makes it a
*choice*, visible in their code, rather than something the transport did to them silently.

---

## ADR-053 — `/healthz` is liveness only, and there is deliberately no `/readyz`

**Status:** accepted (slice 5)

**Context.** An orchestrator wants a probe. The instinct is to make it meaningful by having it check
the store, and to add a readiness probe that reports whether the projection is caught up.

**Decision.** `/healthz` returns `{"status": "ok"}` and touches nothing. There is no `/readyz`.
`/projection` publishes `last_event_id`, `checkpoint`, `instructions_remembered` and the freshness
counters as raw facts.

**Why the health check verifies nothing.** A health check that verifies its dependencies turns one
broken dependency into an outage. If `/healthz` read the store, a slow store would fail every
replica's check simultaneously, the orchestrator would restart all of them together, and a service
that could still serve cached reads would serve nothing — while the restarts added load to the
dependency that was already struggling. The store's health belongs in the store's own alerting, where
one alert fires instead of every replica flapping.

**Why no readiness probe.** This service genuinely cannot know whether it is caught up. Real lag is
`producer_head_event_id - last_event_id`, and only the broker knows the head. A readiness probe would
therefore have to invent a threshold — "ready if staleness < 30s" — which on a quiet stream marks a
perfectly caught-up projection as unready and takes a healthy service out of rotation for the crime of
having nothing to do. Publishing the two raw numbers lets whoever can see both sides do the
subtraction, which is the only place the answer exists.

**Cost accepted.** A deployment that wants a readiness gate has to build it from `/projection` plus its
own knowledge of the stream. That is more work for the operator and it is work only the operator can
do correctly. An absence with a reason is documented here so it is not mistaken for an omission, and
`test_there_is_no_readiness_probe` asserts the 404 so nobody adds one by accident.

---

## ADR-054 — The subscribe/snapshot handover happens inside the SSE generator

**Status:** accepted (slice 5)

**Context.** ADR-033 fixed the order: subscribe, snapshot, `mark_delivered`. *Where* that sequence
runs is a separate decision, because it determines what can be reported when the snapshot read fails.

**Decision.** All three steps run inside `position_stream`, after the response has begun. The
subscription is held by a `with` block. A snapshot failure is reported as an `event: error` frame on a
200 that has already started, and the generator returns.

**Why not in the route function.** Doing the handover before returning the `StreamingResponse` would
allow a clean 500 when the store fails, because the response would not have started yet. It would also
allow a subscription created for a response that is never streamed: a client that disconnects in that
window leaves a subscriber registered forever, being offered every change for the life of the process.
Inside the generator, the `with` block's `finally` runs however the stream ends — normal return, client
disconnect (a `CancelledError` thrown in by the server), or a failed snapshot. A leak that grows with
client churn is worse than a degraded error path.

**Why an `error` frame rather than closing the connection.** Once the response has started there is no
status code left to set, so the choice is "tell the client something went wrong" or "close and let it
guess". A client that receives an `error` frame knows to retry; a client that sees a clean close
renders an empty book, which on a settlement screen looks like good news.

**Why the `except` is broad here when ADR-046 made it narrow in the cache.** Different question. The
cache narrows because it must not *cache* the consequence of an unexpected failure. This clause caches
nothing and swallows nothing: it converts any failure into a frame that names it and then stops. A
narrow clause would let an unexpected exception escape into the ASGI server, where it becomes a
dropped connection — the outcome this decision exists to avoid.

**Cost accepted.** A store outage on a new subscription is a 200 with an error frame rather than a 500,
so it will not appear in a 5xx rate. `test_a_failed_snapshot_is_reported_as_an_error_frame` pins the
frame, and the overflow frame gives the same class of client-visible signal for the same reason.

---

## ADR-055 — The live stream is at `/stream/positions`, not `/positions/stream`

**Status:** accepted (slice 5)

**Context.** `/positions/stream` reads better and groups the endpoint with the resource it serves.

**Decision.** The SSE route is `/stream/positions`. The two read routes keep `/positions/...`.

**Why.** `/positions/{counterparty}` and `/positions/stream` are the same shape, so they compete.
FastAPI matches in registration order, which makes correctness depend on the order routes happen to
be declared in — and no ordering saves it from a counterparty legitimately named `stream`, whose
positions would become unreachable. The failure would present as a 404 for a position that exists,
which is a long afternoon for whoever gets the ticket. Moving the stream to its own prefix means the
paths cannot overlap at all, and `test_the_stream_path_is_not_shadowed_by_a_counterparty` asserts the
absence so a later "tidy up" cannot reintroduce it.

**Related absence: `Last-Event-ID`.** SSE has a resumption mechanism and this service cannot honour
it, because it keeps no history of changes — only current state. A reconnecting client gets a fresh
snapshot, which is correct by construction and is the reason the conflating buffer exists (ADR-029).
Advertising a resumption we would then ignore would be worse than not advertising one: the client
would trust an id we discard.

**Cost accepted.** The URL groups by mechanism rather than by resource, so a reader scanning the route
list sees positions in two places. One sentence in the docs against a class of shadowing bug that
cannot be tested away once the paths overlap.

---

## ADR-056 — SSE is tested three ways, because `httpx.ASGITransport` buffers

**Status:** accepted (slice 5)

**Context.** Every other route in this service is tested in-process through `httpx.ASGITransport`: the
real app, the real routing, the real validation, no port and no process. The obvious thing is to test
the stream the same way.

**Decision.** Request/response routes are tested through `ASGITransport`. Frame ordering, the
heartbeat, the overflow report and the subscription's cleanup are tested by driving
`position_stream(...)` directly as an async generator. Exactly **one** test starts a real `uvicorn`
server on an OS-assigned port and reads the stream over TCP. `uvicorn` is therefore listed in both the
`dev` and `serve` extras.

**Why the stream cannot go through `ASGITransport`.** It buffers. Its response stream is
`ASGIResponseStream(body: list[bytes])`, assembled after the application has finished: it collects
every `http.response.body` message, waits for the app to complete, and only then hands the joined
bytes to `httpx`. That is invisible for an ordinary route and fatal for this one — an event stream
never completes, so `client.stream("GET", "/stream/positions")` would not return a response object at
all. The test would not fail; it would hang until the suite was killed, which is the worst behaviour a
test can have. This was found by running it, not by reasoning about it.

**Why one real socket anyway.** An endpoint that buffered its own output would pass every generator
assertion in the file: the frames would be correct, the order would be correct, and a client would see
nothing until the connection ended, which for this endpoint is never. Flushing is a property of the
transport, below anything the generator can observe, so it needs a real server. That test also
exercises the real lifespan, the real `StreamingResponse`, the real headers and a real client
disconnect, and it costs about a tenth of a second.

**Why `uvicorn` is in `dev` as well as `serve`.** If it were only in `serve`, that test would have to
be written as `pytest.importorskip("uvicorn")` — and a skipped test reports green while checking
nothing. Naming the dependency in `dev` means the suite has no conditional coverage in it at all.

**Cost accepted.** The stream is verified through two seams rather than one, so the route function's
own three lines (parse keys, build the response, set headers) are covered by the socket test and the
400 test rather than by the generator tests. Both are asserted, and the alternative was a suite that
hangs.

---

## ADR-057 — The diagnostics return `as_dict()` verbatim and are not described in the schema

**Status:** accepted (slice 5)

**Context.** `/projection`, `/cache` and `/subscribers` publish numbers owned by `ProjectionStats`,
`CachedPositions`, `SubscriptionHub` and `CheckpointGate`. FastAPI would like a response model.

**Decision.** Those three return `dict[str, Any]` built from the objects' own `as_dict()`, with no
pydantic model. `/healthz` *does* have a model.

**Why no model.** The field names belong to the objects that own the numbers — `ProjectionStats` says
so in its own docstring — and a pydantic mirror would be a second place to rename a metric. The
failure mode is a dashboard that breaks for a reason nobody can find, because the rename looked
complete. One definition means a renamed counter is visible immediately in the endpoint that publishes
it.

**Why `/healthz` is different.** Its response shape is a contract with an orchestrator rather than a
report for a human, it has exactly one field, and it will not grow. A model there costs nothing and
documents the probe's shape where a platform engineer will look for it.

**Cost accepted, and it is real.** Those three bodies appear in the OpenAPI document only as "an
object with some properties" — no named schema in `components`, so a generated client gets no type for
them. They are for an operator with `curl` and a dashboard query.
`test_the_diagnostics_are_not_described_in_the_openapi_schema` asserts the cost rather than hiding it,
so this stays a decision instead of decaying into an oversight.

---

## ADR-058 — A missing position is a 404; a counterparty with no positions is an empty list

**Status:** accepted (slice 5)

**Context.** Two read routes, two plausible answers each. Should an unknown position be a zeroed body
or a 404? Should an unknown counterparty be an empty list or a 404?

**Decision.** `GET /positions/{cp}/{ccy}` returns **404** when nothing is known. `GET /positions/{cp}`
returns **200** with `{"counterparty": ..., "positions": []}`. A malformed currency in the path is a
**400**, not a 404. Absences are likewise dropped from an SSE snapshot rather than rendered as zeros.

**Why not a zeroed position.** "No exposure" and "exposure of zero" are different facts and only one of
them is known. A row of zeros is a claim this service cannot support, a misspelt counterparty in a URL
would look like good news, and on a settlement screen that is the more dangerous of the two mistakes.
The store makes the same distinction for the same reason.

**Why the collection is not a 404.** A collection that exists and is empty is not a missing resource. A
client rendering a table wants zero rows, not an exception. The 404 belongs where the caller named one
specific thing.

**Why 400 for a bad currency.** 400 and 404 are both defensible and they send whoever is debugging to
different places: "your URL is wrong" points at their code, "no such position" points at the data.
Naming the caller's mistake is more useful than reporting an absence that is a consequence of it.

**Cost accepted.** A client that reads both routes has to handle a 404 on one and an empty list on the
other, which looks inconsistent until the reason is read. The inconsistency is in the questions, not in
the answers.

---

## ADR-059 — The freshness timeout has a server-side ceiling

**Status:** accepted (slice 5)

**Context.** `?timeout_ms=` lets a caller choose how long to wait. It arrives in the query string,
which means it arrives from anybody.

**Decision.** `timeout_ms` is `Query(ge=0, le=MAX_AWAIT_TIMEOUT_MS)` with `MAX_AWAIT_TIMEOUT_MS = 5000`
and a default of `DEFAULT_AWAIT_TIMEOUT_MS = 1000`. `await_event_id` is `Query(ge=1)`. Both violations
are FastAPI's own 422.

**Why a ceiling.** Without one, `?timeout_ms=600000` is a way for any caller to pin ten minutes of
server resources — a connection, a worker slot, a future in the gate — per request, from a URL that
looks entirely innocent. That is a denial of service with no payload and nothing to detect. The ceiling
is the control; the default is what keeps a caller who did not think about it from waiting forever.

**Why `ge=1` on the event id.** `NO_CHECKPOINT` is 0, so awaiting event 0 is either meaningless or an
uninitialised variable in the client. Refusing it turns a client bug into a 422 at the boundary rather
than a read that appears to work and guarantees nothing.

**Why 422 and not the API's own 400 envelope.** A query parameter is HTTP's business, and this is the
one place the framework's validation is exactly right — which is the other half of ADR-051. Reproducing
it by hand would mean two validators and one of them untested.

**Cost accepted.** A caller with a legitimate need to wait longer than five seconds cannot, and must
poll instead. Polling with a bounded wait is the behaviour that scales; a long single wait is the one
that does not.
