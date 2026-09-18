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
