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
