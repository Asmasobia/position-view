"""position-view: a CQRS read model over a settlement event stream.

The package is split by *what may depend on what*, not by what things are called:

    model/        the event contract, the state, and the pure projection step. No I/O, no framework,
                  no clock — so every semantic question (duplicate? out of order? illegal
                  transition? whose money moves?) is answerable by a function call in a test.
    projection/   the storage-shaped half: read a snapshot, apply deltas, advance the checkpoint,
                  atomically. Makes no decisions and does no arithmetic.
    push/         the fan-out hub: bounded per-subscriber queues, conflation, and the snapshot-then-
                  stream handover that must not drop or duplicate an update.
    cache/        a read-through cache with single-flight loading and event-driven invalidation.
    api/          the HTTP surface: FastAPI routing, SSE, and the operational endpoints.

The dependency arrow runs one way only — `api` may import everything, `model` may import nothing —
and `tests/test_layering.py` fails the build if it ever runs the other way.
"""
