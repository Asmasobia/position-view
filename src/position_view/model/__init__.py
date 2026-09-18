"""The read model's vocabulary: the event contract, the state, and the pure projection step.

Nothing in this package imports a framework, a driver or a clock. That is checked by a test rather
than promised in a comment (`tests/test_layering.py`), and the reason it is worth checking is that
the first convenient exception — a `datetime.now()` here, a FastAPI import there — is the one that
makes the projection untestable without the rest of the service standing up.

`__all__` is written out rather than left implicit so that this file is the answer to "what is the
public surface of the model?". Anything not named here is an implementation detail, and moving it is
not a breaking change.
"""

from .events import (
    AGGREGATE_INSTRUCTION,
    EVENT_FAILED,
    EVENT_MATCHED,
    EVENT_RECEIVED,
    EVENT_REVERSED,
    EVENT_SETTLED,
    EVENT_VALIDATED,
    SUPPORTED_SCHEMA_VERSION,
    Envelope,
    MalformedEventError,
    decode_envelope,
)
from .money import MAX_AMOUNT, MONEY_EXPONENT, ZERO, MoneyError, parse_amount, quantize
from .positions import (
    TERMINAL_BUCKETS,
    Bucket,
    BucketDelta,
    InstructionSnapshot,
    Position,
    PositionKey,
    Totals,
    bucket_for,
    is_legal_transition,
)
from .project import Applied, Outcome, PositionChange, Skipped, SkipReason, project

__all__ = [
    "AGGREGATE_INSTRUCTION",
    "EVENT_FAILED",
    "EVENT_MATCHED",
    "EVENT_RECEIVED",
    "EVENT_REVERSED",
    "EVENT_SETTLED",
    "EVENT_VALIDATED",
    "MAX_AMOUNT",
    "MONEY_EXPONENT",
    "SUPPORTED_SCHEMA_VERSION",
    "TERMINAL_BUCKETS",
    "ZERO",
    "Applied",
    "Bucket",
    "BucketDelta",
    "Envelope",
    "InstructionSnapshot",
    "MalformedEventError",
    "MoneyError",
    "Outcome",
    "Position",
    "PositionChange",
    "PositionKey",
    "SkipReason",
    "Skipped",
    "Totals",
    "bucket_for",
    "decode_envelope",
    "is_legal_transition",
    "parse_amount",
    "project",
    "quantize",
]
