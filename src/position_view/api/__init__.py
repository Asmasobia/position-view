"""The HTTP surface. The only layer in this package that knows a web framework exists.

That sentence is the architecture, and `test_layering.py` enforces it: `fastapi` appears in exactly
one layer's allowance. The reason is not taste. A cache that raises `HTTPException` cannot be used
by the SSE stream or by a background refresh without one of them pretending to be a request, and a
projection that imports a framework needs a framework installed to be *tested*. Everything inward of
here is testable with nothing but the standard library, and it stays that way because there is a
list and a test that reads it.

What lives here, and why each piece is separate:

  `service.py`   the composition root — the one place the store, projector, cache and hub are wired
                 together — plus `CheckpointGate`, which is what makes read-your-writes possible.
  `payloads.py`  the response shapes, and the rule that money is a JSON string and never a number.
  `stream.py`    Server-Sent Events: the framing, the heartbeat, and the snapshot-to-stream
                 handover, done in the only order that neither loses nor duplicates a change.
  `app.py`       `create_app`, the routes, and the decisions that only exist at the edge — which
                 status code, which header, and what a timeout is allowed to return.

The split is by *kind of decision*, not by resource. A file per endpoint would scatter the SSE
handover, the freshness gate and the error shape across three places and leave none of them
explaining itself.
"""

from __future__ import annotations

from .app import create_app
from .payloads import (
    HealthPayload,
    IngestedPayload,
    PositionListPayload,
    PositionPayload,
    TotalsPayload,
    parse_position_key,
)
from .service import (
    DEFAULT_AWAIT_TIMEOUT_MS,
    MAX_AWAIT_TIMEOUT_MS,
    CheckpointGate,
    Service,
)
from .stream import HEARTBEAT_SECONDS, SSE_MEDIA_TYPE, position_stream

__all__ = [
    "DEFAULT_AWAIT_TIMEOUT_MS",
    "HEARTBEAT_SECONDS",
    "MAX_AWAIT_TIMEOUT_MS",
    "SSE_MEDIA_TYPE",
    "CheckpointGate",
    "HealthPayload",
    "IngestedPayload",
    "PositionListPayload",
    "PositionPayload",
    "Service",
    "TotalsPayload",
    "create_app",
    "parse_position_key",
    "position_stream",
]
