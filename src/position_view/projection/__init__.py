"""The storage-shaped half of the projection: one unit of work per event, and the counters.

Nothing in here decides what an event *means* — that is `model/project.py`, which is a synchronous
function of two values and is tested exhaustively without a store. This package owns the three
things that function cannot: atomicity, serialisation, and the numbers an operator reads.

Depends on `model/` and on nothing else. Not on the fan-out hub (a projector that knew about
subscribers could not be used for an offline rebuild), not on the cache, and not on FastAPI. The
listener seam is a callable, which is how `push/` attaches in slice 3 without this package importing
it.

`__all__` is written out for the same reason as in `model/`: so that "public" is a decision. In this
package the decision has teeth, because `_empty_position` and `_build_changed_positions` are exactly
the kind of helper an api layer would otherwise reach for and thereby couple itself to the fold.
"""

from .projector import ChangeListener, ProjectionStats, Projector, Refused
from .store import NO_CHECKPOINT, InMemoryPositionStore, PositionStore, StoreError, WriteBatch

__all__ = [
    "NO_CHECKPOINT",
    "ChangeListener",
    "InMemoryPositionStore",
    "PositionStore",
    "ProjectionStats",
    "Projector",
    "Refused",
    "StoreError",
    "WriteBatch",
]
