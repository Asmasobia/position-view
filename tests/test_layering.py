"""The architecture test: the dependency arrow runs one way, and the core touches nothing ambient.

WHY AN ARCHITECTURE RULE IS WORTH A TEST RATHER THAN A PARAGRAPH IN A README
---------------------------------------------------------------------------------
Every claim this repository makes about testability rests on one property: `model/` is pure. No
framework, no driver, no clock, no randomness. That property is what lets `project` be exercised
over all 24 bucket/event combinations in a third of a second with no fixtures, and it is what makes
"replaying the stream twice gives the same answer" a checkable statement rather than an aspiration.

It is also the single easiest property in the repository to lose, and it is never lost deliberately.
It goes one convenient import at a time — a `datetime.now()` for a default, a FastAPI
`HTTPException` because the error is going to become a 404 anyway, a settings object read from the
environment. Each one is a two-line change that passes review. The fifth one has made the projection
untestable without the rest of the service standing up, and by then unpicking it is a refactor
nobody has time for.

A test is the only form of this rule that survives contact with a deadline.

HOW IT WORKS, AND WHY IT PARSES RATHER THAN IMPORTS
The imports are read with `ast`, not by importing the modules and inspecting `sys.modules`.
Importing would give a *runtime* answer that depends on what the test process happened to load
already, and it would miss the interesting case entirely: a module that imports something forbidden
inside a function body would look clean because the import never ran. Parsing sees the source, which
is the thing under review.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

PACKAGE_ROOT = Path(__file__).resolve().parent.parent / "src" / "position_view"

#: The layers, innermost first. The index is the rank, and the rule is: a module may import from its
#: own layer or from any layer with a LOWER rank, never higher.
#:
#: Listed here rather than inferred from the directory tree on purpose. A new package appearing
#: under `position_view/` fails `test_every_package_is_a_registered_layer` until it is added to this
#: list, which forces a two-second decision — "where does this sit in the arrow?" — at the moment
#: the package is created rather than after something already depends on it in both directions.
LAYERS = ["model", "projection", "push", "cache", "api"]

#: Third-party packages each layer is allowed to import. Absent from this mapping means "none".
#:
#: `model` is deliberately not a key. Not "an empty list" — absent, because the distinction the test
#: reports is "this layer has no allowance" rather than "this layer's allowance happens to be
#: empty", and the first is a rule while the second reads like an oversight.
ALLOWED_THIRD_PARTY = {
    "api": {"fastapi", "starlette", "sse_starlette", "pydantic"},
}

#: Calls that reach outside the arguments a function was given. Forbidden in `model/`, which is what
#: makes the projection deterministic.
#:
#: Spelled as the attribute access a reader would actually write, and matched on the last two
#: components, so `datetime.now(...)`, `dt.datetime.now(...)` and `datetime.datetime.now(...)` are
#: all caught. Matching only the final name would flag any method called `now`; matching the full
#: dotted path would miss every aliased import.
AMBIENT_CALLS = {
    ("datetime", "now"),
    ("datetime", "today"),
    ("datetime", "utcnow"),
    ("date", "today"),
    ("time", "time"),
    ("time", "monotonic"),
    ("time", "perf_counter"),
    ("time", "sleep"),
    ("uuid", "uuid1"),
    ("uuid", "uuid4"),
    ("random", "random"),
    ("random", "choice"),
    ("random", "randint"),
    ("os", "getenv"),
    ("os", "environ"),
}

#: Modules whose mere presence in `model/` means the purity rule has already been broken, even if
#: nothing is called yet. An unused import is a statement of intent.
FORBIDDEN_IN_MODEL = {"random", "secrets", "socket", "sqlite3", "asyncio", "threading", "logging"}


def python_files() -> list[Path]:
    """Every module in the package, so a new file is covered without being registered anywhere.

    Sorted, because an unsorted `rglob` makes the parametrised test ids depend on filesystem order
    and a failure list that reorders between runs is much harder to read than one that does not.
    """
    return sorted(PACKAGE_ROOT.rglob("*.py"))


def layer_of(path: Path) -> str | None:
    """Which layer a file belongs to, or None for the top-level package `__init__`."""
    relative = path.relative_to(PACKAGE_ROOT)
    return relative.parts[0] if len(relative.parts) > 1 else None


def imported_modules(path: Path) -> set[str]:
    """Every module name this file imports, with relative imports resolved to their layer.

    Returns dotted names as written (`decimal`, `position_view.model.money`, `fastapi.responses`),
    and resolves `from .money import ZERO` inside `model/` to `position_view.model.money` — because
    the rule is about layers, and a relative import hides which layer it lands in.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    package_parts = ("position_view", *path.relative_to(PACKAGE_ROOT).parts[:-1])
    names: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                # An absolute import. `module` cannot be None here — `from import x` is a syntax
                # error — but the type says it can, and asserting is cheaper than a cast.
                assert node.module is not None
                names.add(node.module)
            else:
                # A relative import: `level` counts the dots, so strip that many components off this
                # file's own package to find the base. `from . import x` has `module is None`.
                base = package_parts[: len(package_parts) - (node.level - 1)]
                names.add(".".join([*base, node.module] if node.module else base))

    return names


def called_attributes(path: Path) -> set[tuple[str, str]]:
    """Every `something.name(...)` call in the file, as (owner, name) pairs.

    Only *calls* are collected, not bare attribute access, so a type annotation mentioning
    `datetime.datetime` is not mistaken for reading the clock.
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: set[tuple[str, str]] = set()

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            found.add((func.value.id, func.attr))
        elif isinstance(func, ast.Attribute) and isinstance(func.value, ast.Attribute):
            # `datetime.datetime.now()` — take the last two components.
            found.add((func.value.attr, func.attr))
    return found


def test_the_package_root_is_where_this_test_thinks_it_is() -> None:
    """Guards every other test in the file against silently testing nothing.

    A path typo would make `python_files()` return an empty list, every parametrised test below
    would collect zero cases, and the suite would report a cheerful pass for a rule that was never
    checked. This is the one test that cannot be skipped into vacuity.
    """
    assert PACKAGE_ROOT.is_dir(), f"{PACKAGE_ROOT} is not a directory"
    files = python_files()
    assert len(files) >= 6, f"expected the model package at least; found {files}"


def test_every_package_is_a_registered_layer() -> None:
    """A new sub-package must be placed in the arrow before anything can import it.

    Deliberately strict in one direction only: a layer in `LAYERS` that does not exist yet is fine,
    because the list is the plan and the slices land one at a time.
    """
    packages = {
        path.parent.name
        for path in python_files()
        if path.name == "__init__.py" and path.parent != PACKAGE_ROOT
    }
    unregistered = packages - set(LAYERS)
    assert not unregistered, f"packages missing from LAYERS: {sorted(unregistered)}"


@pytest.mark.parametrize("path", python_files(), ids=lambda p: str(p.name))
def test_no_module_imports_a_higher_layer(path: Path) -> None:
    """⚠️ THE ARROW. `api` may import everything; `model` may import nothing.

    The direction is the whole architecture. If `model` could import `projection`, the pure core
    would need a storage backend to be imported at all, and every test in `test_project.py` would
    need a database — which is precisely the situation this layout exists to avoid.

    The check is per-file rather than per-package so that the failure message names the file, and
    the parametrised id names it too: a broken arrow shows up as
    `test_no_module_imports_a_higher_layer[ positions.py]` rather than as one failure listing forty
    imports.
    """
    layer = layer_of(path)
    if layer is None:
        # The top-level `__init__.py` is documentation. It imports nothing, which is asserted
        # separately, and it has no rank in the arrow.
        return

    rank = LAYERS.index(layer)
    for imported in imported_modules(path):
        if not imported.startswith("position_view."):
            continue
        parts = imported.split(".")
        if len(parts) < 2 or parts[1] not in LAYERS:
            continue
        other = parts[1]
        assert LAYERS.index(other) <= rank, (
            f"{path.name} is in '{layer}' and imports '{other}', which is further out. "
            f"The arrow runs {' -> '.join(LAYERS)}."
        )


@pytest.mark.parametrize("path", python_files(), ids=lambda p: str(p.name))
def test_third_party_imports_are_declared_for_the_layer(path: Path) -> None:
    """A dependency has to be admitted in this file before a module may use it.

    This is the rule that keeps `fastapi` from drifting inwards. FastAPI in `api/` is the point of
    `api/`; FastAPI in `cache/` means the cache raises `HTTPException`, and now the cache cannot be
    used by the SSE stream or by a background refresh without one of them pretending to be a
    request. The framework belongs at the edge, and "at the edge" is only enforceable if there is a
    list.

    The list is an allowance rather than a lock file — it says which *layer* may see a package, not
    which version. Versions are `pyproject.toml`'s job; this is about direction.
    """
    layer = layer_of(path)
    if layer is None:
        return

    allowed = ALLOWED_THIRD_PARTY.get(layer, set())
    for imported in sorted(imported_modules(path)):
        top = imported.split(".")[0]
        if top == "position_view" or top in sys.stdlib_module_names:
            continue
        assert top in allowed, (
            f"{path.name} is in '{layer}' and imports third-party '{imported}'. "
            f"Layer '{layer}' is allowed {sorted(allowed) or 'nothing'}. "
            f"Add it to ALLOWED_THIRD_PARTY only if the dependency genuinely belongs this far in."
        )


@pytest.mark.parametrize(
    "path", [p for p in python_files() if layer_of(p) == "model"], ids=lambda p: str(p.name)
)
class TestModelPurity:
    """The core's own rules, which are stricter than the arrow.

    A class rather than four functions so the parametrisation is declared once, and because these
    four assertions are one idea: `model/` is a library of values and functions over them, and
    nothing else.
    """

    def test_imports_only_the_standard_library(self, path: Path) -> None:
        """No third party at all — not FastAPI, not a driver, not a settings library.

        `sys.stdlib_module_names` is the authority rather than a hand-written allow-list, so this
        test does not need updating when the core starts using a new standard module. It is a frozen
        set of top-level names shipped with the interpreter, which is exactly the question being
        asked.
        """
        for imported in sorted(imported_modules(path)):
            top = imported.split(".")[0]
            if top == "position_view":
                continue
            assert top in sys.stdlib_module_names, (
                f"{path.name} imports third-party module '{imported}'. The model layer is pure: "
                f"a dependency here is a dependency of every test that touches the projection."
            )

    def test_imports_nothing_from_the_forbidden_list(self, path: Path) -> None:
        """Some standard-library modules are as disqualifying as a third-party one.

        `asyncio` and `threading` are concurrency, `logging` is I/O with global configuration,
        `sqlite3` and `socket` are storage and network, `random` and `secrets` are non-determinism.
        Each of them is in the standard library and each of them would end the property that makes
        this layer worth having, so `sys.stdlib_module_names` alone is not a sufficient test.

        `logging` deserves a note, because excluding it looks excessive. A pure function that logs
        is no longer a function of its arguments — its behaviour depends on global configuration,
        its output is a side effect a test has to capture, and the temptation immediately afterwards
        is to log *instead of* returning a reason. `SkipReason` exists because that temptation was
        refused: the caller receives the fact and decides how loud it is.
        """
        for imported in sorted(imported_modules(path)):
            top = imported.split(".")[0]
            assert top not in FORBIDDEN_IN_MODEL, (
                f"{path.name} imports '{imported}', which is not allowed in the model layer."
            )

    def test_reads_no_clock_and_no_randomness(self, path: Path) -> None:
        """⚠️ THE RULE THAT KEEPS `updated_at` HONEST AND REPLAY REPRODUCIBLE.

        `datetime.now()` in the projection would be the most natural line of code in the repository,
        and it would be wrong twice over. It would make `project` non-deterministic, so "folding the
        stream twice gives the same answer" would stop being true and the idempotence test would
        have to be deleted. And it would make `updated_at` *wall-clock* time, so a projection an
        hour behind would report every position as freshly updated — the exact opposite of what an
        operator needs during a backlog.

        Event time comes in as `Envelope.occurred_at` and is passed to `with_deltas(at=...)` by the
        caller. The core never asks what time it is.
        """
        for owner, name in sorted(called_attributes(path)):
            assert (owner, name) not in AMBIENT_CALLS, (
                f"{path.name} calls {owner}.{name}(), which reads state it was not given. "
                f"The model layer is a function of its arguments."
            )

    def test_opens_no_files(self, path: Path) -> None:
        """`open()` in a pure layer is either config or a cache, and both belong elsewhere."""
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id != "open", f"{path.name} calls open()"


def test_the_top_level_init_is_documentation_only() -> None:
    """`position_view/__init__.py` imports nothing, so importing the package costs nothing.

    A convenience re-export here would make `import position_view` pull in `api`, which pulls in
    FastAPI, which means a test of the pure model pays for the web framework — and a CLI or a worker
    that only needs the projector imports an HTTP server it will never listen on.
    """
    init = PACKAGE_ROOT / "__init__.py"
    assert imported_modules(init) == set()


def test_the_model_package_exports_a_deliberate_surface() -> None:
    """`model/__init__.py` declares `__all__`, so "public" is a decision rather than an accident.

    Without it, every name that happens to be importable is part of the surface, including the
    private helpers — and moving one becomes a breaking change to code nobody knew existed.
    """
    tree = ast.parse((PACKAGE_ROOT / "model" / "__init__.py").read_text(encoding="utf-8"))
    exported = [
        node
        for node in tree.body
        if isinstance(node, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets)
    ]
    assert len(exported) == 1, "model/__init__.py must declare exactly one __all__"


def test_every_public_name_in_all_is_importable() -> None:
    """An `__all__` naming something that does not exist is a lie that only fails on a star import.

    Written as a real import rather than an AST check, because the failure being guarded against is
    a rename that updated the definition and not the list — and only actually resolving the names
    can see that.
    """
    import position_view.model as model

    for name in model.__all__:
        assert hasattr(model, name), f"model.__all__ names '{name}', which is not defined"


def test_all_is_grouped_and_sorted() -> None:
    """Cosmetic, and it earns its place: `__all__` is the file's table of contents.

    An unsorted list of thirty names is one somebody has to read linearly to answer "is X
    exported?". Ruff's `RUF022` enforces the same ordering — the test exists because a lint rule is
    easy to silence with a `noqa` under time pressure, and a failing test is a conversation rather
    than a comment.

    The order is three groups, alphabetical inside each: CONSTANTS, then classes, then functions.
    That grouping is more useful than one flat alphabetical run because the three kinds of name
    answer different questions — "what values are fixed?", "what types can I hold?", "what can I
    call?" — and a reader almost always arrives with one of those three questions rather than with a
    spelling.

    Written as a key function rather than as a hardcoded expected list, so adding an export does not
    mean editing this test. A test that has to be updated for every ordinary change is a test people
    learn to update without reading.
    """
    import position_view.model as model

    def rank(name: str) -> tuple[int, str]:
        if name.isupper():
            return (0, name)
        # `isupper()` is False for "Applied" but True for "MAX_AMOUNT", which is exactly the split
        # wanted: CONSTANT_CASE first, then anything else beginning with a capital.
        return (1, name) if name[0].isupper() else (2, name)

    assert list(model.__all__) == sorted(model.__all__, key=rank)
