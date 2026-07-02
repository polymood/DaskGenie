"""Attribute Dask task-graph layers back to the user source line that built them.

The hook point is ``HighLevelGraph.from_collections`` — nearly every Dask
collection operation (array, dataframe, and by extension xarray, since it
wraps dask arrays) calls this classmethod once per new named layer. Patching
it for the duration of ``track()`` gives us a single, stable interception
point instead of having to special-case every array/dataframe/xarray op.
"""

from __future__ import annotations

import functools
import linecache
import sys
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any, TypeVar

from dask.highlevelgraph import HighLevelGraph

_F = TypeVar("_F", bound=Callable[..., Any])

# Path components that mark a frame as library code rather than user code.
# Matched against path *parts* (not substrings) so a user file like
# "my_dask_utils.py" isn't mistaken for the "dask" package itself.
_DEFAULT_LIBRARY_MODULES: tuple[str, ...] = (
    "dask",
    "distributed",
    "xarray",
    "numpy",
    "zarr",
    "daskgenie",
)


@dataclass(frozen=True, slots=True)
class SourceLocation:
    """The user-code call site that produced a graph layer."""

    filename: str
    lineno: int
    code_snippet: str


# Single global accumulator. track() and watch() both write into this so
# results compose whether you use one big `with track():` block or several
# @watch-decorated functions.
_layer_map: dict[str, SourceLocation] = {}
_extra_library_paths: list[str] = []
_active_depth = 0

# The un-patched classmethod descriptor, saved so we can restore it exactly.
_ORIGINAL_DESCRIPTOR = HighLevelGraph.__dict__["from_collections"]
_original_from_collections: Callable[..., HighLevelGraph] = _ORIGINAL_DESCRIPTOR.__func__


def _is_library_frame(filename: str) -> bool:
    parts = Path(filename).parts
    if "site-packages" in parts or "dist-packages" in parts:
        return True
    if any(module in parts for module in _DEFAULT_LIBRARY_MODULES):
        return True
    return any(extra in filename for extra in _extra_library_paths)


def _find_user_frame() -> FrameType | None:
    # Start two frames up: skip this function and the patched classmethod
    # that calls it.
    frame: FrameType | None = sys._getframe(2)
    while frame is not None:
        if not _is_library_frame(frame.f_code.co_filename):
            return frame
        frame = frame.f_back
    return None


def _capture_source_location() -> SourceLocation | None:
    frame = _find_user_frame()
    if frame is None:
        return None
    filename = frame.f_code.co_filename
    lineno = frame.f_lineno
    snippet = linecache.getline(filename, lineno).strip()
    return SourceLocation(filename=filename, lineno=lineno, code_snippet=snippet)


def _patched_from_collections(
    cls: type[HighLevelGraph],
    name: str,
    layer: Any,
    dependencies: Sequence[Any] = (),
) -> HighLevelGraph:
    hlg = _original_from_collections(cls, name, layer, dependencies)
    if name not in _layer_map:
        location = _capture_source_location()
        if location is not None:
            _layer_map[name] = location
    return hlg


@contextmanager
def track(
    *, extra_library_paths: Sequence[str] | None = None
) -> Iterator[dict[str, SourceLocation]]:
    """Capture ``{layer_name: SourceLocation}`` for graph layers built in this block.

    Reentrant: nested ``track()`` blocks (including those started implicitly
    by ``@watch``) share the same underlying map and hook installation.
    """
    global _active_depth
    pushed = list(extra_library_paths) if extra_library_paths else []
    _extra_library_paths.extend(pushed)
    _active_depth += 1
    if _active_depth == 1:
        HighLevelGraph.from_collections = classmethod(_patched_from_collections)  # type: ignore[assignment]
    try:
        yield _layer_map
    finally:
        _active_depth -= 1
        if _active_depth == 0:
            HighLevelGraph.from_collections = _ORIGINAL_DESCRIPTOR  # type: ignore[method-assign]
        for _ in pushed:
            _extra_library_paths.pop()


def watch(func: _F) -> _F:
    """Decorator: run ``func`` with capture hooks installed."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        with track():
            return func(*args, **kwargs)

    return wrapper  # type: ignore[return-value]


def get_layer_map() -> dict[str, SourceLocation]:
    """A snapshot of everything captured so far (across all track()/@watch use)."""
    return dict(_layer_map)


def clear_layer_map() -> None:
    _layer_map.clear()
