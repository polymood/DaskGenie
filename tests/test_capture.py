from __future__ import annotations

import dask.array as da

from daskgenie.graphcapture.capture import _is_library_frame, clear_layer_map, track, watch


def setup_function() -> None:
    clear_layer_map()


def test_is_library_frame_filters_known_libraries() -> None:
    assert _is_library_frame("/usr/lib/python3.12/site-packages/dask/array/core.py")
    assert _is_library_frame("/home/user/venv/lib/python3.12/dask/highlevelgraph.py")
    assert _is_library_frame("/home/user/project/.venv/lib/xarray/core/dataset.py")


def test_is_library_frame_keeps_user_code() -> None:
    assert not _is_library_frame("/home/user/project/pipeline.py")
    # substring "dask" in a filename must not false-positive against the
    # "dask" library-module component check (component match, not substring)
    assert not _is_library_frame("/home/user/project/my_dask_utils.py")


def test_track_captures_layer_for_real_dask_op(tmp_path) -> None:
    script = tmp_path / "pipeline.py"
    script.write_text(
        "import dask.array as da\n"
        "import daskgenie as dg\n"
        "with dg.track() as layer_map:\n"
        "    x = da.ones((10, 10), chunks=(5, 5))\n"
        "    y = x.rechunk((10, 10))\n"
        "import json\n"
        "print(json.dumps({k: [v.filename, v.lineno] for k, v in layer_map.items()}))\n"
    )
    import json
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, check=True
    )
    captured = json.loads(result.stdout)
    linenos = {lineno for _filename, lineno in captured.values()}
    assert 4 in linenos  # the `da.ones(...)` line
    assert 5 in linenos  # the `x.rechunk(...)` line
    for filename, _lineno in captured.values():
        assert filename == str(script)


def test_track_uninstalls_hook_on_exit() -> None:
    from dask.highlevelgraph import HighLevelGraph

    original = HighLevelGraph.from_collections.__func__
    with track():
        x = da.ones((10, 10), chunks=(5, 5))
        del x
    assert HighLevelGraph.from_collections.__func__ is original


def test_watch_decorator_populates_global_map() -> None:
    @watch
    def build() -> da.Array:
        return da.ones((10, 10), chunks=(5, 5))

    build()
    from daskgenie.graphcapture.capture import get_layer_map

    assert len(get_layer_map()) >= 1


def test_track_is_reentrant() -> None:
    with track():
        with track():
            x = da.ones((5, 5), chunks=(5, 5))
            del x
