"""Server-rendered dashboard: multi-section HTML pages served straight from the
collector via Jinja2. No SPA, no JS build step — the collector already holds the
data, so it renders the pages (and the memory chart, as inline SVG) itself.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from daskgenie.collector.store import Store
from daskgenie.collector.svg import memory_chart

_HERE = Path(__file__).parent


def _fmt_bytes(n: float | int) -> str:
    v = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if v < 1024 or unit == "TB":
            return f"{v:.0f} {unit}" if unit == "B" else f"{v:.1f} {unit}"
        v /= 1024
    return f"{v:.1f} TB"


def _ago(ts: float) -> str:
    secs = max(0.0, time.time() - ts)
    if secs < 60:
        return f"{int(secs)}s ago"
    if secs < 3600:
        return f"{int(secs // 60)}m ago"
    if secs < 86400:
        return f"{int(secs // 3600)}h ago"
    return f"{int(secs // 86400)}d ago"


def _short_key(key: str) -> str:
    # "('sum-abc', 1, 0)" -> "sum (1, 0)"
    body = key.strip("()")
    first, _, rest = body.partition(",")
    name = first.strip().strip("'\"").split("-")[0]
    idx = rest.strip()
    return f"{name} ({idx})" if idx else name


def _layer_token(key: str) -> str:
    return key.replace("(", "").replace(")", "").split(",")[0].strip().strip("'\"")


def _source_for(key: str, layers: list[dict[str, Any]]) -> dict[str, Any] | None:
    token = _layer_token(key)
    for layer in layers:
        name = str(layer["layer"])
        if token.startswith(name) or name.startswith(token):
            return {
                "filename": layer["filename"],
                "lineno": layer["lineno"],
                "snippet": layer["code_snippet"],
            }
    return None


def _post_mortem(store: Store, run_id: str) -> list[dict[str, Any]]:
    layers = store.graph(run_id)["layers"]
    out: list[dict[str, Any]] = []
    for d in store.deaths(run_id):
        suspects = []
        for key in d["suspect_keys"]:
            chunks = [c for c in d["suspect_chunks"] if c["task_key"] == key]
            suspects.append(
                {"key": _short_key(key), "source": _source_for(key, layers), "chunks": chunks}
            )
        out.append(
            {
                "worker": d["worker"],
                "reason": d["reason"],
                "suspected_oom": d["suspected_oom"],
                "suspects": suspects,
            }
        )
    return out


def register_html(app: FastAPI, store: Store) -> None:
    templates = Jinja2Templates(directory=str(_HERE / "templates"))
    templates.env.filters["bytes"] = _fmt_bytes
    templates.env.filters["ago"] = _ago
    app.mount("/static", StaticFiles(directory=str(_HERE / "static")), name="static")

    def _run_or_404(run_id: str) -> Any:
        run = store.get_run(run_id)
        if run is None:
            raise _not_found()
        return run

    @app.get("/", response_class=HTMLResponse)
    def index(request: Request) -> Any:
        return templates.TemplateResponse(request, "runs.html", {"runs": store.list_runs()})

    @app.get("/runs/{run_id}", response_class=HTMLResponse)
    def overview(request: Request, run_id: str) -> Any:
        run = _run_or_404(run_id)
        samples = store.timeline(run_id)
        peak = max((int(s["rss_bytes"]) for s in samples), default=0)
        oom = [d for d in store.deaths(run_id) if d["suspected_oom"] and d["suspect_keys"]]
        return templates.TemplateResponse(
            request,
            "run.html",
            {
                "section": "overview",
                "run": run,
                "death_count": run.counts.get("deaths", 0),
                "peak_bytes": peak,
                "chart": memory_chart(samples),
                "oom_count": len(oom),
            },
        )

    @app.get("/runs/{run_id}/postmortem", response_class=HTMLResponse)
    def postmortem(request: Request, run_id: str) -> Any:
        run = _run_or_404(run_id)
        return templates.TemplateResponse(
            request,
            "run.html",
            {
                "section": "postmortem",
                "run": run,
                "death_count": run.counts.get("deaths", 0),
                "deaths": _post_mortem(store, run_id),
            },
        )

    @app.get("/runs/{run_id}/memory", response_class=HTMLResponse)
    def memory(request: Request, run_id: str) -> Any:
        run = _run_or_404(run_id)
        return templates.TemplateResponse(
            request,
            "run.html",
            {
                "section": "memory",
                "run": run,
                "death_count": run.counts.get("deaths", 0),
                "chart": memory_chart(store.timeline(run_id)),
            },
        )

    @app.get("/runs/{run_id}/graph", response_class=HTMLResponse)
    def graph(request: Request, run_id: str) -> Any:
        run = _run_or_404(run_id)
        layers = sorted(store.graph(run_id)["layers"], key=lambda layer: layer["lineno"])
        return templates.TemplateResponse(
            request,
            "run.html",
            {
                "section": "graph",
                "run": run,
                "death_count": run.counts.get("deaths", 0),
                "layers": layers,
            },
        )

    @app.post("/runs/{run_id}/delete")
    def delete(run_id: str) -> RedirectResponse:
        store.delete_run(run_id)
        return RedirectResponse("/", status_code=303)


def _not_found() -> Exception:
    from fastapi import HTTPException

    return HTTPException(status_code=404, detail="unknown run")
