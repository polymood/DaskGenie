"""Render a per-worker memory timeline as a self-contained inline SVG.

No JavaScript charting library — the collector already has the data, so it
draws the chart server-side. Keeps the page a single lightweight HTML document.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

_PALETTE = ("#2f81f7", "#3fb950", "#d29922", "#f85149", "#a371f7", "#39c5cf")

_W = 900
_H = 320
_PAD_L = 64
_PAD_R = 16
_PAD_T = 16
_PAD_B = 32


@dataclass(frozen=True)
class Chart:
    svg: str
    legend: tuple[tuple[str, str], ...]  # (worker, color)


def _fmt_bytes(n: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    v = float(n)
    i = 0
    while v >= 1024 and i < len(units) - 1:
        v /= 1024
        i += 1
    return f"{v:.0f} {units[i]}"


def memory_chart(samples: list[dict[str, Any]]) -> Chart | None:
    """Build an SVG line chart of RSS over time, one line per worker."""
    if not samples:
        return None
    times = [float(s["timestamp"]) for s in samples]
    t0, t1 = min(times), max(times)
    span = t1 - t0 or 1.0
    peak = max(int(s["rss_bytes"]) for s in samples) or 1

    workers = sorted({str(s["worker"]) for s in samples})
    colors = {w: _PALETTE[i % len(_PALETTE)] for i, w in enumerate(workers)}

    def x(t: float) -> float:
        return _PAD_L + (t - t0) / span * (_W - _PAD_L - _PAD_R)

    def y(v: float) -> float:
        return _PAD_T + (1 - v / peak) * (_H - _PAD_T - _PAD_B)

    parts: list[str] = [
        f'<svg viewBox="0 0 {_W} {_H}" width="100%" '
        f'preserveAspectRatio="xMidYMid meet" role="img" class="chart">'
    ]

    # horizontal gridlines + y labels at 0, 25, 50, 75, 100% of peak
    for frac in (0.0, 0.25, 0.5, 0.75, 1.0):
        yy = y(peak * frac)
        parts.append(
            f'<line x1="{_PAD_L}" y1="{yy:.1f}" x2="{_W - _PAD_R}" y2="{yy:.1f}" class="grid"/>'
        )
        parts.append(
            f'<text x="{_PAD_L - 8}" y="{yy + 4:.1f}" class="ylab">{_fmt_bytes(peak * frac)}</text>'
        )

    # x axis end labels (seconds since run start)
    parts.append(f'<text x="{_PAD_L}" y="{_H - 10}" class="xlab">0s</text>')
    parts.append(
        f'<text x="{_W - _PAD_R}" y="{_H - 10}" class="xlab" text-anchor="end">{span:.0f}s</text>'
    )

    # one polyline per worker
    for w in workers:
        pts = [
            (x(float(s["timestamp"])), y(float(s["rss_bytes"])))
            for s in sorted(
                (s for s in samples if str(s["worker"]) == w),
                key=lambda s: float(s["timestamp"]),
            )
        ]
        d = " ".join(f"{px:.1f},{py:.1f}" for px, py in pts)
        parts.append(f'<polyline points="{d}" fill="none" stroke="{colors[w]}" stroke-width="2"/>')

    parts.append("</svg>")
    return Chart(svg="".join(parts), legend=tuple((w, colors[w]) for w in workers))
