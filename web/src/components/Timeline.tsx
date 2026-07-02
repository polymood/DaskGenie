import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { useTimeline } from "../api";
import type { Sample } from "../types";

const PALETTE = [
  "#33d6c4",
  "#9b8cff",
  "#f2b45c",
  "#ff5c6c",
  "#5cc8ff",
  "#7ee081",
];

// Bucket samples to ~0.25s so points from different workers align into shared
// rows Recharts can plot as one line per worker.
function toRows(samples: Sample[]): {
  rows: Record<string, number>[];
  workers: string[];
} {
  if (samples.length === 0) return { rows: [], workers: [] };
  const t0 = Math.min(...samples.map((s) => s.timestamp));
  const workers = [...new Set(samples.map((s) => s.worker))];
  const byBucket = new Map<number, Record<string, number>>();
  for (const s of samples) {
    const t = Math.round((s.timestamp - t0) * 4) / 4;
    const row = byBucket.get(t) ?? { t };
    row[s.worker] = s.rss_bytes / 1e6;
    byBucket.set(t, row);
  }
  const rows = [...byBucket.values()].sort((a, b) => a.t - b.t);
  return { rows, workers };
}

interface Props {
  runId: string;
}

export function Timeline({ runId }: Props) {
  const { data: samples, isLoading } = useTimeline(runId);
  if (isLoading) return <div className="empty">Loading…</div>;
  if (!samples || samples.length === 0)
    return <div className="empty">No memory samples for this run yet.</div>;

  const { rows, workers } = toRows(samples);
  const peak = Math.max(...samples.map((s) => s.rss_bytes)) / 1e6;

  return (
    <div className="card">
      <div className="legend">
        {workers.map((w, i) => (
          <span key={w}>
            <span
              className="swatch"
              style={{ background: PALETTE[i % PALETTE.length] }}
            />
            {w.replace("tcp://", "")}
          </span>
        ))}
      </div>
      <ResponsiveContainer width="100%" height={420}>
        <LineChart
          data={rows}
          margin={{ top: 8, right: 16, bottom: 8, left: 8 }}
        >
          <CartesianGrid stroke="#262b39" vertical={false} />
          <XAxis
            dataKey="t"
            stroke="#5b6478"
            tick={{ fontSize: 12 }}
            tickFormatter={(t) => `${t}s`}
            type="number"
            domain={["dataMin", "dataMax"]}
          />
          <YAxis
            stroke="#5b6478"
            tick={{ fontSize: 12 }}
            tickFormatter={(v) => `${Math.round(v)} MB`}
            domain={[0, Math.ceil(peak * 1.1)]}
          />
          <Tooltip
            contentStyle={{
              background: "#14171f",
              border: "1px solid #262b39",
              borderRadius: 8,
              fontSize: 12,
            }}
            labelFormatter={(t) => `${t}s`}
            formatter={(v: number, name: string) => [
              `${v.toFixed(0)} MB`,
              name.replace("tcp://", ""),
            ]}
          />
          {workers.map((w, i) => (
            <Line
              key={w}
              type="monotone"
              dataKey={w}
              stroke={PALETTE[i % PALETTE.length]}
              dot={false}
              strokeWidth={2}
              connectNulls
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
