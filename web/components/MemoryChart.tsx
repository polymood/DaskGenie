"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { bytes } from "@/lib/format";
import type { Sample } from "@/lib/types";

const PALETTE = ["#4f46e5", "#0891b2", "#16a34a", "#d97706", "#db2777", "#7c3aed"];

function toRows(samples: Sample[]) {
  if (samples.length === 0) return { rows: [] as Record<string, number>[], workers: [] as string[] };
  const t0 = Math.min(...samples.map((s) => s.timestamp));
  const workers = [...new Set(samples.map((s) => s.worker))];
  const byBucket = new Map<number, Record<string, number>>();
  for (const s of samples) {
    const t = Math.round((s.timestamp - t0) * 4) / 4;
    const row = byBucket.get(t) ?? { t };
    row[s.worker] = s.rss_bytes;
    byBucket.set(t, row);
  }
  const rows = [...byBucket.values()].sort((a, b) => a.t - b.t);
  return { rows, workers };
}

export function MemoryChart({ samples }: { samples: Sample[] }) {
  const { rows, workers } = toRows(samples);
  const peak = Math.max(1, ...samples.map((s) => s.rss_bytes));

  return (
    <div className="panel pad">
      <div className="legend">
        {workers.map((w, i) => (
          <span key={w}>
            <span className="sw" style={{ background: PALETTE[i % PALETTE.length] }} />
            {w.replace("tcp://", "")}
          </span>
        ))}
      </div>
      <ResponsiveContainer width="100%" height={380}>
        <LineChart data={rows} margin={{ top: 8, right: 12, bottom: 4, left: 12 }}>
          <CartesianGrid stroke="#eef0f3" vertical={false} />
          <XAxis
            dataKey="t"
            type="number"
            domain={["dataMin", "dataMax"]}
            tick={{ fontSize: 12, fill: "#9298a2" }}
            tickFormatter={(t) => `${t}s`}
            stroke="#e6e8ec"
          />
          <YAxis
            tick={{ fontSize: 12, fill: "#9298a2" }}
            tickFormatter={(v) => bytes(v)}
            domain={[0, Math.ceil(peak * 1.1)]}
            stroke="#e6e8ec"
            width={64}
          />
          <Tooltip
            contentStyle={{
              borderRadius: 8,
              border: "1px solid #e6e8ec",
              boxShadow: "0 4px 12px rgba(16,24,40,0.08)",
              fontSize: 12,
            }}
            labelFormatter={(t) => `t = ${t}s`}
            formatter={(v: number, name: string) => [bytes(v), name.replace("tcp://", "")]}
          />
          {workers.map((w, i) => (
            <Line
              key={w}
              type="monotone"
              dataKey={w}
              stroke={PALETTE[i % PALETTE.length]}
              strokeWidth={2}
              dot={false}
              connectNulls
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
