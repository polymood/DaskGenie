"use client";

import { useState } from "react";
import { useLive } from "@/lib/live";
import { MemoryChart } from "./MemoryChart";
import { SpikeInspector } from "./SpikeInspector";

// The full-page memory explorer: a large, zoomable memory-over-time chart with a
// roomy spike inspector alongside. Click any point to pin an instant and see
// what was running and allocating then.
export function MemoryExplorer({ runId }: { runId: string }) {
  const { samples, spans, deaths } = useLive();
  const [pinned, setPinned] = useState<number | null>(null);

  if (samples.length === 0) return <div className="empty">No memory samples yet.</div>;

  return (
    <div className="mem-explorer">
      <div className="mem-explorer-chart">
        <MemoryChart
          samples={samples}
          deaths={deaths}
          selectedTime={pinned}
          onSelect={setPinned}
          height={520}
        />
      </div>
      <div className="mem-explorer-side">
        <SpikeInspector
          runId={runId}
          time={pinned}
          samples={samples}
          spans={spans}
          onClose={() => setPinned(null)}
        />
      </div>
    </div>
  );
}
