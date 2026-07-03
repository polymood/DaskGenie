"use client";

import { MemoryExplorer } from "@/components/MemoryExplorer";
import { LayerMemoryChart } from "@/components/LayerMemoryChart";

export default function TimelinePage({ params }: { params: { id: string } }) {
  return (
    <>
      <div className="section-label">Worker memory over time</div>
      <MemoryExplorer runId={params.id} />

      <div className="section-label" style={{ marginTop: 24 }}>
        Allocations by task layer over time
      </div>
      <LayerMemoryChart runId={params.id} />
    </>
  );
}
