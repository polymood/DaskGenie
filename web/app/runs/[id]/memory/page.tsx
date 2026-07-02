"use client";

import { useTimeline } from "@/lib/api";
import { MemoryChart } from "@/components/MemoryChart";

export default function MemoryPage({ params }: { params: { id: string } }) {
  const { data: samples, isLoading } = useTimeline(params.id);

  if (isLoading) return <div className="spinner">Loading…</div>;
  if (!samples || samples.length === 0)
    return <div className="empty">No memory samples for this run yet.</div>;

  return (
    <>
      <div className="section-label">Per-worker resident memory (RSS)</div>
      <MemoryChart samples={samples} />
    </>
  );
}
