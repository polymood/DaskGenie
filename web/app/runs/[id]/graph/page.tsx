"use client";

import { TaskGraph } from "@/components/TaskGraph";

export default function GraphPage({ params }: { params: { id: string } }) {
  return (
    <>
      <div className="section-label">Task graph — nodes in red were in flight at a worker death</div>
      <TaskGraph runId={params.id} />
    </>
  );
}
