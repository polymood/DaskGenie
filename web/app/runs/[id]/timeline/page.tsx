"use client";

import { AlignedTimeline } from "@/components/AlignedTimeline";

export default function TimelinePage({ params }: { params: { id: string } }) {
  return (
    <>
      <div className="section-label">Memory &amp; task stream — time flows top to bottom</div>
      <AlignedTimeline runId={params.id} />
    </>
  );
}
