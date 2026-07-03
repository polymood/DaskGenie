"use client";

import { MemoryDeep } from "@/components/MemoryDeep";

export default function MemoryPage({ params }: { params: { id: string } }) {
  return (
    <>
      <div className="section-label">Deep memory — memray, folded to your source lines</div>
      <MemoryDeep runId={params.id} />
    </>
  );
}
