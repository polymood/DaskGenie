"use client";

import { TaskStream } from "@/components/TaskStream";

export default function StreamPage() {
  return (
    <>
      <div className="section-label">Task stream · live</div>
      <TaskStream />
    </>
  );
}
