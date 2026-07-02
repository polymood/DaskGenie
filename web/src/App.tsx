import { useState } from "react";
import { RunList } from "./components/RunList";
import { RunView } from "./components/RunView";

export function App() {
  const [runId, setRunId] = useState<string | null>(null);

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          <h1>
            Dask<span className="dot">Genie</span>
          </h1>
          <small>which chunk killed the worker</small>
        </div>
        <RunList selected={runId} onSelect={setRunId} />
      </aside>
      <main className="main">
        {runId ? (
          <RunView runId={runId} />
        ) : (
          <div className="empty">
            Select a run to inspect its memory, deaths, and source map.
          </div>
        )}
      </main>
    </div>
  );
}
