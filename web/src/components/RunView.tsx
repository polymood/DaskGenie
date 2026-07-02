import { useState } from "react";
import { useDeaths, useRuns } from "../api";
import { PostMortem } from "./PostMortem";
import { Timeline } from "./Timeline";

type Tab = "postmortem" | "timeline";

interface Props {
  runId: string;
}

export function RunView({ runId }: Props) {
  const [tab, setTab] = useState<Tab>("postmortem");
  const { data: runs } = useRuns();
  const { data: deaths } = useDeaths(runId);
  const run = runs?.find((r) => r.id === runId);
  const deathCount = deaths?.length ?? 0;

  return (
    <>
      <div className="header">
        <div>
          <h2>{run?.name ?? runId}</h2>
          <div className="sub">{runId}</div>
        </div>
      </div>

      <div className="tabs">
        <button
          className={`tab${tab === "postmortem" ? " active" : ""}`}
          onClick={() => setTab("postmortem")}
        >
          Post-mortem{deathCount > 0 ? ` (${deathCount})` : ""}
        </button>
        <button
          className={`tab${tab === "timeline" ? " active" : ""}`}
          onClick={() => setTab("timeline")}
        >
          Memory timeline
        </button>
      </div>

      {tab === "postmortem" ? (
        <PostMortem runId={runId} />
      ) : (
        <Timeline runId={runId} />
      )}
    </>
  );
}
