"use client";

import { useDeaths, useGraph } from "@/lib/api";
import { bytes, layerToken, shortKey } from "@/lib/format";
import type { ChunkMeta, GraphLayer } from "@/lib/types";
import { CodeLine } from "./CodeLine";

function sourceFor(key: string, layers: GraphLayer[]): GraphLayer | undefined {
  const token = layerToken(key);
  return layers.find((l) => token.startsWith(l.layer) || l.layer.startsWith(token));
}

function Chunks({ chunks }: { chunks: ChunkMeta[] }) {
  if (chunks.length === 0) return null;
  return (
    <div className="chunks">
      {chunks.map((c, i) => (
        <span className="chunk" key={i}>
          ({c.shape.join(", ")}) {c.dtype} = <b>{bytes(c.nbytes)}</b>
        </span>
      ))}
    </div>
  );
}

export function PostMortem({ runId }: { runId: string }) {
  const { data: deaths, isLoading } = useDeaths(runId);
  const { data: graph } = useGraph(runId);
  const layers = graph?.layers ?? [];

  if (isLoading) return <div className="spinner">Loading…</div>;

  const relevant = (deaths ?? []).filter((d) => d.suspect_keys.length > 0);
  if (relevant.length === 0)
    return (
      <div className="empty">
        No worker deaths recorded for this run. A worker killed mid-task shows up here with the task
        it was running and the chunk it was holding.
      </div>
    );

  return (
    <>
      {relevant.map((d, i) => (
        <div className="death" key={i}>
          <div className="d-head">
            {d.suspected_oom ? (
              <span className="badge danger">
                <span className="dot" />
                suspected OOM
              </span>
            ) : (
              <span className="badge">removed</span>
            )}
            <span className="worker">{d.worker}</span>
          </div>
          <div className="reason">{d.reason}</div>
          {d.suspect_keys.map((key) => {
            const src = sourceFor(key, layers);
            const chunks = d.suspect_chunks.filter((c) => c.task_key === key);
            return (
              <div className="suspect" key={key}>
                <div className="task">{shortKey(key)}</div>
                {src ? (
                  <>
                    <div className="srcpath">
                      {src.filename}:{src.lineno}
                    </div>
                    <CodeLine code={src.code_snippet || "(no snippet)"} />
                  </>
                ) : (
                  <div className="srcpath faint">
                    no source mapping — capture it with track() + upload_graph()
                  </div>
                )}
                <Chunks chunks={chunks} />
              </div>
            );
          })}
        </div>
      ))}
    </>
  );
}
