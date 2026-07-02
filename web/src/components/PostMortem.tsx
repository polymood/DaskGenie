import { useDeaths, useGraph } from "../api";
import { bytes, shortKey } from "../format";
import type { ChunkMeta, GraphLayer } from "../types";

interface Props {
  runId: string;
}

// Match a suspect task key like "('sum-abc', 1, 0)" to the graph layer that
// produced it (layer names are prefixes like "sum-abc"). This is the join that
// turns a dead task into a source line.
function sourceFor(key: string, layers: GraphLayer[]): GraphLayer | undefined {
  const token = key.replace(/[()']/g, "").split(",")[0].trim();
  return layers.find(
    (l) => token.startsWith(l.layer) || l.layer.startsWith(token),
  );
}

function ChunkTags({ chunks }: { chunks: ChunkMeta[] }) {
  if (chunks.length === 0) return null;
  return (
    <div className="chunks">
      {chunks.map((c, i) => (
        <span className="chunk-tag" key={i}>
          ({c.shape.join(", ")}) {c.dtype} = <b>{bytes(c.nbytes)}</b>
        </span>
      ))}
    </div>
  );
}

export function PostMortem({ runId }: Props) {
  const { data: deaths, isLoading } = useDeaths(runId);
  const { data: graph } = useGraph(runId);
  const layers = graph?.layers ?? [];

  if (isLoading) return <div className="empty">Loading…</div>;

  const oomDeaths = (deaths ?? []).filter(
    (d) => d.suspected_oom && d.suspect_keys.length > 0,
  );
  if (oomDeaths.length === 0)
    return (
      <div className="empty">
        No suspected-OOM deaths recorded for this run. Workers that were killed
        mid-task will appear here with the chunk they were holding.
      </div>
    );

  return (
    <>
      {oomDeaths.map((d, i) => (
        <div className="card death" key={i}>
          <div className="card-head">
            <span className="pill">suspected OOM</span>
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
                  <div className="src">
                    {src.filename}:{src.lineno} · {src.code_snippet}
                  </div>
                ) : (
                  <div className="src" style={{ color: "var(--faint)" }}>
                    no source mapping (upload the graph with upload_graph)
                  </div>
                )}
                <ChunkTags chunks={chunks} />
              </div>
            );
          })}
        </div>
      ))}
    </>
  );
}
