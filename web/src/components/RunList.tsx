import { useDeleteRun, useRuns } from "../api";
import { ago } from "../format";

interface Props {
  selected: string | null;
  onSelect: (id: string) => void;
}

export function RunList({ selected, onSelect }: Props) {
  const { data: runs, isLoading } = useRuns();
  const del = useDeleteRun();

  if (isLoading) return <div className="runlist empty">Loading…</div>;
  if (!runs || runs.length === 0)
    return (
      <div className="runlist empty">
        No runs yet. Register a cluster to start profiling.
      </div>
    );

  return (
    <div className="runlist">
      {runs.map((r) => {
        const deaths = r.counts.deaths ?? 0;
        return (
          <button
            key={r.id}
            className={`run-card${r.id === selected ? " active" : ""}`}
            onClick={() => onSelect(r.id)}
          >
            <div className="name">
              <span>{r.name}</span>
              <span style={{ display: "flex", gap: 6, alignItems: "center" }}>
                {deaths > 0 && <span className="pill">{deaths} died</span>}
                <span
                  className="btn-icon"
                  role="button"
                  tabIndex={0}
                  title="Delete run"
                  onClick={(e) => {
                    e.stopPropagation();
                    if (confirm(`Delete run "${r.name}"?`)) del.mutate(r.id);
                  }}
                >
                  ✕
                </span>
              </span>
            </div>
            <div className="meta">
              <span>{ago(r.created_at)}</span>
              <span>{r.counts.workers ?? 0} workers</span>
              <span>{r.counts.samples ?? 0} samples</span>
            </div>
          </button>
        );
      })}
    </div>
  );
}
