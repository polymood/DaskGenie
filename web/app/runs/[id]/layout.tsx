"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { deleteRun, useRun } from "@/lib/api";
import { LiveProvider, useLive } from "@/lib/live";

const LIVE_WINDOW_MS = 8000; // no frame within this → the run is idle/finished

function LiveDot() {
  const { connected, lastFrameAt } = useLive();
  const [now, setNow] = useState(() => Date.now());
  // re-evaluate so a run flips to "idle" once its stream goes quiet
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 2000);
    return () => clearInterval(t);
  }, []);
  const live = connected && lastFrameAt > 0 && now - lastFrameAt < LIVE_WINDOW_MS;
  const label = live ? "live" : lastFrameAt > 0 ? "idle" : connected ? "waiting" : "offline";
  return (
    <span className="livedot" title={live ? "Streaming live" : "No data arriving"}>
      <span className={`dot${live ? " on" : ""}`} />
      {label}
    </span>
  );
}

function ConfirmDelete({
  name,
  busy,
  onCancel,
  onConfirm,
}: {
  name: string;
  busy: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  // Close on Escape; a proper in-app modal instead of the browser confirm().
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onCancel();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onCancel]);

  return (
    <div className="modal-backdrop" onClick={onCancel}>
      <div className="modal" onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true">
        <div className="modal-head">Delete run</div>
        <div className="modal-body">
          Delete <span className="mono">{name}</span> and all of its samples, spans, deaths and
          deep-memory data? This can&apos;t be undone.
        </div>
        <div className="modal-foot">
          <button className="btn" onClick={onCancel} disabled={busy}>
            Cancel
          </button>
          <button className="btn danger solid" onClick={onConfirm} disabled={busy}>
            {busy ? "Deleting…" : "Delete run"}
          </button>
        </div>
      </div>
    </div>
  );
}

function RunChrome({ id, children }: { id: string; children: React.ReactNode }) {
  const { data: run } = useRun(id);
  const { deaths } = useLive();
  const pathname = usePathname();
  const router = useRouter();
  const base = `/runs/${id}`;
  const deathCount = deaths.length;
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);

  const tabs = [
    { href: base, label: "Overview" },
    { href: `${base}/timeline`, label: "Timeline" },
    { href: `${base}/workers`, label: "Workers" },
    { href: `${base}/stream`, label: "Task stream" },
    { href: `${base}/graph`, label: "Graph" },
    { href: `${base}/memory`, label: "Memory" },
    { href: `${base}/postmortem`, label: `Post-mortem${deathCount ? ` · ${deathCount}` : ""}` },
  ];

  async function onConfirmDelete() {
    setDeleting(true);
    try {
      await deleteRun(id);
      router.push("/");
    } finally {
      setDeleting(false);
      setConfirmOpen(false);
    }
  }

  return (
    <>
      <div className="run-header">
        <h1>{run?.name ?? "…"}</h1>
        <span className="rid">{id}</span>
        {run?.origin || run?.origin_ip ? (
          <span className="origin mono" title="Machine that opened this run">
            {run.origin || "?"}
            {run.origin_ip ? ` · ${run.origin_ip}` : ""}
          </span>
        ) : null}
        <LiveDot />
        <span className="spacer" />
        <button className="btn danger" onClick={() => setConfirmOpen(true)}>
          Delete run
        </button>
      </div>
      <nav className="tabs">
        {tabs.map((t) => (
          <Link key={t.href} href={t.href} className={pathname === t.href ? "active" : ""}>
            {t.label}
          </Link>
        ))}
      </nav>
      <div className="section">{children}</div>
      {confirmOpen && (
        <ConfirmDelete
          name={run?.name ?? id}
          busy={deleting}
          onCancel={() => !deleting && setConfirmOpen(false)}
          onConfirm={onConfirmDelete}
        />
      )}
    </>
  );
}

export default function RunLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: { id: string };
}) {
  return (
    <LiveProvider runId={params.id}>
      <RunChrome id={params.id}>{children}</RunChrome>
    </LiveProvider>
  );
}
