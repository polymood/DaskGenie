"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { deleteRun, useDeaths, useRun } from "@/lib/api";

export default function RunLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: { id: string };
}) {
  const { id } = params;
  const { data: run } = useRun(id);
  const { data: deaths } = useDeaths(id);
  const pathname = usePathname();
  const router = useRouter();
  const base = `/runs/${id}`;
  const deathCount = deaths?.length ?? 0;

  const tabs = [
    { href: base, label: "Overview" },
    { href: `${base}/postmortem`, label: `Post-mortem${deathCount ? ` · ${deathCount}` : ""}` },
    { href: `${base}/memory`, label: "Memory" },
    { href: `${base}/graph`, label: "Task graph" },
  ];

  async function onDelete() {
    if (!confirm(`Delete run "${run?.name ?? id}"?`)) return;
    await deleteRun(id);
    router.push("/");
  }

  return (
    <>
      <div className="run-header">
        <h1>{run?.name ?? "…"}</h1>
        <span className="rid">{id}</span>
        <span className="spacer" />
        <button className="btn danger" onClick={onDelete}>
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
    </>
  );
}
