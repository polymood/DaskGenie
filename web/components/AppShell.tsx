"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { Sidebar } from "@/components/Sidebar";

// Array cells stacked into a flamegraph silhouette — arrays + flamegraphs, in
// Dask's warm palette. Mirrors app/icon.svg.
const LOGO_COLS = [2, 4, 3, 5, 4];
const LOGO_RAMP = ["#D6402E", "#E8552D", "#F2762E", "#FDA33E", "#FFC83D"];
function Logo() {
  const cells: React.ReactNode[] = [];
  LOGO_COLS.forEach((h, c) => {
    for (let r = 0; r < h; r++) {
      cells.push(
        <rect
          key={`${c}-${r}`}
          x={2 + c * 6}
          y={30 - (r + 1) * 6}
          width={5}
          height={5}
          rx={1}
          fill={LOGO_RAMP[Math.min(r, LOGO_RAMP.length - 1)]}
        />,
      );
    }
  });
  return (
    <svg width="20" height="20" viewBox="0 0 32 32" aria-hidden style={{ display: "block" }}>
      {cells}
    </svg>
  );
}

// Client shell so the runs sidebar can collapse (state persisted). Keeps the
// root layout a server component.
export function AppShell({ children }: { children: React.ReactNode }) {
  const [collapsed, setCollapsed] = useState(false);

  useEffect(() => {
    setCollapsed(localStorage.getItem("dg-sidebar-collapsed") === "1");
  }, []);
  const toggle = () => {
    setCollapsed((c) => {
      const next = !c;
      localStorage.setItem("dg-sidebar-collapsed", next ? "1" : "0");
      return next;
    });
  };

  return (
    <div className="app">
      <header className="topbar">
        <button className="rail-toggle" onClick={toggle} title="Toggle runs sidebar">
          {collapsed ? "»" : "«"}
        </button>
        <Link href="/" className="brand">
          <Logo />
          <span className="brand-text">
            Dask<span>Genie</span>
          </span>
        </Link>
        <div className="sep" />
        <span className="ctx">dask inspection</span>
        <div className="spacer" />
        <span className="status">
          <span className="dot" />
          collector connected
        </span>
      </header>
      <div className={`shell${collapsed ? " collapsed" : ""}`}>
        <aside className="sidebar">
          <Sidebar />
        </aside>
        <div className="content">{children}</div>
      </div>
    </div>
  );
}
