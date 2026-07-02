export function bytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}

export function shortKey(key: string): string {
  // "('sum-c9daa81c...', 1, 0)" -> "sum · (1, 0)"
  const m = key.match(/^\(?'?([^']+?)'?(?:,\s*(.+))?\)?$/);
  if (!m) return key;
  const name = m[1].split("-")[0];
  const idx = m[2] ? ` (${m[2].replace(/\)$/, "")})` : "";
  return `${name}${idx}`;
}

export function ago(ts: number): string {
  const secs = Math.max(0, Date.now() / 1000 - ts);
  if (secs < 60) return `${Math.floor(secs)}s ago`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ago`;
  if (secs < 86400) return `${Math.floor(secs / 3600)}h ago`;
  return `${Math.floor(secs / 86400)}d ago`;
}
