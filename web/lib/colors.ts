// Stable color-by-layer used by both the task graph and the timeline, so a
// layer looks the same everywhere.

export const LAYER_PALETTE = [
  "#3b5bdb",
  "#2b8a3e",
  "#c2410c",
  "#7048e8",
  "#0b7285",
  "#a61e4d",
  "#5c940d",
  "#862e9c",
  "#1098ad",
  "#e8590c",
];

export function baseName(layer: string): string {
  return layer.replace(/-[0-9a-f]{6,}$/i, "").split("-").slice(0, 2).join("-") || layer;
}

// A closure that assigns palette colors to layer groups on first sight, stably.
export function layerColorMap(): (layer: string) => string {
  const map = new Map<string, string>();
  return (layer: string) => {
    const key = baseName(layer);
    if (!map.has(key)) map.set(key, LAYER_PALETTE[map.size % LAYER_PALETTE.length]);
    return map.get(key)!;
  };
}
