import { NextRequest } from "next/server";

// Runtime proxy: the browser calls the dashboard's own /api/*, and this
// forwards to the collector. Reading COLLECTOR_URL here (not in a build-time
// rewrite) means one image works everywhere — set it to http://collector:8765
// in Docker, or leave the localhost default in dev.
const COLLECTOR = process.env.COLLECTOR_URL || "http://127.0.0.1:8765";

async function proxy(req: NextRequest, ctx: { params: { path: string[] } }) {
  const target = `${COLLECTOR}/api/${ctx.params.path.join("/")}${req.nextUrl.search}`;
  const init: RequestInit = {
    method: req.method,
    headers: { "content-type": req.headers.get("content-type") ?? "application/json" },
  };
  if (req.method !== "GET" && req.method !== "HEAD") init.body = await req.text();
  try {
    const res = await fetch(target, init);
    return new Response(res.body, {
      status: res.status,
      headers: { "content-type": res.headers.get("content-type") ?? "application/json" },
    });
  } catch {
    return new Response(JSON.stringify({ error: "collector unreachable" }), {
      status: 502,
      headers: { "content-type": "application/json" },
    });
  }
}

export const GET = proxy;
export const POST = proxy;
export const DELETE = proxy;
export const PUT = proxy;
