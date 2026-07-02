/** @type {import('next').NextConfig} */

// The dashboard proxies /api to the collector via a runtime route handler
// (see app/api/[...path]/route.ts) so COLLECTOR_URL is read at request time —
// the same image works in local dev and in Docker without a rebuild.
const nextConfig = {
  output: "standalone",
};

export default nextConfig;
