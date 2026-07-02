import type { Metadata } from "next";
import { Inter } from "next/font/google";
import Link from "next/link";
import "./globals.css";
import "@xyflow/react/dist/style.css";
import { Sidebar } from "@/components/Sidebar";

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" });

export const metadata: Metadata = {
  title: "DaskGenie",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={inter.variable}>
      <body>
        <div className="app">
          <header className="topbar">
            <Link href="/" className="brand">
              Dask<span>Genie</span>
            </Link>
            <div className="sep" />
            <span className="ctx">dask inspection</span>
            <div className="spacer" />
            <span className="status">
              <span className="dot" />
              collector connected
            </span>
          </header>
          <div className="shell">
            <aside className="sidebar">
              <Sidebar />
            </aside>
            <div className="content">{children}</div>
          </div>
        </div>
      </body>
    </html>
  );
}
