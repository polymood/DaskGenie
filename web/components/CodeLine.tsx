"use client";

import { Prism as SyntaxHighlighter } from "react-syntax-highlighter";
import { oneLight } from "react-syntax-highlighter/dist/esm/styles/prism";

// A single source line with syntax highlighting, shown where a task's source
// attribution points. Kept intentionally minimal — one line, no line numbers.
export function CodeLine({ code }: { code: string }) {
  return (
    <div className="code">
      <SyntaxHighlighter
        language="python"
        style={oneLight}
        customStyle={{ margin: 0, padding: "10px 14px", background: "var(--panel-2)" }}
        wrapLongLines
      >
        {code}
      </SyntaxHighlighter>
    </div>
  );
}
