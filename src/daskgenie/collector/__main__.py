"""Run the collector: ``python -m daskgenie.collector [--db PATH] [--port N]``."""

from __future__ import annotations

import argparse
import os

import uvicorn

from daskgenie.collector.app import create_app
from daskgenie.collector.store import Store


def main() -> None:
    parser = argparse.ArgumentParser(description="DaskGenie collector service")
    parser.add_argument(
        "--db",
        default=os.environ.get("DASKGENIE_DB", "daskgenie.db"),
        help="SQLite file (or :memory:)",
    )
    parser.add_argument("--host", default=os.environ.get("DASKGENIE_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("DASKGENIE_PORT", "8765")))
    args = parser.parse_args()

    app = create_app(Store(args.db))
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
