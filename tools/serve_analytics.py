#!/usr/bin/env python3
"""Serve the read-only local analytics UI and API."""

from __future__ import annotations

import argparse
from pathlib import Path

from ph_catalog.analytics_server import serve


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--site", type=Path, default=Path("site"))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    serve(args.database, args.site, args.host, args.port)


if __name__ == "__main__":
    main()
