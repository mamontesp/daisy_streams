"""CLI script to launch the FastAPI inspection server via Uvicorn."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Ensure project root is on sys.path
ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

import uvicorn
from services.api.api.config import APISettings


def main() -> None:
    settings = APISettings()

    parser = argparse.ArgumentParser(description="Daisy Streams FastAPI Inspection Service")
    parser.add_argument(
        "--host",
        type=str,
        default=settings.api_host,
        help=f"Bind host interface (default: {settings.api_host})",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=settings.api_port,
        help=f"Bind port (default: {settings.api_port})",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        default=False,
        help="Enable auto-reload on code changes (development)",
    )

    args = parser.parse_args()

    print(f"Starting Daisy Streams Inspection API on http://{args.host}:{args.port}")
    uvicorn.run(
        "services.api.api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
