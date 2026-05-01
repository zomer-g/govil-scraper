"""Unified entry point. Replaces local_scrape.py, worker.py, over_worker.py,
nadlan_worker.py, bulk_nadlan.py, run_single_parcel.py, and start.bat
delegations.

Phase A: subcommand stubs only — each prints a TODO and exits 2. Real wiring
arrives phase by phase as the relevant subsystem is migrated.
"""
from __future__ import annotations

import argparse
import sys


def _cmd_serve(args: argparse.Namespace) -> int:
    print("[govscraper] `serve` will boot the Flask app via web.app.create_app()", file=sys.stderr)
    print("[govscraper] not yet wired in phase A — use `python app.py` for now", file=sys.stderr)
    return 2


def _cmd_worker(args: argparse.Namespace) -> int:
    """Boot the unified Worker against the chosen TaskSource."""
    import os
    from pathlib import Path

    # Import scrapers package so registry self-registration runs.
    from govscraper import scrapers as _scrapers  # noqa: F401
    from govscraper.config import work_dir
    from govscraper.worker import Worker

    if args.source == "over":
        from govscraper.worker.sources import OverOrgSource
        api_key = args.key or os.environ.get("OVER_API_KEY")
        if not api_key:
            print("[govscraper] missing --key or OVER_API_KEY env var", file=sys.stderr)
            return 2
        source = OverOrgSource(api_key, poll_interval=int(args.poll_interval) or 30)
    elif args.source == "local":
        from govscraper.worker.sources import LocalServerSource
        server = args.server or os.environ.get("RENDER_SERVER_URL") or os.environ.get("WORKER_SERVER_URL")
        api_key = args.key or os.environ.get("WORKER_API_KEY")
        worker_id = os.environ.get("WORKER_ID") or os.environ.get("HOSTNAME") or "worker-1"
        if not (server and api_key):
            print("[govscraper] missing --server/--key (or env vars)", file=sys.stderr)
            return 2
        source = LocalServerSource(server, api_key, worker_id, poll_interval=int(args.poll_interval) or 10)
    elif args.source == "nadlan-queue":
        from govscraper.worker.sources import NadlanQueueSource
        server = args.server or os.environ.get("RENDER_SERVER_URL")
        worker_id = os.environ.get("WORKER_ID") or os.environ.get("HOSTNAME") or "nadlan-worker-1"
        if not server:
            print("[govscraper] missing --server (or RENDER_SERVER_URL env)", file=sys.stderr)
            return 2
        source = NadlanQueueSource(server, worker_id, api_key=args.key)
    else:
        print(f"[govscraper] unknown --source {args.source!r}", file=sys.stderr)
        return 2

    Worker(source=source, work_dir=Path(work_dir()), poll_interval=float(args.poll_interval)).run()
    return 0


def _cmd_scrape(args: argparse.Namespace) -> int:
    print(f"[govscraper] `scrape {args.url}` not yet wired in phase A", file=sys.stderr)
    print("[govscraper] use `python local_scrape.py --url ...` for now", file=sys.stderr)
    return 2


def _cmd_bulk_nadlan(args: argparse.Namespace) -> int:
    print("[govscraper] `bulk-nadlan` not yet wired in phase A", file=sys.stderr)
    print("[govscraper] use `python bulk_nadlan.py` / `incremental_nadlan_daily.py` for now", file=sys.stderr)
    return 2


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="govscraper", description="Israeli public-data scraper")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("serve", help="Run the Flask web app")
    sp.set_defaults(func=_cmd_serve)

    sp = sub.add_parser("worker", help="Run a worker that polls a TaskSource")
    sp.add_argument("--source", choices=["local", "over", "nadlan-queue"], required=True)
    sp.add_argument("--server")
    sp.add_argument("--key")
    sp.add_argument("--poll-interval", type=float, default=5.0)
    sp.set_defaults(func=_cmd_worker)

    sp = sub.add_parser("scrape", help="One-shot scrape of a single URL")
    sp.add_argument("url")
    sp.add_argument("--upload", action="store_true")
    sp.add_argument("--server")
    sp.set_defaults(func=_cmd_scrape)

    sp = sub.add_parser("bulk-nadlan", help="Incremental nadlan-settlement scrape")
    sp.add_argument("--archive-dir", required=True)
    sp.add_argument("--settlements")
    sp.add_argument("--lookback-days", type=int, default=30)
    sp.set_defaults(func=_cmd_bulk_nadlan)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
