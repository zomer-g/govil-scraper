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
    print(f"[govscraper] `worker --source {args.source}` not yet wired in phase A", file=sys.stderr)
    print("[govscraper] use the existing worker.py / over_worker.py / nadlan_worker.py for now", file=sys.stderr)
    return 2


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
