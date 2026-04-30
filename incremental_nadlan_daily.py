"""
Daily incremental nadlan.gov.il deal updater — local CLI wrapper.

Looks for ``<archive_dir>/checkpoint.json``. If absent, runs a bootstrap; else
runs an incremental and updates the checkpoint in place.

Schedule via Windows Task Scheduler or cron. Sample task command:

    python incremental_nadlan_daily.py --archive-dir D:\\nadlan_archive

For a smoke test against one or two cities:

    python incremental_nadlan_daily.py --archive-dir test_archive --settlements 3000,4000
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime

import nadlan_incremental_engine as eng

logger = logging.getLogger(__name__)

CHECKPOINT_FILE = "checkpoint.json"


def _load_checkpoint(archive_dir: str) -> dict | None:
    p = os.path.join(archive_dir, CHECKPOINT_FILE)
    if not os.path.exists(p):
        return None
    with open(p, encoding="utf-8") as f:
        ck = json.load(f)
    # known_urls is a set in memory but stored as a sorted list on disk.
    if isinstance(ck.get("known_urls"), list):
        ck["known_urls"] = set(ck["known_urls"])
    return ck


def _save_checkpoint(archive_dir: str, ck: dict):
    p = os.path.join(archive_dir, CHECKPOINT_FILE)
    serial = dict(ck)
    if isinstance(serial.get("known_urls"), set):
        serial["known_urls"] = sorted(serial["known_urls"])
    tmp = p + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(serial, f, ensure_ascii=False)
    os.replace(tmp, p)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--archive-dir", required=True,
                    help="Directory holding nadlan_master.csv + checkpoint.json")
    ap.add_argument("--settlements", default=None,
                    help="Comma-separated setlCodes to scope the run "
                         "(e.g. '3000,4000,5000'). Default: all settlements.")
    ap.add_argument("--lookback-days", type=int, default=90,
                    help="Backfill window for incremental (default 90)")
    ap.add_argument("--force-bootstrap", action="store_true",
                    help="Ignore existing checkpoint and re-bootstrap")
    args = ap.parse_args()

    os.makedirs(args.archive_dir, exist_ok=True)
    settlements_filter = ([s.strip() for s in args.settlements.split(",")]
                          if args.settlements else None)

    ck = None if args.force_bootstrap else _load_checkpoint(args.archive_dir)

    started = datetime.now()
    if ck and ck.get("archive_csv") and \
            os.path.exists(os.path.join(args.archive_dir, ck["archive_csv"])):
        logger.info("Running INCREMENTAL (last run: %s, master rows: %s)",
                    ck.get("last_run_utc"), ck.get("total_archived"))
        new_count, ck = eng.run_incremental(
            archive_dir=args.archive_dir,
            checkpoint=ck,
            lookback_days=args.lookback_days,
            settlements_filter=settlements_filter,
        )
        logger.info("Incremental finished: +%d deals in %s",
                    new_count, datetime.now() - started)
    else:
        logger.info("Running BOOTSTRAP (no usable checkpoint found)")
        file_info, ck = eng.run_bootstrap(
            archive_dir=args.archive_dir,
            settlements_filter=settlements_filter,
        )
        logger.info("Bootstrap finished: %d deals in %s",
                    file_info["record_count"], datetime.now() - started)

    _save_checkpoint(args.archive_dir, ck)
    logger.info("Checkpoint saved to %s", os.path.join(args.archive_dir, CHECKPOINT_FILE))


if __name__ == "__main__":
    main()
