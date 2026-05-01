"""
Upload parcels.csv to /api/nadlan/bulk-queue in small chunks so Render's
512 MB instance can handle each request without OOM.

Each chunk is sent as a fresh multipart upload; the endpoint is idempotent
(INSERT OR IGNORE), so re-running the script is safe.

Usage:
    python upload_parcels_chunked.py parcels_for_distributed_upload.csv \
           --server https://govil-scraper.onrender.com \
           --worker-key $WORKER_API_KEY \
           --chunk-size 25000
"""
import argparse
import csv
import io
import sys
import time

import requests


def _chunks(reader, fieldnames, chunk_size):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=fieldnames)
    w.writeheader()
    n = 0
    for row in reader:
        w.writerow(row)
        n += 1
        if n >= chunk_size:
            yield buf.getvalue().encode("utf-8-sig"), n
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=fieldnames)
            w.writeheader()
            n = 0
    if n:
        yield buf.getvalue().encode("utf-8-sig"), n


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("csv_path")
    ap.add_argument("--server", required=True)
    ap.add_argument("--worker-key", required=True)
    ap.add_argument("--chunk-size", type=int, default=25000)
    ap.add_argument("--filter-status", default="",
                    help="server-side filter (already applied client-side, optional)")
    ap.add_argument("--pause", type=float, default=0.5,
                    help="seconds to wait between chunks to avoid overwhelming "
                         "the server (Render Starter is sensitive to bursts)")
    args = ap.parse_args()

    headers = {"X-Worker-Key": args.worker_key}
    url = args.server.rstrip("/") + "/api/nadlan/bulk-queue"

    total_inserted = 0
    total_skipped = 0
    chunk_idx = 0

    with open(args.csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for body, count in _chunks(reader, fieldnames, args.chunk_size):
            chunk_idx += 1
            for attempt in range(3):
                try:
                    files = {"file": (f"chunk_{chunk_idx}.csv", body,
                                      "text/csv; charset=utf-8")}
                    data = {}
                    if args.filter_status:
                        data["filter_status"] = args.filter_status
                    t0 = time.time()
                    r = requests.post(url, headers=headers,
                                      files=files, data=data, timeout=300)
                    elapsed = time.time() - t0
                    if r.status_code != 200:
                        print(f"  chunk {chunk_idx} ({count} rows): HTTP {r.status_code} "
                              f"{r.text[:200]}", flush=True)
                        if attempt < 2:
                            time.sleep(5 * (attempt + 1))
                            continue
                        sys.exit(1)
                    j = r.json()
                    total_inserted += j.get("inserted", 0)
                    total_skipped += j.get("skipped", 0)
                    print(f"  chunk {chunk_idx:>3} ({count:>6,} rows, {len(body)/1e6:.1f}MB): "
                          f"+{j.get('inserted', 0):>5,} new, skip {j.get('skipped', 0):>4} "
                          f"({elapsed:.1f}s)",
                          flush=True)
                    break
                except requests.RequestException as e:
                    print(f"  chunk {chunk_idx}: {e}; retry {attempt+1}", flush=True)
                    time.sleep(5 * (attempt + 1))
            else:
                sys.exit(1)
            if args.pause:
                time.sleep(args.pause)

    print()
    print(f"DONE: {total_inserted:,} new, {total_skipped:,} skipped, "
          f"{chunk_idx} chunks")


if __name__ == "__main__":
    main()
