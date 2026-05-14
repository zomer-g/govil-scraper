"""
Postgres backend for nadlan distributed scrape state.

Keeps the rest of the app on SQLite (collections/files/archives/tasks for
gov.il scrapes) but moves the two pieces that must survive Render redeploys
into Postgres:

  * nadlan_tasks  — the 746k parcel queue
  * nadlan_deals  — every deal the workers upload (replaces the central
                    nadlan_deals_master.csv that lived on the wiped disk)

Activation: set DATABASE_URL in the environment. Render's managed Postgres
provides this automatically when a database service is attached.
Without DATABASE_URL the original SQLite paths in storage.py + the CSV
in nadlan_api_routes.py keep working unchanged (used by local dev/tests).

Connection pooling is intentionally small (1-3 conns per gunicorn worker);
Render free Postgres allows ~20 total which fits gunicorn's 2×4 layout.
"""
from __future__ import annotations

import datetime
import logging
import os
import threading
from typing import Iterable, Optional

logger = logging.getLogger(__name__)


# Header for the deals CSV download — must stay in sync with the worker's
# row layout (govscraper.legacy.nadlan_worker DEAL_FIELDS / PARCEL_FIELDS /
# META_FIELDS) so a downloader can drop the bytes into Excel unchanged.
DEALS_CSV_HEADER = [
    "gush", "chelka", "locality", "municipality",
    "status", "legal_area_sqm", "area_sqm",
    "centroid_lat", "centroid_lon",
    "meta_setl_name", "meta_neigh_name", "meta_base_level",
    "dealDate", "dealAmount", "priceSM",
    "roomNum", "floor", "assetArea", "yearBuilt", "buildingFloors",
    "dealNature", "hokHamecher",
    "trend_rate", "trend_years", "prev_deals",
    "address", "parcelNum", "neighborhoodName", "ownership",
    "assetId", "addressId", "polygonId",
    "streetCode", "settlmentID", "neighborhoodId", "row_id",
    "worker_id", "scraped_at",
]


_NADLAN_TASKS_DDL = """
CREATE TABLE IF NOT EXISTS nadlan_tasks (
    parcel_id      TEXT PRIMARY KEY,
    gush           TEXT NOT NULL,
    chelka         TEXT NOT NULL,
    locality       TEXT DEFAULT '',
    municipality   TEXT DEFAULT '',
    parcel_type    TEXT DEFAULT '',
    status_text    TEXT DEFAULT '',
    legal_area_sqm TEXT DEFAULT '',
    area_sqm       TEXT DEFAULT '',
    centroid_lat   TEXT DEFAULT '',
    centroid_lon   TEXT DEFAULT '',
    state          TEXT DEFAULT 'pending',
    worker_id      TEXT DEFAULT '',
    deals_count    INTEGER DEFAULT 0,
    error          TEXT DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at     TIMESTAMPTZ,
    completed_at   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_nadlan_state ON nadlan_tasks(state);
CREATE INDEX IF NOT EXISTS idx_nadlan_claimed ON nadlan_tasks(claimed_at);
"""


_NADLAN_SETTLEMENT_TASKS_DDL = """
CREATE TABLE IF NOT EXISTS nadlan_settlement_tasks (
    setl_code     TEXT PRIMARY KEY,
    setl_name     TEXT DEFAULT '',
    population    INTEGER DEFAULT 0,
    state         TEXT DEFAULT 'pending',
    worker_id     TEXT DEFAULT '',
    deals_count   INTEGER DEFAULT 0,
    total_fetch   INTEGER DEFAULT 0,   -- pages reported by API on page 1
    error         TEXT DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_nadlan_setl_state ON nadlan_settlement_tasks(state);
CREATE INDEX IF NOT EXISTS idx_nadlan_setl_claimed ON nadlan_settlement_tasks(claimed_at);
"""


_NADLAN_SETTLEMENT_SLICES_DDL = """
CREATE TABLE IF NOT EXISTS nadlan_settlement_slices (
    slice_key     TEXT PRIMARY KEY,            -- "<setl>:<room>:<sort>"
    setl_code     TEXT NOT NULL,
    setl_name     TEXT DEFAULT '',
    population    INTEGER DEFAULT 0,
    room_filter   TEXT,                        -- '1','2','3','4','5','6plus' or NULL
    sort_order    TEXT NOT NULL DEFAULT 'dealDate_down',
    state         TEXT DEFAULT 'pending',      -- pending|claimed|done|failed
    worker_id     TEXT DEFAULT '',
    deals_count   INTEGER DEFAULT 0,
    total_rows    INTEGER DEFAULT 0,           -- as reported by API on this slice
    error         TEXT DEFAULT '',
    attempts      INTEGER DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_nadlan_slice_state
  ON nadlan_settlement_slices(state);
CREATE INDEX IF NOT EXISTS idx_nadlan_slice_pending_pop
  ON nadlan_settlement_slices(state, population DESC NULLS LAST)
  WHERE state = 'pending';
CREATE INDEX IF NOT EXISTS idx_nadlan_slice_setl
  ON nadlan_settlement_slices(setl_code);
CREATE INDEX IF NOT EXISTS idx_nadlan_slice_claimed
  ON nadlan_settlement_slices(claimed_at);
"""


_NADLAN_DEALS_DDL = """
CREATE TABLE IF NOT EXISTS nadlan_deals (
    id              BIGSERIAL PRIMARY KEY,
    parcel_id       TEXT NOT NULL,
    gush            TEXT,
    chelka          TEXT,
    locality        TEXT,
    municipality    TEXT,
    status          TEXT,
    legal_area_sqm  TEXT,
    area_sqm        TEXT,
    centroid_lat    TEXT,
    centroid_lon    TEXT,
    meta_setl_name  TEXT,
    meta_neigh_name TEXT,
    meta_base_level TEXT,
    deal_date       TEXT,
    deal_amount     TEXT,
    price_sm        TEXT,
    room_num        TEXT,
    floor           TEXT,
    asset_area      TEXT,
    year_built      TEXT,
    building_floors TEXT,
    deal_nature     TEXT,
    hok_hamecher    TEXT,
    trend_rate      TEXT,
    trend_years     TEXT,
    prev_deals      TEXT,
    address         TEXT,
    parcel_num      TEXT,
    neighborhood_name TEXT,
    ownership       TEXT,
    asset_id        TEXT,
    address_id      TEXT,
    polygon_id      TEXT,
    street_code     TEXT,
    settlment_id    TEXT,
    neighborhood_id TEXT,
    row_id          TEXT,
    worker_id       TEXT,
    scraped_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_nadlan_deals_parcel ON nadlan_deals(parcel_id);
CREATE INDEX IF NOT EXISTS idx_nadlan_deals_scraped ON nadlan_deals(scraped_at);
"""


# Map CSV field names → DB column names (DB uses snake_case so SQL stays sane).
_DEAL_CSV_TO_DB = {
    "gush": "gush", "chelka": "chelka",
    "locality": "locality", "municipality": "municipality",
    "status": "status", "legal_area_sqm": "legal_area_sqm",
    "area_sqm": "area_sqm",
    "centroid_lat": "centroid_lat", "centroid_lon": "centroid_lon",
    "meta_setl_name": "meta_setl_name",
    "meta_neigh_name": "meta_neigh_name",
    "meta_base_level": "meta_base_level",
    "dealDate": "deal_date", "dealAmount": "deal_amount",
    "priceSM": "price_sm",
    "roomNum": "room_num", "floor": "floor",
    "assetArea": "asset_area", "yearBuilt": "year_built",
    "buildingFloors": "building_floors",
    "dealNature": "deal_nature", "hokHamecher": "hok_hamecher",
    "trend_rate": "trend_rate", "trend_years": "trend_years",
    "prev_deals": "prev_deals",
    "address": "address", "parcelNum": "parcel_num",
    "neighborhoodName": "neighborhood_name", "ownership": "ownership",
    "assetId": "asset_id", "addressId": "address_id",
    "polygonId": "polygon_id", "streetCode": "street_code",
    "settlmentID": "settlment_id", "neighborhoodId": "neighborhood_id",
    "row_id": "row_id", "worker_id": "worker_id",
    "scraped_at": "scraped_at",
}
# Reverse for SELECT...AS aliasing on download.
_DEAL_DB_TO_CSV = {v: k for k, v in _DEAL_CSV_TO_DB.items()}

_DB_COLS_ORDERED = [_DEAL_CSV_TO_DB[c] for c in DEALS_CSV_HEADER]


class PgStore:
    """Tiny psycopg-3 wrapper around the two nadlan tables.

    Threadsafe by virtue of opening a fresh connection per call (the cost
    is amortised by the connection pool psycopg keeps internally).
    """

    def __init__(self, dsn: str):
        # Lazy import so the module loads even if psycopg isn't installed
        # (that's fine — code paths only hit it when DATABASE_URL is set).
        import psycopg  # noqa
        from psycopg.rows import dict_row  # noqa
        self._psycopg = psycopg
        self._dict_row = dict_row
        self._dsn = dsn
        self._lock = threading.Lock()
        self._init_schema()

    def _conn(self):
        return self._psycopg.connect(self._dsn, row_factory=self._dict_row)

    def _init_schema(self):
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(_NADLAN_TASKS_DDL)
            cur.execute(_NADLAN_DEALS_DDL)
            cur.execute(_NADLAN_SETTLEMENT_TASKS_DDL)
            cur.execute(_NADLAN_SETTLEMENT_SLICES_DDL)
            conn.commit()

    # --- nadlan_tasks ----------------------------------------------------

    def create_tasks(self, rows: list[dict]) -> dict:
        """Bulk INSERT ... ON CONFLICT DO NOTHING.  Returns counts."""
        if not rows:
            return {"inserted": 0, "skipped": 0}
        prepared = []
        for r in rows:
            gush = (r.get("gush") or "").strip()
            chelka = (r.get("chelka") or "").strip()
            if not gush or not chelka:
                continue
            prepared.append((
                f"{gush}-{chelka}",
                gush, chelka,
                r.get("locality") or "",
                r.get("municipality") or "",
                r.get("parcel_type") or "",
                r.get("status") or "",
                str(r.get("legal_area_sqm") or ""),
                str(r.get("area_sqm") or ""),
                str(r.get("centroid_lat") or ""),
                str(r.get("centroid_lon") or ""),
            ))
        if not prepared:
            return {"inserted": 0, "skipped": len(rows)}
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            # executemany with RETURNING so we can count actual inserts.
            cur.executemany(
                """INSERT INTO nadlan_tasks
                   (parcel_id, gush, chelka, locality, municipality,
                    parcel_type, status_text, legal_area_sqm, area_sqm,
                    centroid_lat, centroid_lon)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (parcel_id) DO NOTHING""",
                prepared,
            )
            inserted = cur.rowcount
            conn.commit()
        return {"inserted": inserted, "skipped": len(rows) - inserted}

    def claim_tasks(self, worker_id: str, count: int = 1) -> list[dict]:
        """Atomic SELECT ... FOR UPDATE SKIP LOCKED + UPDATE.

        Multiple gunicorn workers (and multiple machines) can call this
        concurrently without ever returning the same parcel twice.
        """
        if count < 1:
            return []
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """WITH picked AS (
                       SELECT parcel_id FROM nadlan_tasks
                       WHERE state = 'pending'
                       ORDER BY created_at
                       FOR UPDATE SKIP LOCKED
                       LIMIT %s
                   )
                   UPDATE nadlan_tasks t
                   SET state = 'claimed',
                       worker_id = %s,
                       claimed_at = NOW()
                   FROM picked
                   WHERE t.parcel_id = picked.parcel_id
                   RETURNING t.*""",
                (count, worker_id),
            )
            rows = cur.fetchall()
            conn.commit()
        # Convert datetimes for JSON serialization
        for r in rows:
            for k in ("created_at", "claimed_at", "completed_at"):
                if r.get(k):
                    r[k] = r[k].isoformat()
        return list(rows)

    def complete_task(self, parcel_id: str, deals_count: int,
                       worker_id: str | None = None) -> bool:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            if worker_id:
                cur.execute(
                    """UPDATE nadlan_tasks
                       SET state = 'done', deals_count = %s,
                           completed_at = NOW(), error = ''
                       WHERE parcel_id = %s
                         AND state = 'claimed'
                         AND worker_id = %s""",
                    (int(deals_count or 0), parcel_id, worker_id),
                )
            else:
                cur.execute(
                    """UPDATE nadlan_tasks
                       SET state = 'done', deals_count = %s,
                           completed_at = NOW(), error = ''
                       WHERE parcel_id = %s""",
                    (int(deals_count or 0), parcel_id),
                )
            conn.commit()
            return cur.rowcount > 0

    def set_deals_count(self, parcel_id: str, deals_count: int) -> bool:
        """Adjust deals_count after the actual CSV append, without state change."""
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "UPDATE nadlan_tasks SET deals_count = %s WHERE parcel_id = %s",
                (int(deals_count or 0), parcel_id),
            )
            conn.commit()
            return cur.rowcount > 0

    def fail_task(self, parcel_id: str, error: str, transient: bool,
                   worker_id: str | None = None) -> bool:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            if transient:
                if worker_id:
                    cur.execute(
                        """UPDATE nadlan_tasks
                           SET state = 'pending', worker_id = '',
                               claimed_at = NULL, error = %s
                           WHERE parcel_id = %s
                             AND state = 'claimed'
                             AND worker_id = %s""",
                        (str(error)[:500], parcel_id, worker_id),
                    )
                else:
                    cur.execute(
                        """UPDATE nadlan_tasks
                           SET state = 'pending', worker_id = '',
                               claimed_at = NULL, error = %s
                           WHERE parcel_id = %s""",
                        (str(error)[:500], parcel_id),
                    )
            else:
                if worker_id:
                    cur.execute(
                        """UPDATE nadlan_tasks
                           SET state = 'failed', completed_at = NOW(),
                               error = %s
                           WHERE parcel_id = %s
                             AND state = 'claimed'
                             AND worker_id = %s""",
                        (str(error)[:500], parcel_id, worker_id),
                    )
                else:
                    cur.execute(
                        """UPDATE nadlan_tasks
                           SET state = 'failed', completed_at = NOW(),
                               error = %s
                           WHERE parcel_id = %s""",
                        (str(error)[:500], parcel_id),
                    )
            conn.commit()
            return cur.rowcount > 0

    def reset_stale(self, timeout_seconds: int = 600) -> int:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE nadlan_tasks
                   SET state = 'pending', worker_id = '', claimed_at = NULL
                   WHERE state = 'claimed'
                     AND claimed_at < NOW() - make_interval(secs => %s)""",
                (timeout_seconds,),
            )
            conn.commit()
            return cur.rowcount

    def status(self) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT state, COUNT(*) AS c FROM nadlan_tasks GROUP BY state"
            )
            counts = {r["state"]: r["c"] for r in cur.fetchall()}
            cur.execute(
                "SELECT COALESCE(SUM(deals_count), 0) AS t FROM nadlan_tasks"
            )
            tasks_deals = cur.fetchone()["t"]
            cur.execute("SELECT COUNT(*) AS c FROM nadlan_deals")
            deals_actual = cur.fetchone()["c"]
        total = sum(counts.values())
        return {
            "pending": counts.get("pending", 0),
            "claimed": counts.get("claimed", 0),
            "done":    counts.get("done", 0),
            "failed":  counts.get("failed", 0),
            "total":   total,
            # Two distinct numbers — sum from the tasks table is what each
            # task uploaded; count from the deals table is what's actually
            # stored. They agree under healthy operation.
            "deals_collected": int(tasks_deals or 0),
            "deals_in_db":     int(deals_actual or 0),
        }

    def clear_queue(self) -> int:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM nadlan_tasks")
            n = cur.rowcount
            conn.commit()
            return n

    def clear_deals(self) -> int:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM nadlan_deals")
            n = cur.rowcount
            conn.commit()
            return n

    # --- nadlan_deals ----------------------------------------------------

    def append_deals(self, csv_rows: list[dict]) -> int:
        """Insert deal rows. CSV-style keys are translated to db columns.

        parcel_id is computed from gush+chelka because the schema marks it
        NOT NULL but the worker only sends those two columns separately.
        """
        if not csv_rows:
            return 0
        # parcel_id first so the column list and value order line up.
        cols = ["parcel_id"] + list(_DEAL_CSV_TO_DB.values())
        rows = []
        for r in csv_rows:
            gush = (r.get("gush") or "").strip()
            chelka = (r.get("chelka") or "").strip()
            parcel_id = f"{gush}-{chelka}" if gush and chelka else ""
            rows.append(
                [parcel_id]
                + [r.get(csv_key, "") for csv_key in _DEAL_CSV_TO_DB.keys()]
            )
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join(cols)
        sql = (f"INSERT INTO nadlan_deals ({col_list}) "
               f"VALUES ({placeholders})")
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.executemany(sql, rows)
            conn.commit()
        return len(rows)

    def stream_deals_csv(self):
        """Generator yielding the deals_master.csv as bytes for streaming
        downloads — avoids materialising 600k+ rows in memory.
        """
        import csv
        import io

        # Header
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(DEALS_CSV_HEADER)
        yield buf.getvalue().encode("utf-8-sig")

        # Stream rows in chunks of 10k via server-side cursor
        with self._conn() as conn:
            with conn.cursor(name="deals_export") as cur:
                cur.itersize = 10000
                col_aliases = ", ".join(
                    f"{db} AS \"{csv_name}\""
                    for csv_name, db in _DEAL_CSV_TO_DB.items()
                )
                cur.execute(f"SELECT {col_aliases} FROM nadlan_deals "
                            f"ORDER BY id")
                while True:
                    chunk = cur.fetchmany(10000)
                    if not chunk:
                        break
                    buf = io.StringIO()
                    w = csv.DictWriter(buf, fieldnames=DEALS_CSV_HEADER,
                                       extrasaction="ignore")
                    for row in chunk:
                        w.writerow(row)
                    yield buf.getvalue().encode("utf-8-sig")

    # --- nadlan_settlement_tasks ---------------------------------------

    def settlement_create_tasks(self, settlements: list[dict]) -> dict:
        """Bulk-insert settlement tasks. Idempotent.

        ``settlements`` rows must include ``setl_code``; ``setl_name`` and
        ``population`` are stored for visibility.
        """
        if not settlements:
            return {"inserted": 0, "skipped": 0}
        prepared = []
        for s in settlements:
            code = str(s.get("setl_code") or s.get("SETL_CODE") or "").strip()
            if not code:
                continue
            # Population: API may return int, str, "", None or omit it.
            raw_pop = s.get("population") or s.get("POPULATION") or 0
            try:
                pop = int(raw_pop) if raw_pop not in ("", None) else 0
            except (TypeError, ValueError):
                pop = 0
            prepared.append((
                code,
                str(s.get("setl_name") or s.get("SETL_NAME") or ""),
                pop,
            ))
        if not prepared:
            return {"inserted": 0, "skipped": len(settlements)}
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO nadlan_settlement_tasks
                   (setl_code, setl_name, population)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (setl_code) DO NOTHING""",
                prepared,
            )
            inserted = cur.rowcount
            conn.commit()
        return {"inserted": inserted, "skipped": len(settlements) - inserted}

    def settlement_claim_tasks(self, worker_id: str, count: int = 1) -> list[dict]:
        if count < 1:
            return []
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """WITH picked AS (
                       SELECT setl_code FROM nadlan_settlement_tasks
                       WHERE state = 'pending'
                       ORDER BY population DESC NULLS LAST, created_at
                       FOR UPDATE SKIP LOCKED
                       LIMIT %s
                   )
                   UPDATE nadlan_settlement_tasks t
                   SET state = 'claimed',
                       worker_id = %s,
                       claimed_at = NOW()
                   FROM picked
                   WHERE t.setl_code = picked.setl_code
                   RETURNING t.*""",
                (count, worker_id),
            )
            rows = cur.fetchall()
            conn.commit()
        for r in rows:
            for k in ("created_at", "claimed_at", "completed_at"):
                if r.get(k):
                    r[k] = r[k].isoformat()
        return list(rows)

    def settlement_complete_task(self, setl_code: str,
                                  deals_count: int,
                                  total_fetch: int = 0) -> bool:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE nadlan_settlement_tasks
                   SET state = 'done', deals_count = %s, total_fetch = %s,
                       completed_at = NOW(), error = ''
                   WHERE setl_code = %s""",
                (int(deals_count or 0), int(total_fetch or 0), setl_code),
            )
            conn.commit()
            return cur.rowcount > 0

    def settlement_fail_task(self, setl_code: str, error: str,
                              transient: bool) -> bool:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            if transient:
                cur.execute(
                    """UPDATE nadlan_settlement_tasks
                       SET state = 'pending', worker_id = '',
                           claimed_at = NULL, error = %s
                       WHERE setl_code = %s""",
                    (str(error)[:500], setl_code),
                )
            else:
                cur.execute(
                    """UPDATE nadlan_settlement_tasks
                       SET state = 'failed', completed_at = NOW(),
                           error = %s
                       WHERE setl_code = %s""",
                    (str(error)[:500], setl_code),
                )
            conn.commit()
            return cur.rowcount > 0

    def settlement_reset_stale(self, timeout_seconds: int = 1800) -> int:
        """Default 30 min — settlements with many pages take longer than parcels."""
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE nadlan_settlement_tasks
                   SET state = 'pending', worker_id = '', claimed_at = NULL
                   WHERE state = 'claimed'
                     AND claimed_at < NOW() - make_interval(secs => %s)""",
                (timeout_seconds,),
            )
            conn.commit()
            return cur.rowcount

    def settlement_status(self) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT state, COUNT(*) AS c FROM nadlan_settlement_tasks GROUP BY state"
            )
            counts = {r["state"]: r["c"] for r in cur.fetchall()}
            cur.execute(
                "SELECT COALESCE(SUM(deals_count), 0) AS t "
                "FROM nadlan_settlement_tasks"
            )
            total_deals = cur.fetchone()["t"]
        total = sum(counts.values())
        return {
            "pending": counts.get("pending", 0),
            "claimed": counts.get("claimed", 0),
            "done":    counts.get("done", 0),
            "failed":  counts.get("failed", 0),
            "total":   total,
            "deals_collected": int(total_deals or 0),
        }

    def settlement_clear(self) -> int:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM nadlan_settlement_tasks")
            n = cur.rowcount
            conn.commit()
            return n

    # --- nadlan_settlement_slices --------------------------------------
    # Each slice = one (settlement × room × sort) tuple → one /deal-data
    # call returning ≤500 deals. Many slices per settlement; the worker
    # claims slices and the slice_worker drives UI clicks per slice.

    def slice_create_tasks(self, slices: list[dict]) -> dict:
        """Bulk-insert slice tasks. Each row needs:
           setl_code, setl_name, population, room_filter, sort_order.
        slice_key is computed from setl_code:room_filter:sort_order."""
        if not slices:
            return {"inserted": 0, "skipped": 0}
        prepared = []
        for s in slices:
            code = str(s.get("setl_code") or "").strip()
            if not code:
                continue
            room = s.get("room_filter")  # may be None for "all rooms"
            sort = s.get("sort_order") or "dealDate_down"
            slice_key = f"{code}:{room or '_'}:{sort}"
            try:
                pop = int(s.get("population") or 0)
            except (TypeError, ValueError):
                pop = 0
            prepared.append((
                slice_key, code,
                str(s.get("setl_name") or ""),
                pop, room, sort,
            ))
        if not prepared:
            return {"inserted": 0, "skipped": len(slices)}
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO nadlan_settlement_slices
                   (slice_key, setl_code, setl_name, population,
                    room_filter, sort_order)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (slice_key) DO NOTHING""",
                prepared,
            )
            inserted = cur.rowcount
            conn.commit()
        return {"inserted": inserted, "skipped": len(slices) - inserted}

    def slice_claim_tasks(self, worker_id: str, count: int = 1,
                            max_attempts: int = 3) -> list[dict]:
        """Claim N pending slices, sorted by population DESC so the big
        cities (most slices) get parallelized first.

        Slices are grouped by setl_code so a single worker tends to get
        consecutive slices of the same settlement.

        Slices with attempts >= max_attempts are auto-marked failed and
        excluded from future claims — guards against stuck-loop slices
        (tiny settlements where SPA doesn't re-fire /deal-data because
        all data is already in DOM cache).
        """
        if count < 1:
            return []
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            # First, auto-fail slices that have hit max_attempts
            cur.execute(
                """UPDATE nadlan_settlement_slices
                   SET state = 'failed', completed_at = NOW(),
                       error = 'max_attempts_exceeded'
                   WHERE state = 'pending' AND attempts >= %s""",
                (max_attempts,),
            )
            cur.execute(
                """WITH picked AS (
                       SELECT slice_key FROM nadlan_settlement_slices
                       WHERE state = 'pending' AND attempts < %s
                       ORDER BY population DESC NULLS LAST,
                                setl_code, sort_order, room_filter
                       FOR UPDATE SKIP LOCKED
                       LIMIT %s
                   )
                   UPDATE nadlan_settlement_slices t
                   SET state = 'claimed',
                       worker_id = %s,
                       claimed_at = NOW(),
                       attempts = attempts + 1
                   FROM picked
                   WHERE t.slice_key = picked.slice_key
                   RETURNING t.*""",
                (max_attempts, count, worker_id),
            )
            rows = cur.fetchall()
            conn.commit()
        for r in rows:
            for k in ("created_at", "claimed_at", "completed_at"):
                if r.get(k):
                    r[k] = r[k].isoformat()
        return list(rows)

    def slice_complete_task(self, slice_key: str,
                              deals_count: int,
                              total_rows: int = 0) -> bool:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE nadlan_settlement_slices
                   SET state = 'done', deals_count = %s,
                       total_rows = %s, completed_at = NOW(), error = ''
                   WHERE slice_key = %s""",
                (int(deals_count or 0), int(total_rows or 0), slice_key),
            )
            conn.commit()
            return cur.rowcount > 0

    def slice_fail_task(self, slice_key: str, error: str,
                          transient: bool) -> bool:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            if transient:
                cur.execute(
                    """UPDATE nadlan_settlement_slices
                       SET state = 'pending', worker_id = '',
                           claimed_at = NULL, error = %s
                       WHERE slice_key = %s""",
                    (str(error)[:500], slice_key),
                )
            else:
                cur.execute(
                    """UPDATE nadlan_settlement_slices
                       SET state = 'failed', completed_at = NOW(),
                           error = %s
                       WHERE slice_key = %s""",
                    (str(error)[:500], slice_key),
                )
            conn.commit()
            return cur.rowcount > 0

    def slice_reset_stale(self, timeout_seconds: int = 1800) -> int:
        """Reclaim slices stuck in 'claimed' for >30min."""
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE nadlan_settlement_slices
                   SET state = 'pending', worker_id = '', claimed_at = NULL
                   WHERE state = 'claimed'
                     AND claimed_at < NOW() - make_interval(secs => %s)""",
                (timeout_seconds,),
            )
            conn.commit()
            return cur.rowcount

    def slice_status(self) -> dict:
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT state, COUNT(*) AS c FROM nadlan_settlement_slices "
                "GROUP BY state"
            )
            counts = {r["state"]: r["c"] for r in cur.fetchall()}
            cur.execute(
                "SELECT COALESCE(SUM(deals_count), 0) AS t "
                "FROM nadlan_settlement_slices"
            )
            total_deals = cur.fetchone()["t"]
        total = sum(counts.values())
        return {
            "pending": counts.get("pending", 0),
            "claimed": counts.get("claimed", 0),
            "done":    counts.get("done", 0),
            "failed":  counts.get("failed", 0),
            "total":   total,
            "deals_collected": int(total_deals or 0),
        }

    def slice_clear(self) -> int:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM nadlan_settlement_slices")
            n = cur.rowcount
            conn.commit()
            return n

    def slice_seed_amount_sorts_for_capped(self) -> int:
        """For every settlement whose existing slices hit the API cap
        (total_rows > 500), generate two new `dealAmount`-sorted slices per
        existing (setl, room) combo so the worker can capture a different
        subset of the 137K-deal pool that's invisible with only date sorts.

        Doubles the per-settlement coverage from ~6,000 to ~12,000 deals.
        Idempotent — uses ON CONFLICT DO NOTHING on slice_key.

        Returns: number of new slices inserted.
        """
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """WITH capped AS (
                       SELECT DISTINCT setl_code, room_filter,
                              FIRST_VALUE(setl_name) OVER w AS setl_name,
                              FIRST_VALUE(population) OVER w AS population
                       FROM nadlan_settlement_slices
                       WHERE state = 'done'
                         AND total_rows > 500
                         AND room_filter IS NOT NULL
                       WINDOW w AS (
                           PARTITION BY setl_code
                           ORDER BY population DESC
                       )
                   ),
                   new_slices AS (
                       SELECT setl_code, room_filter, setl_name, population,
                              s.sort_order
                       FROM capped
                       CROSS JOIN (VALUES ('dealAmount_down'), ('dealAmount_up'))
                              AS s(sort_order)
                   )
                   INSERT INTO nadlan_settlement_slices
                       (slice_key, setl_code, setl_name, population,
                        room_filter, sort_order)
                   SELECT
                       setl_code || ':' || COALESCE(room_filter, '_') || ':' || sort_order,
                       setl_code, setl_name, population, room_filter, sort_order
                   FROM new_slices
                   ON CONFLICT (slice_key) DO NOTHING"""
            )
            inserted = cur.rowcount
            conn.commit()
            return inserted

    def slice_reset_failed(self) -> int:
        """Reset all failed slices back to pending with attempts=0 so the
        worker re-tries them. Used after a full run when some slices failed
        due to transient blocks rather than genuine emptiness."""
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE nadlan_settlement_slices
                   SET state = 'pending', worker_id = '',
                       claimed_at = NULL, completed_at = NULL,
                       attempts = 0, error = NULL
                   WHERE state = 'failed'"""
            )
            n = cur.rowcount
            conn.commit()
            return n

    def slice_delete_room_null(self) -> int:
        """Remove all 'all rooms' slices — they can't be UI-clicked because
        'כל החדרים' is the dropdown toggle, not a selectable option."""
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                "DELETE FROM nadlan_settlement_slices "
                "WHERE room_filter IS NULL"
            )
            n = cur.rowcount
            conn.commit()
            return n

    # -----------------------------------------------------------------
    # Read-only SQL exploration (admin SQL console)
    # -----------------------------------------------------------------

    def list_tables(self) -> list[dict]:
        """List all tables in the 'public' schema with a quick row estimate."""
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """SELECT relname AS name,
                          n_live_tup AS row_estimate
                   FROM pg_stat_user_tables
                   WHERE schemaname = 'public'
                   ORDER BY relname"""
            )
            return list(cur.fetchall())

    def describe_table(self, name: str) -> list[dict]:
        """Return columns for a table (anti-injection: regex-validated name)."""
        import re
        if not re.match(r"^[a-z_][a-z0-9_]*$", name):
            raise ValueError("invalid table name")
        with self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """SELECT column_name AS column,
                          data_type AS type,
                          is_nullable AS nullable
                   FROM information_schema.columns
                   WHERE table_schema = 'public' AND table_name = %s
                   ORDER BY ordinal_position""",
                (name,),
            )
            return list(cur.fetchall())

    def run_readonly_query(self, sql: str, *, timeout_ms: int = 30000,
                            max_rows: int = 1000) -> dict:
        """Run a read-only query (SELECT/EXPLAIN/SHOW/WITH) and return rows.

        Enforces READ ONLY transaction + statement_timeout server-side, so even
        if upstream validation slips, no data modification is possible.

        Returns: {columns, rows, row_count, truncated, elapsed_ms, error?}.
        Non-fatal errors (syntax, missing table, timeout) are returned in
        the response dict so the UI can render them cleanly.
        """
        import time
        import datetime
        import decimal
        import uuid

        def _to_json_safe(v):
            if v is None or isinstance(v, (bool, int, float, str)):
                return v
            if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
                return v.isoformat()
            if isinstance(v, (datetime.timedelta,)):
                return str(v)
            if isinstance(v, decimal.Decimal):
                # Render small Decimals as int/float for nicer UI
                try:
                    f = float(v)
                    return int(f) if f.is_integer() else f
                except Exception:
                    return str(v)
                return str(v)
            if isinstance(v, uuid.UUID):
                return str(v)
            if isinstance(v, (bytes, bytearray, memoryview)):
                return f"<bytes:{len(bytes(v))}>"
            if isinstance(v, (list, tuple)):
                return [_to_json_safe(x) for x in v]
            if isinstance(v, dict):
                return {str(k): _to_json_safe(x) for k, x in v.items()}
            return str(v)

        t0 = time.time()
        try:
            with self._conn() as conn:
                # Force read-only at the transaction level
                conn.autocommit = False
                with conn.cursor() as cur:
                    cur.execute("SET TRANSACTION READ ONLY")
                    cur.execute(
                        f"SET LOCAL statement_timeout = {int(timeout_ms)}"
                    )
                    cur.execute(sql)
                    elapsed_ms = int((time.time() - t0) * 1000)
                    if cur.description is None:
                        # No rowset (rare for SELECT, common for SHOW with no
                        # output) — return empty result successfully
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        return {
                            "columns": [], "rows": [], "row_count": 0,
                            "truncated": False, "elapsed_ms": elapsed_ms,
                        }
                    cols = [d.name for d in cur.description]
                    fetched = cur.fetchmany(max_rows + 1)
                    truncated = len(fetched) > max_rows
                    if truncated:
                        fetched = fetched[:max_rows]
                    # psycopg3 with dict_row returns dicts; normalize to a list
                    # of lists in column order so the UI is predictable
                    rows = []
                    for r in fetched:
                        if isinstance(r, dict):
                            rows.append([_to_json_safe(r.get(c)) for c in cols])
                        else:
                            rows.append([_to_json_safe(v) for v in r])
                    try:
                        conn.rollback()  # always rollback read-only txn
                    except Exception:
                        pass
                    return {
                        "columns": cols, "rows": rows, "row_count": len(rows),
                        "truncated": truncated, "elapsed_ms": elapsed_ms,
                    }
        except Exception as exc:
            elapsed_ms = int((time.time() - t0) * 1000)
            msg = str(exc)
            # Try to classify common psycopg errors for nicer UI messages
            code = getattr(getattr(exc, "diag", None), "sqlstate", None)
            kind = "error"
            if code == "57014":
                kind = "timeout"
                msg = f"query exceeded {timeout_ms}ms — try LIMIT or add a filter"
            elif code == "42P01":
                kind = "undefined_table"
            elif code == "42703":
                kind = "undefined_column"
            elif code == "42601":
                kind = "syntax_error"
            elif code == "25006":
                kind = "read_only_violation"
                msg = "write operations are blocked (read-only transaction)"
            return {
                "columns": [], "rows": [], "row_count": 0,
                "truncated": False, "elapsed_ms": elapsed_ms,
                "error": msg, "error_kind": kind,
                "error_code": code,
            }


# Module-level singleton (lazy)
_pg_store: Optional[PgStore] = None


def get_pg_store() -> Optional[PgStore]:
    """Return the singleton PgStore if DATABASE_URL is set, else None."""
    global _pg_store
    if _pg_store is not None:
        return _pg_store
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return None
    # Render gives us postgresql://, but psycopg-3 prefers that prefix as-is.
    _pg_store = PgStore(dsn)
    logger.info("PgStore initialized")
    return _pg_store
