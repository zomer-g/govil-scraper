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

    def complete_task(self, parcel_id: str, deals_count: int) -> bool:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            cur.execute(
                """UPDATE nadlan_tasks
                   SET state = 'done', deals_count = %s,
                       completed_at = NOW(), error = ''
                   WHERE parcel_id = %s""",
                (int(deals_count or 0), parcel_id),
            )
            conn.commit()
            return cur.rowcount > 0

    def fail_task(self, parcel_id: str, error: str, transient: bool) -> bool:
        with self._lock, self._conn() as conn, conn.cursor() as cur:
            if transient:
                cur.execute(
                    """UPDATE nadlan_tasks
                       SET state = 'pending', worker_id = '',
                           claimed_at = NULL, error = %s
                       WHERE parcel_id = %s""",
                    (str(error)[:500], parcel_id),
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
            prepared.append((
                code,
                str(s.get("setl_name") or s.get("SETL_NAME") or ""),
                int(s.get("population") or s.get("POPULATION") or 0 or 0),
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
