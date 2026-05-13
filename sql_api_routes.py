"""
SQL exploration API for the admin SQL console (templates/sql.html).

All endpoints require admin authentication. Queries run inside a READ ONLY
transaction with a statement timeout — defense-in-depth on top of the regex
keyword filter, so a syntax-clever query still cannot mutate state.
"""

import logging
import re

from flask import Blueprint, jsonify, request

from auth import admin_required

logger = logging.getLogger(__name__)

sql_api_bp = Blueprint("sql_api", __name__, url_prefix="/api/sql")


def _require_pg():
    try:
        from pg_store import get_pg_store
        pg = get_pg_store()
    except Exception:
        pg = None
    if pg is None:
        return None, (jsonify({"error": "Postgres backend required"}), 501)
    return pg, None


# Strip leading SQL comments + whitespace so the first-keyword check works
# even when the user pastes a query with leading -- comments or /* */ blocks.
_LEADING_COMMENT_RE = re.compile(
    r"^\s*(?:--[^\n]*\n|/\*.*?\*/|\s)*",
    re.DOTALL,
)
_ALLOWED_FIRST_KW = re.compile(r"^(select|explain|show|with)\b", re.IGNORECASE)
# Reject anything that looks like statement chaining: a `;` followed by a
# non-empty token. Trailing single `;` is fine.
_CHAINING_RE = re.compile(r";\s*\S")


@sql_api_bp.route("/tables", methods=["GET"])
@admin_required
def list_tables():
    pg, err = _require_pg()
    if err: return err
    return jsonify({"tables": pg.list_tables()})


@sql_api_bp.route("/schema/<table>", methods=["GET"])
@admin_required
def describe_table(table):
    pg, err = _require_pg()
    if err: return err
    try:
        cols = pg.describe_table(table)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify({"table": table, "columns": cols})


@sql_api_bp.route("/query", methods=["POST"])
@admin_required
def run_query():
    body = request.get_json(silent=True) or {}
    sql = (body.get("sql") or "").strip()
    if not sql:
        return jsonify({"error": "missing sql"}), 400
    if len(sql) > 50_000:
        return jsonify({"error": "query too long (max 50k chars)"}), 400

    # Strip leading comments and whitespace for the keyword check.
    stripped = _LEADING_COMMENT_RE.sub("", sql).strip()
    if not _ALLOWED_FIRST_KW.match(stripped):
        return jsonify({
            "error": "only SELECT/EXPLAIN/SHOW/WITH queries are allowed",
        }), 400
    if _CHAINING_RE.search(sql):
        return jsonify({
            "error": "statement chaining (multiple ;-separated statements) is not allowed",
        }), 400

    pg, err = _require_pg()
    if err: return err

    timeout_ms = int(body.get("timeout_ms") or 30000)
    max_rows = int(body.get("max_rows") or 1000)
    timeout_ms = max(1000, min(timeout_ms, 60000))
    max_rows = max(1, min(max_rows, 5000))

    result = pg.run_readonly_query(sql, timeout_ms=timeout_ms, max_rows=max_rows)
    return jsonify(result)
