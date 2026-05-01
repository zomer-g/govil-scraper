"""Re-export shim — canonical Flask app at govscraper.web.app.

Render's entry command is `gunicorn app:app`, so this file must continue
to expose a module-level `app` that is the live Flask instance. New code
should import from `govscraper.web.app` directly.

Phase D was kept conservative: this is a code-move only. The 1325-line
canonical file at govscraper/web/app.py preserves the existing structure
(module-level routes, in-memory jobs dict, SSE, archive scheduler thread).
A blueprint split (web/routes_*.py) is deferred to a follow-up branch
where OAuth / SSE / threading interactions can be tested in isolation.
"""
from govscraper.web.app import app  # noqa: F401  Render entry: gunicorn app:app
from govscraper.web.app import (    # noqa: F401  back-compat for any direct importers
    store,
    jobs,
    jobs_lock,
    workers,
    workers_lock,
    Phase,
    TEMP_DIR,
)

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
