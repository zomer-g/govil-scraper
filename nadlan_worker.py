#!/usr/bin/env python3
"""Re-export shim — canonical implementation lives in govscraper.legacy.nadlan_worker.

`python nadlan_worker.py --server X --worker-id Y` continues to work; new
code should prefer `python -m govscraper.cli worker --source nadlan-queue`.
"""
from govscraper.legacy.nadlan_worker import *  # noqa: F401,F403
from govscraper.legacy.nadlan_worker import NadlanWorkerClient, main  # noqa: F401

if __name__ == "__main__":
    main()
