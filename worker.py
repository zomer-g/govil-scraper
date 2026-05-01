#!/usr/bin/env python3
"""Re-export shim — canonical implementation lives in govscraper.legacy.worker_client.

`python worker.py --server X --key Y` continues to work; new code should
prefer `python -m govscraper.cli worker --source local`.
"""
from govscraper.legacy.worker_client import *  # noqa: F401,F403
from govscraper.legacy.worker_client import WorkerClient, main  # noqa: F401

if __name__ == "__main__":
    main()
