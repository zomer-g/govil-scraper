#!/usr/bin/env python3
"""Re-export shim — canonical implementation lives in govscraper.legacy.over_worker.

`python over_worker.py --key X` continues to work; new code should prefer
`python -m govscraper.cli worker --source over`.
"""
from govscraper.legacy.over_worker import *  # noqa: F401,F403
from govscraper.legacy.over_worker import OverWorkerClient, main  # noqa: F401

if __name__ == "__main__":
    main()
