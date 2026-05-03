"""
Loop wrapper for the slice worker. Restarts the worker after each
exit (typically 5-consecutive-fails circuit breaker). Also restarts
Chrome if its CDP port stops responding.

Run: python run_slice_worker_loop.py
"""
import os
import subprocess
import sys
import time
import urllib.request


SERVER = os.environ.get("NADLAN_SERVER_URL",
                        "https://govil-scraper.onrender.com")
WORKER_ID = os.environ.get("NADLAN_WORKER_ID",
                            os.environ.get("COMPUTERNAME", "host") + "-slice")
PROFILE_DIR = os.path.join(os.environ["USERPROFILE"], "nadlan-chrome-profile")
CHROME_EXE_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
]
COOLDOWN_SHORT_S = 60
COOLDOWN_LONG_S = 300


def chrome_alive() -> bool:
    try:
        with urllib.request.urlopen("http://127.0.0.1:9222/json/version",
                                      timeout=3) as r:
            return r.status == 200
    except Exception:
        return False


def launch_chrome():
    chrome = next((c for c in CHROME_EXE_CANDIDATES if os.path.exists(c)), None)
    if not chrome:
        print("[FATAL] Chrome not found in standard install paths")
        sys.exit(1)
    print(f"[INFO] Launching Chrome: {chrome}")
    subprocess.Popen(
        [chrome,
         "--remote-debugging-port=9222",
         f"--user-data-dir={PROFILE_DIR}",
         "https://www.nadlan.gov.il/"],
        creationflags=subprocess.DETACHED_PROCESS if os.name == "nt" else 0,
    )
    # Wait for CDP to come up
    for _ in range(20):
        time.sleep(1)
        if chrome_alive():
            print("[INFO] Chrome CDP ready")
            return True
    print("[WARN] Chrome CDP did not become ready within 20s")
    return False


def run_worker_once():
    cmd = [
        sys.executable, "-u", "nadlan_slice_worker.py",
        "--server", SERVER,
        "--worker-id", WORKER_ID,
        "--slices-per-claim", "14",
        "--per-settlement-pause", "3",
        "--max-failed", "5",
    ]
    print(f"[INFO] Running worker: {' '.join(cmd)}")
    return subprocess.call(cmd)


def main():
    iteration = 0
    fast_exits = 0
    while True:
        iteration += 1
        print(f"\n{'='*60}\nIteration {iteration} ({time.strftime('%H:%M:%S')})\n{'='*60}")

        if not chrome_alive():
            print("[INFO] Chrome not responding — relaunching")
            launch_chrome()

        t0 = time.time()
        rc = run_worker_once()
        elapsed = time.time() - t0
        print(f"\n[INFO] Worker exited rc={rc} after {elapsed:.0f}s")

        if elapsed < 60:
            fast_exits += 1
            print(f"[WARN] Fast exit #{fast_exits}")
            if fast_exits >= 10:
                print("[FATAL] 10 fast exits in a row — giving up")
                return
        else:
            fast_exits = 0

        # Cooldown — longer if worker exited fast
        cooldown = COOLDOWN_LONG_S if elapsed < 60 else COOLDOWN_SHORT_S
        print(f"[INFO] Sleeping {cooldown}s before restart")
        time.sleep(cooldown)


if __name__ == "__main__":
    main()
