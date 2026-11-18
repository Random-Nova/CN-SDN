"""Start the client server, worker manager, fair scheduler and web dashboard.

Usage:
    python3 run_server.py
    python3 run_server.py --round-robin-workers

Client scheduling is always fair round-robin across clients. Worker selection
uses least-loaded by default; --round-robin-workers changes only worker choice.
"""
import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "member_a"))
import server

from dashboard import start_dashboard
from scheduler import Scheduler
from worker_manager import WorkerManager


def main():
    worker_policy = "rr" if "--round-robin-workers" in sys.argv else "least"
    threading.Thread(target=server.main, daemon=True).start()
    manager = WorkerManager(
        server.report_result,
        server.requeue_job,
        worker_policy,
        on_assignment=server.record_assignment,
    )
    manager.start()
    Scheduler(manager, server.get_next_job).start()

    dashboard_provider = lambda: server.get_dashboard_snapshot(manager.get_status_snapshot())
    threading.Thread(target=start_dashboard, args=(dashboard_provider,), daemon=True).start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("stopped", flush=True)


if __name__ == "__main__":
    main()
