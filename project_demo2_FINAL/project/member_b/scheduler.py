"""Pulls fair per-client jobs from Member A and assigns them to workers."""
import threading
import time

from worker_manager import log


class Scheduler(threading.Thread):
    def __init__(self, manager, get_next_job):
        super().__init__(daemon=True)
        self.manager = manager
        self.get_next_job = get_next_job

    def run(self):
        log("scheduler started (round-robin client fairness)")
        while True:
            job = self.get_next_job(timeout=1)
            if job is None:
                continue
            warned = False
            while not self.manager.assign(job):
                if not warned:
                    log("no ALIVE worker for %s, holding it" % job["job_id"])
                    warned = True
                time.sleep(0.5)
