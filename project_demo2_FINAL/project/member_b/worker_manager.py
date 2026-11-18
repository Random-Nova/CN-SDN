"""Server-side worker registry, load balancer, heartbeat monitor and failover."""
import json
import socket
import threading
import time

WORKER_PORT = 6000
HEARTBEAT_TIMEOUT = 6
MONITOR_INTERVAL = 1
STATUS_EVERY = 5

_print_lock = threading.Lock()


def log(msg):
    with _print_lock:
        print("{} [MANAGER] {}".format(time.strftime("%H:%M:%S"), msg), flush=True)


class Worker:
    def __init__(self, worker_id, sock):
        self.id = worker_id
        self.sock = sock
        self.send_lock = threading.Lock()
        self.last_heartbeat = time.time()
        self.load = 0
        self.status = "ALIVE"
        self.completed_jobs = 0
        self.failed_jobs = 0

    def send(self, msg):
        data = (json.dumps(msg, default=str) + "\n").encode("utf-8")
        with self.send_lock:
            try:
                self.sock.sendall(data)
                return True
            except OSError:
                return False

    def close(self):
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class WorkerManager:
    def __init__(self, report_result, requeue_job, policy="least", on_assignment=None):
        self.report_result = report_result
        self.requeue_job = requeue_job
        self.policy = policy
        self.on_assignment = on_assignment
        self.lock = threading.RLock()
        self.workers = {}
        self.in_progress = {}       # job_id -> (Worker, job)
        self._rr = 0
        self.total_assignments = 0
        self.total_worker_failures = 0

    def start(self):
        threading.Thread(target=self._accept_loop, daemon=True).start()
        threading.Thread(target=self._monitor_loop, daemon=True).start()

    # ---------- connections ----------
    def _accept_loop(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", WORKER_PORT))
        srv.listen(20)
        log("listening for workers on port %d (policy=%s)" % (WORKER_PORT, self.policy))
        while True:
            sock, addr = srv.accept()
            threading.Thread(target=self._handle_worker, args=(sock, addr), daemon=True).start()

    def _handle_worker(self, sock, addr):
        reader = sock.makefile("r", encoding="utf-8")
        worker = None
        try:
            for line in reader:
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    continue
                kind = msg.get("type")
                if worker is None:
                    if kind == "REGISTER" and msg.get("worker_id"):
                        worker = Worker(msg["worker_id"], sock)
                        self._register(worker, addr)
                    continue
                worker.last_heartbeat = time.time()
                if kind == "HEARTBEAT":
                    worker.load = max(0, int(msg.get("load", 0) or 0))
                elif kind == "DONE":
                    self._complete(worker, msg.get("job_id"), msg.get("result"))
        except (OSError, ValueError):
            pass
        finally:
            if worker is not None:
                self._mark_dead(worker, "connection closed")
            else:
                sock.close()
            try:
                reader.close()
            except OSError:
                pass

    def _register(self, worker, addr):
        with self.lock:
            old = self.workers.get(worker.id)
            self.workers[worker.id] = worker
        if old is not None and old.status == "ALIVE":
            self._mark_dead(old, "replaced by a new connection")
        log("REGISTER %s from %s:%s" % (worker.id, addr[0], addr[1]))

    # ---------- results and failures ----------
    def _complete(self, worker, job_id, result):
        with self.lock:
            entry = self.in_progress.get(job_id)
            current = entry is not None and entry[0] is worker
            if current:
                del self.in_progress[job_id]
                if str(result).startswith("ERROR:"):
                    worker.failed_jobs += 1
                else:
                    worker.completed_jobs += 1
        if not current:
            log("ignored late/duplicate DONE for %s from %s" % (job_id, worker.id))
            return
        log("DONE %s from %s" % (job_id, worker.id))
        try:
            self.report_result(job_id, result, worker.id)
        except TypeError:
            # Compatibility with an older two-argument result callback.
            self.report_result(job_id, result)

    def _mark_dead(self, worker, reason):
        with self.lock:
            if worker.status == "DEAD":
                return
            worker.status = "DEAD"
            self.total_worker_failures += 1
            lost = [(jid, job) for jid, (w, job) in self.in_progress.items() if w is worker]
            for jid, _ in lost:
                del self.in_progress[jid]
        worker.close()
        if lost:
            log("WORKER FAILURE | failed_worker={} | reason={} | affected_jobs={}".format(
                worker.id, reason, len(lost)))
            header = "  {:<12} | {:<18} | {:<16} | {:<18}".format(
                "SERVER JOB", "CLIENT", "CLIENT JOB", "NEXT ACTION")
            log(header)
            log("  " + "-" * 76)
            for jid, job in lost:
                job.setdefault("failed_workers", []).append(worker.id)
                log("  {:<12} | {:<18} | {:<16} | {:<18}".format(
                    jid, job.get("client_id", "unknown"),
                    job.get("client_job_id", "—"), "REQUEUE"))
        else:
            log("WORKER FAILURE | failed_worker={} | reason={} | no in-flight jobs".format(
                worker.id, reason))
        for _, job in lost:
            self.requeue_job(job)

    # ---------- scheduling support ----------
    def _pick_locked(self):
        alive = sorted((w for w in self.workers.values() if w.status == "ALIVE"),
                       key=lambda w: w.id)
        if not alive:
            return None
        if self.policy == "rr":
            worker = alive[self._rr % len(alive)]
            self._rr += 1
            return worker
        in_flight = {}
        for current_worker, _ in self.in_progress.values():
            in_flight[current_worker] = in_flight.get(current_worker, 0) + 1
        return min(alive, key=lambda w: (in_flight.get(w, 0), w.load, w.id))

    def assign(self, job):
        """Send a job to a worker. Returns False only when it still needs a home."""
        with self.lock:
            worker = self._pick_locked()
            if worker is None:
                return False
            self.in_progress[job["job_id"]] = (worker, job)
            if self.on_assignment:
                self.on_assignment(job, worker.id)
            self.total_assignments += 1
        if worker.send(job):
            log("JOB %s -> %s (client=%s)" %
                (job["job_id"], worker.id, job.get("client_id", "unknown")))
            return True
        with self.lock:
            mine = self.in_progress.pop(job["job_id"], None)
        self._mark_dead(worker, "send failed")
        return mine is None

    # ---------- dashboard support ----------
    def get_status_snapshot(self):
        with self.lock:
            counts = {}
            for worker, _ in self.in_progress.values():
                counts[worker] = counts.get(worker, 0) + 1
            rows = [{
                "worker_id": worker.id,
                "status": worker.status,
                "in_flight": counts.get(worker, 0),
                "reported_load": worker.load,
                "completed_jobs": worker.completed_jobs,
                "failed_jobs": worker.failed_jobs,
                "heartbeat_age_seconds": round(time.time() - worker.last_heartbeat, 1),
            } for worker in sorted(self.workers.values(), key=lambda item: item.id)]
            alive = sum(1 for row in rows if row["status"] == "ALIVE")
            return {
                "workers": rows,
                "total": len(rows),
                "alive": alive,
                "dead": len(rows) - alive,
                "jobs_in_progress": len(self.in_progress),
                "total_assignments": self.total_assignments,
                "total_worker_failures": self.total_worker_failures,
            }

    def _log_status(self):
        snapshot = self.get_status_snapshot()
        if not snapshot["workers"]:
            return
        parts = ["%s %s in_flight=%d reported_load=%d completed=%d" % (
            worker["worker_id"], worker["status"], worker["in_flight"],
            worker["reported_load"], worker["completed_jobs"])
            for worker in snapshot["workers"]]
        log("STATUS | " + " | ".join(parts))

    # ---------- monitor ----------
    def _monitor_loop(self):
        last_status = 0
        while True:
            time.sleep(MONITOR_INTERVAL)
            now = time.time()
            with self.lock:
                snapshot = list(self.workers.values())
            for worker in snapshot:
                if worker.status == "ALIVE" and now - worker.last_heartbeat > HEARTBEAT_TIMEOUT:
                    self._mark_dead(worker, "no heartbeat for %.0fs" % (now - worker.last_heartbeat))
            if now - last_status >= STATUS_EVERY:
                last_status = now
                self._log_status()
