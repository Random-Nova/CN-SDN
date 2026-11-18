"""Client-facing job server with per-client fair scheduling and dashboard data."""
import json
import socket
import sys
import threading
import time
from collections import defaultdict, deque

HOST = "0.0.0.0"
CLIENT_PORT = 5000


# ---------- task labels shown in the dashboard ----------
TASK_TITLES = {
    "sum": "Sum of numbers",
    "reverse": "Reverse a string",
    "sleep": "Simulated long-running job",
}


def task_title(task):
    return TASK_TITLES.get(task, "Custom task: %s" % (task or "Unknown"))


# ---------- fair, per-client job queue ----------
class FairClientQueue:
    """Round-robin queue: each active client gets one job per scheduling turn."""

    def __init__(self):
        self._condition = threading.Condition()
        self._queues = defaultdict(deque)  # client_id -> jobs
        self._active_clients = deque()
        self._active_set = set()
        self._pending = 0

    def put(self, job, front=False):
        client_id = str(job.get("client_id") or "unknown-client")
        with self._condition:
            q = self._queues[client_id]
            if front:
                q.appendleft(job)
            else:
                q.append(job)
            if client_id not in self._active_set:
                if front:
                    self._active_clients.appendleft(client_id)
                else:
                    self._active_clients.append(client_id)
                self._active_set.add(client_id)
            self._pending += 1
            self._condition.notify()

    def get(self, timeout=None):
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._active_clients:
                if deadline is None:
                    self._condition.wait()
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    self._condition.wait(remaining)

            client_id = self._active_clients.popleft()
            self._active_set.discard(client_id)
            q = self._queues[client_id]
            job = q.popleft()
            self._pending -= 1

            if q:
                self._active_clients.append(client_id)
                self._active_set.add(client_id)
            else:
                del self._queues[client_id]
            return job

    def pending_count(self):
        with self._condition:
            return self._pending


job_queue = FairClientQueue()
job_owner = {}                 # server_job_id -> ClientConn
owner_lock = threading.Lock()
_counter = 0
_counter_lock = threading.Lock()

# Dashboard / reporting state.
stats_lock = threading.RLock()
job_records = {}               # server_job_id -> complete display record
active_client_connections = defaultdict(int)
known_clients = set()
total_client_connections = 0
server_started_at = time.time()


def log(msg):
    print("{}  SERVER  {}".format(time.strftime("%H:%M:%S"), msg), flush=True)


def next_job_id():
    global _counter
    with _counter_lock:
        _counter += 1
        return "j%d" % _counter


class ClientConn:
    """One client socket; the lock prevents ACK/RESULT writes interleaving."""

    def __init__(self, sock):
        self.sock = sock
        self.send_lock = threading.Lock()
        self.client_id = None
        self.counted = False

    def send(self, msg):
        data = (json.dumps(msg, default=str) + "\n").encode("utf-8")
        with self.send_lock:
            try:
                self.sock.sendall(data)
                return True
            except OSError:
                return False


# ---------- functions called by Member B ----------
def get_next_job(timeout=None):
    """Return the next job using fair round-robin turns between clients."""
    return job_queue.get(timeout=timeout)


def requeue_job(job):
    """Put a failed worker's job back at the front of that client's queue."""
    job["status"] = "QUEUED"
    with stats_lock:
        record = job_records.get(job["job_id"])
        if record:
            record["status"] = "QUEUED"
            record["worker_id"] = "—"
            record["started_at"] = None
            record["attempts"] = record.get("attempts", 1) + 1
            record["failed_workers"] = list(job.get("failed_workers", []))
    # Update the visible state before the scheduler can dequeue the retry.
    job_queue.put(job, front=True)
    log("RETRY QUEUED | server_job={:<8} | client={:<14} | client_job={:<14} | attempt={}".format(
        job["job_id"], job.get("client_id", "unknown"), job.get("client_job_id", "—"),
        job.get("attempts", 2)))


def record_assignment(job, worker_id):
    """Record which worker received a job (called before it is sent)."""
    now = time.time()
    with stats_lock:
        record = job_records.get(job["job_id"])
        if record:
            record["worker_id"] = worker_id
            record["status"] = "RUNNING"
            if record.get("started_at") is None:
                record["started_at"] = now
            record["attempts"] = max(1, record.get("attempts", 1))
    log("DISPATCHED | server_job={:<8} | client={:<14} | client_job={:<14} | worker={:<10} | title={}".format(
        job["job_id"], job.get("client_id", "unknown"), job.get("client_job_id", "—"),
        worker_id, task_title(job.get("task"))))


def report_result(job_id, result, worker_id=None):
    """Store the completed job and send its result to the original client."""
    completed_at = time.time()
    with owner_lock:
        conn = job_owner.pop(job_id, None)

    with stats_lock:
        record = job_records.get(job_id)
        if record:
            record["status"] = "FAILED" if str(result).startswith("ERROR:") else "COMPLETED"
            record["completed_at"] = completed_at
            if worker_id:
                record["worker_id"] = worker_id
            record["result"] = str(result)
            if record.get("started_at"):
                record["duration_seconds"] = round(completed_at - record["started_at"], 3)
            else:
                record["duration_seconds"] = None

    if conn is None:
        log("no owner for %s (client gone or duplicate result)" % job_id)
        return False

    with stats_lock:
        record = job_records.get(job_id, {}).copy()
    message = {
        "type": "RESULT",
        "job_id": job_id,
        "client_job_id": record.get("client_job_id"),
        "client_id": record.get("client_id"),
        "task": record.get("task"),
        "job_title": record.get("job_title"),
        "worker_id": record.get("worker_id") or worker_id,
        "status": record.get("status", "COMPLETED"),
        "result": result,
        "duration_seconds": record.get("duration_seconds"),
        "attempts": record.get("attempts", 1),
        "failed_workers": record.get("failed_workers", []),
    }
    ok = conn.send(message)
    log("RESULT SENT | server_job={:<8} | client={:<14} | client_job={:<14} | worker={:<10} | status={}".format(
        job_id, message.get("client_id") or "unknown", message.get("client_job_id") or "—",
        message.get("worker_id") or "unknown", message.get("status", "—"))
        if ok else "RESULT DELIVERY FAILED | server_job={} | client={}".format(
            job_id, message.get("client_id") or "unknown"))
    return ok


# ---------- per-client thread ----------
def handle_client(sock, addr):
    conn = ClientConn(sock)
    reader = sock.makefile("r", encoding="utf-8")
    log("client connected %s:%s" % addr)
    try:
        for line in reader:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                conn.send({"type": "ERROR", "reason": "bad JSON"})
                continue

            if msg.get("type") != "SUBMIT":
                conn.send({"type": "ERROR", "reason": "unknown type"})
                continue

            client_id = str(msg.get("client_id") or ("%s:%s" % addr))
            if not conn.counted:
                conn.client_id = client_id
                conn.counted = True
                with stats_lock:
                    active_client_connections[client_id] += 1
                    known_clients.add(client_id)

            client_job_id = str(msg.get("job_id") or "client-job")
            task = msg.get("task")
            job_id = next_job_id()
            submitted_at = time.time()
            title = task_title(task)
            job = {
                "type": "JOB",
                "job_id": job_id,
                "client_id": client_id,
                "client_job_id": client_job_id,
                "task": task,
                "task_title": title,
                "data": msg.get("data"),
                "submitted_at": submitted_at,
            }
            with owner_lock:
                job_owner[job_id] = conn
            with stats_lock:
                job_records[job_id] = {
                    "job_id": job_id,
                    "client_id": client_id,
                    "client_job_id": client_job_id,
                    "job_title": title,
                    "task": task or "Unknown",
                    "worker_id": "—",
                    "status": "QUEUED",
                    "submitted_at": submitted_at,
                    "started_at": None,
                    "completed_at": None,
                    "duration_seconds": None,
                    "result": "—",
                    "attempts": 1,
                    "failed_workers": [],
                }

            # ACK is sent before queueing to ensure it cannot be overtaken by RESULT.
            conn.send({
                "type": "ACK",
                "job_id": job_id,
                "client_job_id": client_job_id,
                "client_id": client_id,
                "job_title": title,
                "status": "QUEUED",
            })
            job_queue.put(job)
            log("JOB ACCEPTED | server_job={:<8} | client={:<14} | client_job={:<14} | title={}".format(
                job_id, client_id, client_job_id, title))
    except OSError:
        pass
    finally:
        if conn.counted:
            with stats_lock:
                active_client_connections[conn.client_id] -= 1
                if active_client_connections[conn.client_id] <= 0:
                    del active_client_connections[conn.client_id]
        reader.close()
        sock.close()
        log("client disconnected %s:%s" % addr)


# ---------- dashboard data API ----------
def get_dashboard_snapshot(worker_snapshot=None):
    with stats_lock:
        records = [record.copy() for record in job_records.values()]
        active_clients = dict(active_client_connections)
        known = sorted(known_clients)
        total_connections = total_client_connections

    queued = sum(1 for job in records if job["status"] == "QUEUED")
    running = sum(1 for job in records if job["status"] == "RUNNING")
    completed = sum(1 for job in records if job["status"] == "COMPLETED")
    durations = [job["duration_seconds"] for job in records
                 if job.get("duration_seconds") is not None]
    now = time.time()

    client_map = {}
    for client_id in known:
        client_map[client_id] = {
            "client_id": client_id,
            "connections": active_clients.get(client_id, 0),
            "queued": 0,
            "running": 0,
            "completed": 0,
            "total": 0,
        }
    for job in records:
        cid = job["client_id"]
        row = client_map.setdefault(cid, {
            "client_id": cid, "connections": active_clients.get(cid, 0),
            "queued": 0, "running": 0, "completed": 0, "total": 0,
        })
        row["total"] += 1
        key = job["status"].lower()
        if key in ("queued", "running", "completed", "failed"):
            row[key] += 1

    records.sort(key=lambda item: item.get("submitted_at") or 0, reverse=True)
    return {
        "server": {
            "status": "ONLINE",
            "host": HOST,
            "client_port": CLIENT_PORT,
            "started_at": server_started_at,
            "uptime_seconds": round(now - server_started_at, 1),
            "total_client_connections": total_connections,
            "active_clients": len(active_clients),
            "known_clients": len(known),
        },
        "stats": {
            "submitted": len(records),
            "queued": queued,
            "running": running,
            "completed": completed,
            "failed": sum(1 for job in records if job["status"] == "FAILED"),
            "average_duration_seconds": round(sum(durations) / len(durations), 3) if durations else 0,
            "pending_in_queue": job_queue.pending_count(),
        },
        "clients": sorted(client_map.values(), key=lambda item: item["client_id"]),
        "jobs": records[:250],
        "workers": worker_snapshot or {"workers": [], "total": 0, "alive": 0, "dead": 0},
        "updated_at": now,
        "scheduling": {
            "client_policy": "Round-robin: one job per active client turn",
            "worker_policy": "Least-loaded active worker",
        },
    }


# ---------- fake worker, for testing without Member B ----------
def fake_worker():
    while True:
        job = get_next_job(timeout=1)
        if job is None:
            continue
        record_assignment(job, "fake-worker")
        time.sleep(2)
        report_result(job["job_id"], "dummy-result-for-" + job["job_id"], "fake-worker")


def main():
    global total_client_connections
    if "--fake-worker" in sys.argv:
        threading.Thread(target=fake_worker, daemon=True).start()
        log("fake worker started")

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, CLIENT_PORT))
    srv.listen(50)
    log("listening on port %d" % CLIENT_PORT)

    while True:
        sock, addr = srv.accept()
        with stats_lock:
            total_client_connections += 1
        threading.Thread(target=handle_client, args=(sock, addr), daemon=True).start()


if __name__ == "__main__":
    main()
