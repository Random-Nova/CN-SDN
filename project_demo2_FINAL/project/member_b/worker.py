"""
worker.py  (Member B)
Runs on each worker machine. Connects to the server on port 6000,
registers, sends a heartbeat every 2 seconds, executes JOBs, replies DONE.
If the connection drops it keeps retrying every 2 seconds.

Usage:  python3 worker.py <worker_id> [server_ip]
Example: python3 worker.py w1
         python3 worker.py w2 10.0.0.10
"""
import json
import socket
import sys
import threading
import time

WORKER_PORT = 6000
HEARTBEAT_EVERY = 2   # seconds
RETRY_EVERY = 2       # seconds


def log(worker_id, msg):
    print(time.strftime("%H:%M:%S"), "[%s]" % worker_id, msg, flush=True)


def run_task(task, data):
    """The actual work. Raises an exception on bad input."""
    if task == "sum":
        return sum(data)
    if task == "reverse":
        return data[::-1]
    if task == "sleep":
        time.sleep(data)          # simulates heavy work
        return "slept %s" % data
    raise ValueError("unknown task %r" % task)


def serve_once(worker_id, host):
    """One connection's lifetime. Returns or raises OSError when it ends."""
    sock = socket.create_connection((host, WORKER_PORT), timeout=5)
    sock.settimeout(None)
    send_lock = threading.Lock()   # heartbeat thread and job threads share the socket
    load_lock = threading.Lock()
    load = [0]                     # jobs running right now
    stop = threading.Event()

    def send(msg):
        data = (json.dumps(msg) + "\n").encode("utf-8")
        with send_lock:
            sock.sendall(data)

    def heartbeat():
        while not stop.is_set():
            try:
                with load_lock:
                    current = load[0]
                send({"type": "HEARTBEAT", "worker_id": worker_id, "load": current})
            except OSError:
                break
            stop.wait(HEARTBEAT_EVERY)
        try:                       # wake the reader so it notices the dead connection
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def execute(job):
        with load_lock:
            load[0] += 1
        try:
            log(worker_id, "start %s (%s)" % (job["job_id"], job.get("task")))
            try:
                result = run_task(job.get("task"), job.get("data"))
            except Exception as e:          # bad input must not kill the worker
                result = "ERROR: %s" % e
            try:
                send({"type": "DONE", "job_id": job["job_id"], "result": result})
                log(worker_id, "done  %s" % job["job_id"])
            except OSError:
                log(worker_id, "connection lost, could not report %s" % job["job_id"])
        finally:
            with load_lock:
                load[0] -= 1

    send({"type": "REGISTER", "worker_id": worker_id})
    log(worker_id, "registered with %s:%d" % (host, WORKER_PORT))
    reader = sock.makefile("r", encoding="utf-8")
    threading.Thread(target=heartbeat, daemon=True).start()
    try:
        for line in reader:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue
            if msg.get("type") == "JOB":
                threading.Thread(target=execute, args=(msg,), daemon=True).start()
    finally:
        stop.set()
        reader.close()
        sock.close()


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    worker_id = sys.argv[1]
    host = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1"
    try:
        while True:
            try:
                serve_once(worker_id, host)
                log(worker_id, "server closed the connection, retrying")
            except OSError as e:
                log(worker_id, "cannot reach server (%s), retrying" % e)
            time.sleep(RETRY_EVERY)
    except KeyboardInterrupt:
        log(worker_id, "stopped")


if __name__ == "__main__":
    main()
