"""Submit jobs to the server and display ACKs/results in readable tables.

Usage: python3 client.py <client_id> <task> [num_jobs] [server_ip]
Tasks: sum, reverse, sleep
"""
import json
import socket
import sys
import time

SERVER_PORT = 5000


def log(client_id, message):
    print("{} [CLIENT {:<12}] {}".format(time.strftime("%H:%M:%S"), client_id, message), flush=True)


def print_table(title, headers, rows):
    rows = [[str(value if value is not None else "—") for value in row] for row in rows]
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for i, value in enumerate(row):
            widths[i] = max(widths[i], len(value))
    border = "+-" + "-+-".join("-" * w for w in widths) + "-+"
    fmt = "| " + " | ".join("{{:<{}}}".format(w) for w in widths) + " |"
    print("\n{}".format(title), flush=True)
    print(border, flush=True)
    print(fmt.format(*headers), flush=True)
    print(border, flush=True)
    for row in rows:
        print(fmt.format(*row), flush=True)
    print(border, flush=True)


def make_data(task, n):
    if task == "sum":
        return list(range(1, n + 3))
    if task == "reverse":
        return "hello-job-%d" % n
    if task == "sleep":
        return 2
    return None


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)

    client_id = sys.argv[1]
    task = sys.argv[2]
    num_jobs = int(sys.argv[3]) if len(sys.argv) > 3 else 1
    server_ip = sys.argv[4] if len(sys.argv) > 4 else "127.0.0.1"

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((server_ip, SERVER_PORT))
    reader = sock.makefile("r", encoding="utf-8")
    log(client_id, "CONNECTED to {}:{}".format(server_ip, SERVER_PORT))

    pending = {}
    accepted_rows = []
    for i in range(num_jobs):
        client_job_id = "{}-{}".format(client_id, i + 1)
        msg = {"type": "SUBMIT", "client_id": client_id, "job_id": client_job_id,
               "task": task, "data": make_data(task, i)}
        sock.sendall((json.dumps(msg) + "\n").encode("utf-8"))
        line = reader.readline()
        if not line:
            log(client_id, "ERROR: server closed the connection before ACK")
            sock.close()
            return
        ack = json.loads(line)
        if ack.get("type") == "ACK":
            pending[ack["job_id"]] = client_job_id
            accepted_rows.append([client_job_id, ack.get("job_id", "—"),
                                  ack.get("job_title", task), ack.get("status", "QUEUED")])
        else:
            accepted_rows.append([client_job_id, "—", ack.get("reason", "Unexpected reply"), "REJECTED"])

    print_table("1. JOBS SUBMITTED TO SERVER", ["CLIENT JOB", "SERVER JOB", "JOB TITLE", "SERVER STATUS"], accepted_rows)
    log(client_id, "SUBMISSION COMPLETE: {}/{} accepted".format(len(pending), num_jobs))

    results = []
    while pending:
        line = reader.readline()
        if not line:
            log(client_id, "CONNECTION LOST: {} result(s) still pending".format(len(pending)))
            break
        msg = json.loads(line)
        if msg.get("type") == "RESULT":
            pending.pop(msg.get("job_id"), None)
            failed_workers = msg.get("failed_workers") or []
            results.append([len(results) + 1, msg.get("client_job_id", "—"),
                            msg.get("job_id", "—"), msg.get("worker_id", "—"),
                            ", ".join(failed_workers) if failed_workers else "—",
                            msg.get("attempts", 1), msg.get("status", "COMPLETED"),
                            msg.get("result", "—")])

    print_table("2. RESULTS RECEIVED FROM SERVER (completion order)",
                ["NO.", "CLIENT JOB", "SERVER JOB", "FINAL WORKER", "FAILED WORKER(S)",
                 "ATTEMPTS", "STATUS", "RESULT"], results)
    print("\nSUMMARY: {} received | {} pending".format(len(results), len(pending)), flush=True)
    sock.close()
    log(client_id, "DISCONNECTED")


if __name__ == "__main__":
    main()
