"""Multi-client failover test with readable submission, recovery and result tables.

Usage: python3 test_failover.py [total_jobs] [seconds_per_job] [server_ip]

Run with at least three workers. While jobs are running, stop one worker process
(Ctrl+C in that worker's terminal). The manager will identify the failed worker
and list every affected client/job before re-queuing those jobs.
"""
import json
import socket
import sys
import threading
import time

SERVER_PORT = 5000
CLIENT_IDS = ["client1", "client2", "client3"]


def print_table(headers, rows):
    rows = [[str(value if value is not None else "—") for value in row]
            for row in rows]
    widths = [len(str(header)) for header in headers]
    for row in rows:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))
    border = "+-" + "-+-".join("-" * width for width in widths) + "-+"
    fmt = "| " + " | ".join("{{:<{}}}".format(width) for width in widths) + " |"
    print(border)
    print(fmt.format(*headers))
    print(border)
    for row in rows:
        print(fmt.format(*row))
    print(border)


def main():
    total_jobs = int(sys.argv[1]) if len(sys.argv) > 1 else 12
    seconds = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    host = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1"
    if total_jobs < 1 or seconds < 1:
        raise SystemExit("total_jobs and seconds_per_job must both be positive")

    clients = {}
    for client_id in CLIENT_IDS:
        sock = socket.create_connection((host, SERVER_PORT))
        clients[client_id] = {
            "socket": sock,
            "reader": sock.makefile("r", encoding="utf-8"),
            "pending": set(),
        }

    submitted_rows = []
    started = time.time()
    print("\n" + "=" * 92)
    print("                         MULTI-CLIENT WORKER FAILOVER TEST")
    print("=" * 92)
    print("Server: {}:{}    Clients: {}    Total jobs: {}    Simulated duration: {}s".format(
        host, SERVER_PORT, ", ".join(CLIENT_IDS), total_jobs, seconds))

    # Submit jobs in a repeating client order so the fair queue has work from
    # multiple clients and can demonstrate client-level round-robin scheduling.
    for index in range(total_jobs):
        client_id = CLIENT_IDS[index % len(CLIENT_IDS)]
        client_job_id = "{}-{}".format(client_id, (index // len(CLIENT_IDS)) + 1)
        request = {
            "type": "SUBMIT",
            "client_id": client_id,
            "job_id": client_job_id,
            "task": "sleep",
            "data": seconds,
        }
        client = clients[client_id]
        client["socket"].sendall((json.dumps(request) + "\n").encode("utf-8"))
        line = client["reader"].readline()
        if not line:
            raise RuntimeError("Server closed {} connection before ACK".format(client_id))
        ack = json.loads(line)
        if ack.get("type") != "ACK":
            raise RuntimeError("Job from {} was not acknowledged: {}".format(client_id, ack))
        client["pending"].add(ack["job_id"])
        submitted_rows.append([
            client_id, client_job_id, ack["job_id"], ack.get("job_title", "Sleep job"),
            ack.get("status", "ACCEPTED")
        ])

    print("\n1. JOBS SUBMITTED BY CLIENTS / ACCEPTED BY SERVER")
    print_table(["CLIENT", "CLIENT JOB ID", "SERVER JOB ID", "JOB TITLE", "SERVER STATUS"],
                submitted_rows)
    print("Accepted: {}/{} jobs in {:.2f}s".format(
        len(submitted_rows), total_jobs, time.time() - started))
    print("\nFAILOVER INSTRUCTION: stop one worker now, while the sleep jobs are running.")
    print("The manager terminal will show the failed worker and the client jobs it affected.\n")

    results = []
    errors = []
    result_lock = threading.Lock()

    def receive_for_client(client_id, client):
        pending = client["pending"]
        reader = client["reader"]
        while pending:
            line = reader.readline()
            if not line:
                with result_lock:
                    errors.append("{} disconnected with {} result(s) outstanding".format(
                        client_id, len(pending)))
                break
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            if message.get("type") != "RESULT":
                continue
            pending.discard(message.get("job_id"))
            with result_lock:
                results.append(message)

    threads = []
    for client_id, client in clients.items():
        thread = threading.Thread(target=receive_for_client,
                                  args=(client_id, client), daemon=True)
        thread.start()
        threads.append(thread)
    for thread in threads:
        thread.join()

    for client in clients.values():
        try:
            client["reader"].close()
        finally:
            client["socket"].close()

    results.sort(key=lambda item: (item.get("client_id", ""), item.get("client_job_id", "")))
    result_rows = []
    for index, message in enumerate(results, 1):
        failed_workers = message.get("failed_workers") or []
        result_rows.append([
            index,
            message.get("client_id", "—"),
            message.get("client_job_id", "—"),
            message.get("job_id", "—"),
            ", ".join(failed_workers) if failed_workers else "—",
            message.get("worker_id", "—"),
            message.get("attempts", 1),
            message.get("status", "COMPLETED"),
            str(message.get("result", "—"))[:24],
        ])

    print("\n2. RESULTS DELIVERED BY SERVER TO EACH CLIENT")
    print_table(["#", "CLIENT", "CLIENT JOB", "SERVER JOB", "FAILED WORKER(S)",
                 "FINAL WORKER", "ATTEMPTS", "STATUS", "RESULT"], result_rows)

    recovered = [m for m in results if int(m.get("attempts", 1)) > 1]
    recovery_rows = []
    for message in recovered:
        recovery_rows.append([
            message.get("client_id", "—"),
            message.get("client_job_id", "—"),
            message.get("job_id", "—"),
            ", ".join(message.get("failed_workers") or []) or "—",
            message.get("worker_id", "—"),
            message.get("attempts", 1),
            "RECOVERED",
        ])
    print("\n3. JOBS RECOVERED AFTER WORKER FAILURE")
    if recovery_rows:
        print_table(["AFFECTED CLIENT", "CLIENT JOB", "SERVER JOB", "FAILED WORKER",
                     "REASSIGNED WORKER", "ATTEMPTS", "RECOVERY"], recovery_rows)
        affected_clients = sorted({m.get("client_id", "unknown") for m in recovered})
        print("Affected clients: {}".format(", ".join(affected_clients)))
    else:
        print("No retried jobs were reported. Stop a worker while jobs are in progress to test recovery.")

    missing = sum(len(client["pending"]) for client in clients.values())
    completed = sum(1 for message in results if message.get("status") == "COMPLETED")
    failed = len(results) - completed
    print("\n4. FINAL TEST SUMMARY")
    print_table(["TOTAL SUBMITTED", "RESULTS RECEIVED", "COMPLETED", "FAILED", "RECOVERED", "MISSING"],
                [[total_jobs, len(results), completed, failed, len(recovered), missing]])
    if errors:
        print("Connection issues: " + "; ".join(errors))
    passed = missing == 0 and len(results) == total_jobs and failed == 0
    print("OVERALL STATUS: {}".format(
        "PASS - all client jobs received successful results" if passed
        else "FAIL - one or more jobs did not complete successfully"))
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
