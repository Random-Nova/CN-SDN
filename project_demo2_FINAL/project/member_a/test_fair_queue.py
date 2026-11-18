"""Small check for Demo 2's round-robin client fairness."""
import unittest

from server import FairClientQueue


class FairClientQueueTests(unittest.TestCase):
    def test_clients_take_turns_even_when_one_submits_more_jobs(self):
        jobs = FairClientQueue()
        for client_id, job_id in [
            ("client_A", "A1"), ("client_A", "A2"), ("client_A", "A3"),
            ("client_B", "B1"), ("client_B", "B2"),
            ("client_C", "C1"),
        ]:
            jobs.put({"client_id": client_id, "job_id": job_id})

        actual = [jobs.get(timeout=0.1)["job_id"] for _ in range(6)]
        self.assertEqual(actual, ["A1", "B1", "C1", "A2", "B2", "A3"])
        self.assertEqual(jobs.pending_count(), 0)

    def test_empty_queue_times_out(self):
        jobs = FairClientQueue()
        self.assertIsNone(jobs.get(timeout=0.01))


if __name__ == "__main__":
    unittest.main(verbosity=2)
