import os
import tempfile
import threading
import unittest

from sheets_queue import SheetsQueueStore, calculate_backoff_sec


class SheetsQueueTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(prefix="sheetq_", suffix=".sqlite3")
        os.close(fd)
        self.store = SheetsQueueStore(self.db_path)

    def tearDown(self):
        try:
            os.remove(self.db_path)
        except OSError:
            pass

    def test_enqueue_fetch_mark_done(self):
        event_id = self.store.enqueue("today_upsert", {"peer_id": 1, "name": "A"})
        batch = self.store.fetch_batch(limit=10)
        self.assertEqual(len(batch), 1)
        self.assertEqual(batch[0].id, event_id)
        self.assertEqual(batch[0].event_type, "today_upsert")
        self.store.mark_done(event_id)
        self.assertEqual(self.store.fetch_batch(limit=10), [])

    def test_mark_retry(self):
        event_id = self.store.enqueue("today_upsert", {"peer_id": 2})
        first = self.store.fetch_batch(limit=10)[0]
        self.store.mark_retry(event_id, attempts=first.attempts + 1, backoff_sec=10, error="429")
        immediate = self.store.fetch_batch(limit=10)
        self.assertEqual(len(immediate), 0)
        after = self.store.fetch_batch(limit=10, now_ts=first.created_at + 20)
        self.assertEqual(len(after), 1)
        self.assertEqual(after[0].attempts, 1)
        self.assertIn("429", after[0].last_error)

    def test_stats(self):
        self.store.enqueue("today_upsert", {"peer_id": 1})
        stats = self.store.stats()
        self.assertEqual(stats["pending"], 1)
        self.assertIsNotNone(stats["oldest_age_sec"])

    def test_backoff_bounds(self):
        for attempts in (1, 2, 3, 5, 8, 20):
            delay = calculate_backoff_sec(attempts, hard_error=False)
            self.assertGreaterEqual(delay, 1.0)
            self.assertLessEqual(delay, 300.0)
        hard = calculate_backoff_sec(1, hard_error=True)
        self.assertGreaterEqual(hard, 300.0 - 1.0)
        self.assertLessEqual(hard, 300.0)

    def test_concurrent_enqueue(self):
        total_threads = 5
        per_thread = 50
        errors = []

        def worker(thread_idx: int):
            try:
                for i in range(per_thread):
                    self.store.enqueue("today_upsert", {"peer_id": thread_idx * 1000 + i})
            except Exception as err:
                errors.append(str(err))

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(total_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(errors, [])
        stats = self.store.stats()
        self.assertEqual(stats["pending"], total_threads * per_thread)


if __name__ == "__main__":
    unittest.main()
