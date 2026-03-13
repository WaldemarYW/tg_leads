import os
import tempfile
import time
import unittest

from flow_engine import PeerRuntimeState
from v2_state import V2EnrollmentStore, V2RuntimeStore


class V2StateStoreTests(unittest.TestCase):
    def test_runtime_store_overrides_stale_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "runtime.json")
            lock_path = f"{path}.lock"
            with open(lock_path, "w", encoding="utf-8") as f:
                f.write("stale")
            old_ts = time.time() - 30
            os.utime(lock_path, (old_ts, old_ts))

            store = V2RuntimeStore(path)
            state = PeerRuntimeState(peer_id=123, flow_step="screening_wait", auto_mode="ON", paused=False)
            store.set(state)
            loaded = store.get(123)
            self.assertEqual(loaded.peer_id, 123)
            self.assertEqual(loaded.flow_step, "screening_wait")

    def test_runtime_store_persists_balance_resume_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "runtime.json")
            store = V2RuntimeStore(path)
            state = PeerRuntimeState(
                peer_id=321,
                flow_step="balance_confirm",
                resume_step_after_balance="schedule_confirm",
                resume_checkpoint_after_balance="after_schedule_confirm_question",
                balance_block_shown=True,
                balance_block_skipped=False,
            )
            store.set(state)
            loaded = store.get(321)
            self.assertEqual(loaded.resume_step_after_balance, "schedule_confirm")
            self.assertEqual(loaded.resume_checkpoint_after_balance, "after_schedule_confirm_question")
            self.assertTrue(loaded.balance_block_shown)
            self.assertFalse(loaded.balance_block_skipped)

    def test_enrollment_store_overrides_stale_lock(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "enrolled.json")
            lock_path = f"{path}.lock"
            with open(lock_path, "w", encoding="utf-8") as f:
                f.write("stale")
            old_ts = time.time() - 30
            os.utime(lock_path, (old_ts, old_ts))

            store = V2EnrollmentStore(path)
            store.add(777)
            self.assertTrue(store.has(777))


if __name__ == "__main__":
    unittest.main()
