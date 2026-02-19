import os
import tempfile
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo

from auto_reply_flow import STEP_CLARIFY, STEP_CONTACT, STEP_ORDER
from auto_reply_state import (
    FollowupState,
    LocalPauseStore,
    StepState,
    adjust_to_followup_window,
    within_followup_window,
)


class StateTests(unittest.TestCase):
    def test_step_state_monotonic(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "step.json")
            store = StepState(path, STEP_ORDER)
            store.set(1, STEP_CLARIFY)
            store.set(1, STEP_CONTACT)
            self.assertEqual(store.get(1), STEP_CLARIFY)

    def test_pause_store(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "pause.json")
            now = datetime(2026, 2, 19, 10, 0, 0)
            store = LocalPauseStore(path, now_factory=lambda: now)
            store.set_status(10, "@User", None, None, "ACTIVE")
            self.assertEqual(store.get_status(10, "user"), "ACTIVE")
            self.assertIn(10, store.active_peer_ids())

    def test_followup_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "followup.json")
            state = FollowupState(
                path=path,
                templates=[(60, "a"), (120, "b")],
                start_hour=9,
                end_hour=18,
                test_user_id=None,
            )
            now = datetime(2026, 2, 19, 8, 30, tzinfo=ZoneInfo("Europe/Kyiv"))
            state.schedule_from_now(1, now)
            self.assertEqual(state.get(1).get("stage"), 0)
            nxt_stage, nxt_dt = state.mark_sent_and_advance(1, datetime(2026, 2, 19, 10, 0, tzinfo=ZoneInfo("Europe/Kyiv")))
            self.assertEqual(nxt_stage, 1)
            self.assertIsNotNone(nxt_dt)

    def test_followup_window(self):
        dt = datetime(2026, 2, 19, 7, 0)
        self.assertFalse(within_followup_window(dt, 9, 18))
        adjusted = adjust_to_followup_window(dt, 9, 18)
        self.assertEqual(adjusted.hour, 9)


if __name__ == "__main__":
    unittest.main()
