import importlib
import os
import sys
import types
import unittest
from datetime import datetime
from zoneinfo import ZoneInfo


os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "test-hash")
os.environ.setdefault("SESSION_FILE", "/tmp/test.session")
os.environ.setdefault("SHEET_NAME", "test-sheet")
os.environ.setdefault("GOOGLE_CREDS", "/tmp/test-creds.json")

dotenv_mod = types.ModuleType("dotenv")
dotenv_mod.load_dotenv = lambda *args, **kwargs: None
sys.modules.setdefault("dotenv", dotenv_mod)

telethon_mod = types.ModuleType("telethon")
telethon_mod.TelegramClient = object
telethon_mod.events = types.SimpleNamespace(NewMessage=object)
sys.modules.setdefault("telethon", telethon_mod)

telethon_errors_mod = types.ModuleType("telethon.errors")
telethon_errors_mod.UsernameNotOccupiedError = type("UsernameNotOccupiedError", (Exception,), {})
telethon_errors_mod.PhoneNumberInvalidError = type("PhoneNumberInvalidError", (Exception,), {})
sys.modules.setdefault("telethon.errors", telethon_errors_mod)

telethon_tl_mod = types.ModuleType("telethon.tl")
telethon_tl_mod.functions = types.SimpleNamespace()
sys.modules.setdefault("telethon.tl", telethon_tl_mod)

telethon_tl_types_mod = types.ModuleType("telethon.tl.types")
telethon_tl_types_mod.User = type("User", (), {})
sys.modules.setdefault("telethon.tl.types", telethon_tl_types_mod)

gspread_mod = types.ModuleType("gspread")
gspread_mod.authorize = lambda *args, **kwargs: None
sys.modules.setdefault("gspread", gspread_mod)

gspread_exceptions_mod = types.ModuleType("gspread.exceptions")
gspread_exceptions_mod.APIError = type("APIError", (Exception,), {})
gspread_exceptions_mod.WorksheetNotFound = type("WorksheetNotFound", (Exception,), {})
sys.modules.setdefault("gspread.exceptions", gspread_exceptions_mod)

google_mod = types.ModuleType("google")
sys.modules.setdefault("google", google_mod)
google_oauth2_mod = types.ModuleType("google.oauth2")
sys.modules.setdefault("google.oauth2", google_oauth2_mod)
google_service_account_mod = types.ModuleType("google.oauth2.service_account")


class _Credentials:
    @classmethod
    def from_service_account_file(cls, *args, **kwargs):
        return cls()

    def with_scopes(self, *args, **kwargs):
        return self


google_service_account_mod.Credentials = _Credentials
sys.modules.setdefault("google.oauth2.service_account", google_service_account_mod)

auto_reply = importlib.import_module("auto_reply")
PeerRuntimeState = auto_reply.PeerRuntimeState
STEP_SCHEDULE_SHIFT_WAIT = auto_reply.STEP_SCHEDULE_SHIFT_WAIT
STEP_TEST_REVIEW = auto_reply.STEP_TEST_REVIEW
v2_wait_followup_abort_reason = auto_reply.v2_wait_followup_abort_reason


class AutoReplyV2FollowupTests(unittest.TestCase):
    def test_abort_when_runtime_paused(self):
        state = PeerRuntimeState(peer_id=1, flow_step=STEP_SCHEDULE_SHIFT_WAIT, paused=True)
        reason = v2_wait_followup_abort_reason(state, STEP_SCHEDULE_SHIFT_WAIT)
        self.assertEqual(reason, "paused_runtime")

    def test_abort_when_new_incoming_arrives_during_rewrite(self):
        state = PeerRuntimeState(peer_id=1, flow_step=STEP_SCHEDULE_SHIFT_WAIT, paused=False)
        reason = v2_wait_followup_abort_reason(
            state,
            STEP_SCHEDULE_SHIFT_WAIT,
            latest_incoming_ts=200.0,
            rewrite_started_at=100.0,
        )
        self.assertEqual(reason, "new_incoming")

    def test_abort_when_step_changed(self):
        state = PeerRuntimeState(peer_id=1, flow_step=STEP_TEST_REVIEW, paused=False)
        reason = v2_wait_followup_abort_reason(state, STEP_SCHEDULE_SHIFT_WAIT)
        self.assertEqual(reason, "step_changed")

    def test_no_abort_when_state_is_still_valid(self):
        state = PeerRuntimeState(peer_id=1, flow_step=STEP_SCHEDULE_SHIFT_WAIT, paused=False)
        reason = v2_wait_followup_abort_reason(
            state,
            STEP_SCHEDULE_SHIFT_WAIT,
            latest_incoming_ts=50.0,
            rewrite_started_at=100.0,
            paused_in_sheet=False,
        )
        self.assertIsNone(reason)

    def test_default_step_first_followup_after_two_hours(self):
        tz = ZoneInfo("Europe/Kyiv")
        started_at = datetime(2026, 2, 19, 10, 0, tzinfo=tz).timestamp()
        plan = auto_reply.resolve_v2_followup_stage(
            auto_reply.STEP_SCHEDULE_SHIFT_WAIT,
            started_at,
            0,
            datetime(2026, 2, 19, 11, 59, tzinfo=tz),
            tz,
        )
        self.assertIsNone(plan)
        plan = auto_reply.resolve_v2_followup_stage(
            auto_reply.STEP_SCHEDULE_SHIFT_WAIT,
            started_at,
            0,
            datetime(2026, 2, 19, 12, 0, tzinfo=tz),
            tz,
        )
        self.assertEqual(plan[:3], ("STEP_WAIT_NUDGE1_SENT", 0, 1))

    def test_form_step_second_followup_after_forty_eight_hours(self):
        tz = ZoneInfo("Europe/Kyiv")
        started_at = datetime(2026, 2, 19, 10, 0, tzinfo=tz).timestamp()
        plan = auto_reply.resolve_v2_followup_stage(
            auto_reply.STEP_FORM_FORWARD,
            started_at,
            1,
            datetime(2026, 2, 21, 9, 59, tzinfo=tz),
            tz,
        )
        self.assertIsNone(plan)
        plan = auto_reply.resolve_v2_followup_stage(
            auto_reply.STEP_FORM_FORWARD,
            started_at,
            1,
            datetime(2026, 2, 21, 10, 0, tzinfo=tz),
            tz,
        )
        self.assertEqual(plan[:3], ("STEP_WAIT_NUDGE2_SENT", 1, 2))


if __name__ == "__main__":
    unittest.main()
