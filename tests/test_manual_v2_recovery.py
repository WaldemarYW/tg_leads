import importlib
import os
import sys
import types
import unittest


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


class ManualV2RecoveryTests(unittest.TestCase):
    def test_detects_company_intro_as_company_step(self):
        self.assertEqual(
            auto_reply.detect_manual_v2_step_from_text(auto_reply.COMPANY_INTRO_TEXT),
            auto_reply.STEP_COMPANY_INTRO,
        )

    def test_detects_shift_prompt_as_shift_wait(self):
        self.assertEqual(
            auto_reply.detect_manual_v2_step_from_text(auto_reply.SCHEDULE_SHIFT_TEXT),
            auto_reply.STEP_SCHEDULE_SHIFT_WAIT,
        )

    def test_detects_balance_block_as_balance_confirm(self):
        self.assertEqual(
            auto_reply.detect_manual_v2_step_from_text(auto_reply.EARNINGS_EXPLAINER_TEXT_1),
            auto_reply.STEP_BALANCE_CONFIRM,
        )

    def test_detects_form_as_form_forward(self):
        self.assertEqual(
            auto_reply.detect_manual_v2_step_from_text(auto_reply.FORM_TEXT),
            auto_reply.STEP_FORM_FORWARD,
        )

    def test_non_script_manual_message_has_no_v2_recovery_step(self):
        self.assertIsNone(auto_reply.detect_manual_v2_step_from_text("Добрий день, пишу вручну без шаблону"))

    def test_prime_manual_intro_recovery_state(self):
        state = PeerRuntimeState(peer_id=1)
        auto_reply.prime_manual_v2_runtime_state(state, auto_reply.STEP_COMPANY_INTRO, now_ts=123.0)
        self.assertEqual(state.flow_step, auto_reply.STEP_COMPANY_INTRO)
        self.assertEqual(state.auto_mode, "OFF")
        self.assertTrue(state.paused)
        self.assertEqual(state.step_wait_step, auto_reply.STEP_COMPANY_INTRO)
        self.assertEqual(state.step_wait_started_at, 123.0)

    def test_prime_manual_shift_recovery_state(self):
        state = PeerRuntimeState(peer_id=2)
        auto_reply.prime_manual_v2_runtime_state(state, auto_reply.STEP_SCHEDULE_SHIFT_WAIT, now_ts=200.0)
        self.assertEqual(state.flow_step, auto_reply.STEP_SCHEDULE_SHIFT_WAIT)
        self.assertEqual(state.shift_prompted_at, 200.0)
        self.assertFalse(state.schedule_shift_fit_check_pending)
        self.assertEqual(state.step_wait_step, auto_reply.STEP_SCHEDULE_SHIFT_WAIT)

    def test_prime_manual_form_recovery_state(self):
        state = PeerRuntimeState(peer_id=3, form_waiting_photo=True)
        auto_reply.prime_manual_v2_runtime_state(state, auto_reply.STEP_FORM_FORWARD, now_ts=300.0)
        self.assertEqual(state.flow_step, auto_reply.STEP_FORM_FORWARD)
        self.assertFalse(state.form_waiting_photo)
        self.assertEqual(state.step_wait_step, auto_reply.STEP_FORM_FORWARD)


if __name__ == "__main__":
    unittest.main()
