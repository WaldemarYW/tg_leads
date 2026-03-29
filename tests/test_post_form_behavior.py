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


class PostFormBehaviorTests(unittest.TestCase):
    def test_form_lock_reply_no_longer_mentions_verification(self):
        self.assertNotIn("верифікації", auto_reply.FORM_LOCK_REPLY_TEXT)
        self.assertNotIn("документа", auto_reply.FORM_LOCK_REPLY_TEXT)

    def test_form_forward_status_is_form_requested(self):
        self.assertEqual(
            auto_reply._status_from_step(auto_reply.STEP_FORM_FORWARD, None, ""),
            auto_reply.STATUS_FORM_REQUESTED,
        )
        self.assertEqual(
            auto_reply._status_from_step(
                auto_reply.STEP_FORM_FORWARD,
                None,
                "Будь ласка, надішліть фото або скрін документа для верифікації.",
            ),
            auto_reply.STATUS_FORM_REQUESTED,
        )

    def test_manual_form_recovery_keeps_photo_wait_disabled(self):
        state = auto_reply.PeerRuntimeState(peer_id=7, form_waiting_photo=True)
        auto_reply.prime_manual_v2_runtime_state(state, auto_reply.STEP_FORM_FORWARD, now_ts=500.0)
        self.assertFalse(state.form_waiting_photo)
        self.assertEqual(state.step_wait_step, auto_reply.STEP_FORM_FORWARD)


if __name__ == "__main__":
    unittest.main()
