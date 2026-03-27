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


class SpecialStartPolicyTests(unittest.TestCase):
    def test_parse_lead_age_supports_plain_and_plus_values(self):
        self.assertEqual(auto_reply.parse_lead_age("15"), 15)
        self.assertEqual(auto_reply.parse_lead_age("40+"), 40)
        self.assertEqual(auto_reply.parse_lead_age("55 років"), 55)
        self.assertIsNone(auto_reply.parse_lead_age(""))
        self.assertIsNone(auto_reply.parse_lead_age("unknown"))

    def test_underage_15_has_highest_priority(self):
        reason = auto_reply.classify_special_start_policy({"age": "15", "pc": "нет"})
        self.assertEqual(reason, auto_reply.SPECIAL_START_UNDERAGE_15)

    def test_no_pc_has_priority_over_40_plus(self):
        reason = auto_reply.classify_special_start_policy({"age": "41", "pc": "немає"})
        self.assertEqual(reason, auto_reply.SPECIAL_START_NO_PC)

    def test_16_and_17_are_eligible(self):
        self.assertEqual(
            auto_reply.classify_special_start_policy({"age": "16", "pc": "так"}),
            auto_reply.SPECIAL_START_ELIGIBLE,
        )
        self.assertEqual(
            auto_reply.classify_special_start_policy({"age": "17", "pc": "є"}),
            auto_reply.SPECIAL_START_ELIGIBLE,
        )

    def test_40_plus_gets_risk_policy(self):
        self.assertEqual(
            auto_reply.classify_special_start_policy({"age": "40", "pc": "так"}),
            auto_reply.SPECIAL_START_AGE_40_PLUS,
        )
        self.assertEqual(
            auto_reply.classify_special_start_policy({"age": "55", "pc": "так"}),
            auto_reply.SPECIAL_START_AGE_40_PLUS,
        )

    def test_missing_or_unparsed_age_does_not_block_start(self):
        self.assertEqual(
            auto_reply.classify_special_start_policy({"age": "", "pc": "так"}),
            auto_reply.SPECIAL_START_ELIGIBLE,
        )
        self.assertEqual(
            auto_reply.classify_special_start_policy({"age": "невідомо", "pc": "так"}),
            auto_reply.SPECIAL_START_ELIGIBLE,
        )

    def test_new_statuses_are_canonical(self):
        self.assertEqual(
            auto_reply.canonical_sheet_status(auto_reply.STATUS_AGE_REJECTED, None),
            auto_reply.STATUS_AGE_REJECTED,
        )
        self.assertEqual(
            auto_reply.canonical_sheet_status(auto_reply.STATUS_AGE_RISK_40_PLUS, None),
            auto_reply.STATUS_AGE_RISK_40_PLUS,
        )


if __name__ == "__main__":
    unittest.main()
