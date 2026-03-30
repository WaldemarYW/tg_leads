import importlib
import os
import sys
import types
import unittest
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


class _FakeWorksheet:
    def __init__(self, rows=None):
        self.rows = [list(row) for row in (rows or [auto_reply.REGISTRATION_HEADERS[:]])]

    def row_values(self, idx):
        if 1 <= idx <= len(self.rows):
            return list(self.rows[idx - 1])
        return []

    def get_all_values(self):
        return [list(row) for row in self.rows]

    def update(self, range_name=None, values=None, value_input_option=None):
        start_row = int(range_name.split(":")[0][1:])
        row = list(values[0])
        while len(self.rows) < start_row:
            self.rows.append([""] * len(auto_reply.REGISTRATION_HEADERS))
        self.rows[start_row - 1] = row


class RegistrationSheetUpsertTests(unittest.TestCase):
    def setUp(self):
        self.sheet = auto_reply.RegistrationSheet.__new__(auto_reply.RegistrationSheet)
        self.sheet.ws = _FakeWorksheet()
        self.sheet.lock_path = "/tmp/test-registration.lock"
        self.sheet._ensure_headers_exact = lambda: None
        self.tz = ZoneInfo("Europe/Kiev")
        self._orig_acquire = auto_reply.acquire_lock
        self._orig_release = auto_reply.release_lock
        auto_reply.acquire_lock = lambda *args, **kwargs: True
        auto_reply.release_lock = lambda *args, **kwargs: None

    def tearDown(self):
        auto_reply.acquire_lock = self._orig_acquire
        auto_reply.release_lock = self._orig_release

    def test_upsert_updates_existing_row_by_source_message_id(self):
        first = {
            "full_name": "Candidate One",
            "phone": "+380991112233",
            "candidate_tg": "@candidate",
            "message_link": "https://t.me/c/123/10",
            "source_message_id": "10",
            "source_group": "Traffic",
            "raw_text": "first",
        }
        second = {
            **first,
            "full_name": "Candidate Updated",
            "phone": "+380000000000",
            "raw_text": "second",
        }

        self.sheet.upsert(self.tz, first)
        self.sheet.upsert(self.tz, second)

        rows = self.sheet.ws.get_all_values()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1][0], "Candidate Updated")
        self.assertEqual(rows[1][2], "+380000000000")
        self.assertEqual(rows[1][13], "10")
        self.assertEqual(rows[1][11], "second")

    def test_upsert_falls_back_to_message_link_when_source_message_id_missing(self):
        first = {
            "full_name": "Candidate One",
            "message_link": "https://t.me/c/123/11",
            "raw_text": "first",
        }
        second = {
            **first,
            "full_name": "Candidate Updated",
            "raw_text": "second",
        }

        self.sheet.upsert(self.tz, first)
        self.sheet.upsert(self.tz, second)

        rows = self.sheet.ws.get_all_values()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1][0], "Candidate Updated")
        self.assertEqual(rows[1][10], "https://t.me/c/123/11")
        self.assertEqual(rows[1][11], "second")

    def test_different_message_ids_create_separate_rows(self):
        self.sheet.upsert(self.tz, {"full_name": "One", "source_message_id": "10"})
        self.sheet.upsert(self.tz, {"full_name": "Two", "source_message_id": "11"})

        rows = self.sheet.ws.get_all_values()
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[1][0], "One")
        self.assertEqual(rows[2][0], "Two")


if __name__ == "__main__":
    unittest.main()
