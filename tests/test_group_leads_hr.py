import importlib
import os
import sys
import tempfile
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
    def __init__(self):
        self.values = []

    def row_values(self, row_idx):
        if row_idx != 1 or not self.values:
            return []
        return self.values[0][:]

    def get_all_values(self):
        return [row[:] for row in self.values]

    def update(self, range_name, values, value_input_option="USER_ENTERED"):
        del value_input_option
        start = range_name.split(":")[0]
        row_idx = int("".join(ch for ch in start if ch.isdigit()))
        while len(self.values) < row_idx:
            self.values.append([])
        self.values[row_idx - 1] = values[0][:]


class GroupLeadsHrTests(unittest.TestCase):
    def test_group_leads_headers_include_hr(self):
        self.assertIn("HR", auto_reply.GROUP_LEADS_HEADERS)

    def test_upsert_writes_hr_value(self):
        sheet = auto_reply.GroupLeadsSheet.__new__(auto_reply.GroupLeadsSheet)
        sheet.ws = _FakeWorksheet()
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        sheet.lock_path = os.path.join(tmpdir.name, "group_leads.lock")

        sheet._ensure_headers_exact = auto_reply.GroupLeadsSheet._ensure_headers_exact.__get__(sheet, auto_reply.GroupLeadsSheet)
        sheet._find_row = auto_reply.GroupLeadsSheet._find_row.__get__(sheet, auto_reply.GroupLeadsSheet)
        sheet._find_month_link = lambda tz, peer_id: ""

        sheet.upsert(
            ZoneInfo("Europe/Kyiv"),
            {
                "full_name": "Test User",
                "tg": "@testuser",
                "source_name": "@hr_volodymyr",
                "hr_username": "@redfox1378",
                "peer_id": "2113208211",
                "raw_text": "raw",
            },
            "new",
        )

        headers = sheet.ws.values[0]
        self.assertEqual(sheet.ws.values[0], auto_reply.GROUP_LEADS_HEADERS)
        self.assertEqual(sheet.ws.values[1][headers.index("HR")], "@redfox1378")
        self.assertEqual(sheet.ws.values[1][headers.index("Пир")], "2113208211")
        self.assertEqual(sheet.ws.values[1][headers.index("Сырой текст")], "raw")


if __name__ == "__main__":
    unittest.main()
