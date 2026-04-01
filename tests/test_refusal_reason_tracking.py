import importlib
import os
import re
import sys
import types
import unittest


os.environ.setdefault("API_ID", "1")
os.environ.setdefault("API_HASH", "test-hash")
os.environ.setdefault("SESSION_FILE", "/tmp/test.session")
os.environ.setdefault("SHEET_NAME", "test-sheet")
os.environ.setdefault("GOOGLE_CREDS", "/tmp/test-creds.json")
os.environ["DIALOG_REFUSAL_URL"] = ""

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


class FakeWorksheet:
    def __init__(self, values):
        self.values = [list(row) for row in values]
        self.id = 101
        self.title = "April 2026"

    def row_values(self, idx):
        if idx <= len(self.values):
            return list(self.values[idx - 1])
        return []

    def get_all_values(self):
        return [list(row) for row in self.values]

    def clear(self):
        self.values = []

    def append_row(self, row, value_input_option=None):
        _ = value_input_option
        self.values.append(list(row))

    def append_rows(self, rows, value_input_option=None):
        _ = value_input_option
        for row in rows:
            self.values.append(list(row))

    def update(self, range_name=None, values=None, value_input_option=None):
        _ = value_input_option
        match = re.search(r"(\d+):[A-Z]+(\d+)$", range_name or "")
        if not match:
            raise AssertionError(f"Unexpected range: {range_name}")
        row_idx = int(match.group(1))
        self.values[row_idx - 1] = list(values[0])


class RefusalReasonTrackingTests(unittest.TestCase):
    def test_today_headers_include_refusal_columns(self):
        self.assertIn("Причина отказа", auto_reply.TODAY_HEADERS)
        self.assertIn("Фраза отказа", auto_reply.TODAY_HEADERS)

    def test_local_refusal_classifier_covers_key_categories(self):
        self.assertEqual(
            auto_reply.classify_refusal_reason_local("Мені не підходить нічна зміна", auto_reply.STEP_SCHEDULE_SHIFT_WAIT),
            auto_reply.REFUSAL_REASON_SCHEDULE,
        )
        self.assertEqual(
            auto_reply.classify_refusal_reason_local("Без ставки мені не підходить", auto_reply.STEP_BALANCE_CONFIRM),
            auto_reply.REFUSAL_REASON_INCOME_MODEL,
        )
        self.assertEqual(
            auto_reply.classify_refusal_reason_local("Поки що неактуально, повернуся пізніше", auto_reply.STEP_COMPANY_INTRO),
            auto_reply.REFUSAL_REASON_LATER,
        )

    def test_special_start_reason_maps_to_refusal_payload(self):
        self.assertEqual(
            auto_reply.refusal_reason_from_special_start(auto_reply.SPECIAL_START_NO_PC),
            auto_reply.REFUSAL_REASON_NO_PC,
        )
        self.assertEqual(
            auto_reply.refusal_reason_from_special_start(auto_reply.SPECIAL_START_AGE_40_PLUS),
            auto_reply.REFUSAL_REASON_AGE,
        )
        self.assertEqual(
            auto_reply.refusal_raw_from_special_start(auto_reply.SPECIAL_START_UNDERAGE_15),
            "special_start:underage_15",
        )

    def test_today_header_migration_preserves_existing_rows(self):
        old_headers = [h for h in auto_reply.TODAY_HEADERS if h not in {"Причина отказа", "Фраза отказа"}]
        ws = FakeWorksheet(
            [
                old_headers,
                ["2026-04-01", "Test", "@lead", "23", "Так", "", "chat", "app", "Отказ кандидата", "123", "primary", "2026-04-01"],
            ]
        )
        writer = auto_reply.SheetWriter.__new__(auto_reply.SheetWriter)
        writer._invalidate_ws_cache = lambda ws_obj: None
        writer._ensure_today_headers(ws)
        self.assertEqual(ws.values[0], auto_reply.TODAY_HEADERS)
        self.assertEqual(ws.values[1][0], "2026-04-01")
        self.assertEqual(ws.values[1][1], "Test")

    def test_upsert_preserves_existing_refusal_when_new_payload_is_empty(self):
        ws = FakeWorksheet(
            [
                auto_reply.TODAY_HEADERS,
                [
                    "2026-04-01",
                    "Lead",
                    "@lead",
                    "23",
                    "Так",
                    "",
                    "chat",
                    "",
                    auto_reply.STATUS_STOPPED,
                    auto_reply.REFUSAL_REASON_LATER,
                    "поки неактуально",
                    "123",
                    "primary",
                    "2026-04-01",
                ],
            ]
        )
        writer = auto_reply.SheetWriter.__new__(auto_reply.SheetWriter)
        writer._ensure_today_ws = lambda tz: ws
        writer._get_headers = lambda ws_obj: list(ws_obj.values[0])
        writer._find_row_by_peer = lambda ws_obj, peer_id: (2, list(ws_obj.values[1]))
        writer._find_group_lead_info = lambda username, name: None
        writer._owner_account_for_peer = lambda peer_id, existing_account="": existing_account or "primary"
        writer._col_letter = auto_reply.SheetWriter._col_letter.__get__(writer, auto_reply.SheetWriter)
        writer._invalidate_ws_cache = lambda ws_obj: None
        writer._row_index_cache = {}
        writer._next_row_cache = {}
        writer.upsert(
            tz=auto_reply.ZoneInfo("Europe/Kiev"),
            peer_id=123,
            name="Lead",
            username="lead",
            chat_link="chat",
            status=None,
            refusal_reason=None,
            refusal_raw=None,
        )
        headers = ws.values[0]
        reason_idx = headers.index("Причина отказа")
        raw_idx = headers.index("Фраза отказа")
        self.assertEqual(ws.values[1][reason_idx], auto_reply.REFUSAL_REASON_LATER)
        self.assertEqual(ws.values[1][raw_idx], "поки неактуально")


if __name__ == "__main__":
    unittest.main()
