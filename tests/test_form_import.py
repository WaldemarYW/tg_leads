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


class FormImportTests(unittest.TestCase):
    def test_normalizes_valid_username(self):
        self.assertEqual(auto_reply.normalize_form_import_username("waldemarwood"), ("@waldemarwood", True))
        self.assertEqual(auto_reply.normalize_form_import_username("@waldemarwood"), ("@waldemarwood", True))

    def test_keeps_invalid_username_raw(self):
        self.assertEqual(auto_reply.normalize_form_import_username("wa"), ("wa", False))

    def test_parses_form_import_row(self):
        headers = [
            "Отметка времени",
            "Імʼя",
            "Telegram Username",
            "Скільки повних років",
            "Номер телефона",
            "Наявність ПК або ноутбука",
        ]
        row = [
            "13.04.2026 16:05:31",
            "Володимир",
            "waldemarwood",
            "30",
            "+380991112233",
            "Так, є",
        ]
        parsed = auto_reply.parse_form_import_row(headers, row)
        self.assertEqual(parsed["name"], "Володимир")
        self.assertEqual(parsed["username"], "@waldemarwood")
        self.assertTrue(parsed["username_valid"])
        self.assertEqual(parsed["age"], "30")
        self.assertEqual(parsed["phone"], "+380991112233")
        self.assertEqual(parsed["pc"], "Так, є")

    def test_renders_form_import_message_without_phone(self):
        message = auto_reply.build_form_import_message(
            {
                "name": "Володимир",
                "username": "@waldemarwood",
                "age": "30",
                "phone": "",
                "pc": "Так, є",
            }
        )
        self.assertIn("🚹 НОВА АНКЕТА", message)
        self.assertIn("❇️ Імʼя: Володимир", message)
        self.assertIn("ℹ️ Користувач: @waldemarwood", message)
        self.assertNotIn("☎️ Номер телефону:", message)
        self.assertIn("⏳ Вік: 30", message)
        self.assertIn("💻 Ноутбук: Так, є", message)
        self.assertIn("🪧 Примітка: Facebook", message)


if __name__ == "__main__":
    unittest.main()
