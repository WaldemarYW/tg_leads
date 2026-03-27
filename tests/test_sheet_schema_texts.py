import unittest
from pathlib import Path


class SheetSchemaTextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parent.parent
        cls.auto_reply = (root / "auto_reply.py").read_text(encoding="utf-8")
        cls.legacy = (root / "tg_to_sheets.py").read_text(encoding="utf-8")

    def test_month_sheet_title_format_present(self):
        self.assertIn('return f"{RU_MONTHS[dt.month]} {dt.month:02d} {dt.year}"', self.auto_reply)
        self.assertIn('return f"{RU_MONTHS[dt.month]} {dt.month:02d} {dt.year}"', self.legacy)

    def test_today_headers_replaced_with_month_schema(self):
        expected_headers = [
            '"Дата"',
            '"Имя"',
            '"Username"',
            '"Возраст"',
            '"Наличие ПК/ноутбука"',
            '"Смена"',
            '"Ссылка на чат"',
            '"Ссылка на заявку"',
            '"Статус"',
            '"Пир"',
            '"Аккаунт"',
            '"Дата первого старта"',
        ]
        for header in expected_headers:
            self.assertIn(header, self.auto_reply)

    def test_new_funnel_statuses_present(self):
        expected_statuses = [
            "Вводные отправлены",
            "Интерес подтвержден",
            "Ожидание выбора смены",
            "Смена выбрана",
            "Формат работы объяснен",
            "Доход и обучение объяснены",
            "Анкета запрошена",
            "Анкета получена",
            "Ожидание документа",
            "Передано тимлиду",
            "Пауза",
            "Не актуально",
            "Отказ кандидата",
            "Не подходит график",
            "Нет ПК/ноутбука",
        ]
        for status in expected_statuses:
            self.assertIn(status, self.auto_reply)


if __name__ == "__main__":
    unittest.main()
