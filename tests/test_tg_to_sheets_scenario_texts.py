import unittest
from pathlib import Path


class ScenarioTextTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root = Path(__file__).resolve().parent.parent
        cls.content = (root / "tg_to_sheets.py").read_text(encoding="utf-8")

    def test_dating_includes_duties_block(self):
        self.assertIn("Що таке дейтинг?", self.content)
        self.assertIn("Ваші основні завдання:", self.content)

    def test_shifts_text_updated(self):
        self.assertIn("У вас є вибір із кількох графіків:", self.content)
        self.assertIn("- Денна 14:00–23:00", self.content)
        self.assertIn("- Нічна 23:00–08:00", self.content)
        self.assertIn("8 вихідних днів", self.content)

    def test_shift_question_updated(self):
        self.assertIn('SHIFT_QUESTION_TEXT = "Який графік роботи тобі підходить?"', self.content)

    def test_format_text_updated(self):
        self.assertIn("короткий мінікурс + відео", self.content)
        self.assertIn("Там просто і по суті", self.content)
        self.assertIn("FORMAT_QUESTION_TEXT", self.content)


if __name__ == "__main__":
    unittest.main()
