import unittest

from registration_ingest import (
    build_message_link,
    is_media_registration_message,
    parse_registration_message,
)


class _FakeMedia:
    def __init__(self, photo=None, document=None):
        self.photo = photo
        self.document = document


class _FakeMessage:
    def __init__(self, photo=None, document=None, media=None):
        self.photo = photo
        self.document = document
        self.media = media


class RegistrationIngestTests(unittest.TestCase):
    def test_parse_numbered_format(self):
        text = (
            "1. Клюшта Анастасія Богданівна\n"
            "2.09.03.2006\n"
            "3.+380689450648\n"
            "4. anastasiyaa234501\n"
            "5. Ні, немає\n"
            "6. Денна\n"
            "7.20.02\n"
            "8. Івано-Франківськ\n"
            "9. 1anaatasiaa1k@gmail.com\n"
            "@CERBERUS_777\n"
        )
        data = parse_registration_message(text)
        self.assertEqual(data["full_name"], "Клюшта Анастасія Богданівна")
        self.assertEqual(data["birth_date"], "09.03.2006")
        self.assertEqual(data["phone"], "+380689450648")
        self.assertEqual(data["email"], "1anaatasiaa1k@gmail.com")
        self.assertEqual(data["schedule"], "Денна")
        self.assertEqual(data["start_date"], "20.02")
        self.assertEqual(data["city"], "Івано-Франківськ")
        self.assertEqual(data["admin_tg"], "@CERBERUS_777")
        self.assertEqual(data["candidate_tg"], "")

    def test_parse_unordered_non_numbered_format(self):
        text = (
            "Юсипович Тетяна\n"
            "15.11.1997\n"
            "@tinahing\n"
            "tinahing132@gmail.com\n"
            "Немає\n"
            "З 14 до 23\n"
            "Сьогодні 18.02\n"
            "Львів\n"
            "@\u200cDINEROveech\n"
        )
        data = parse_registration_message(text)
        self.assertEqual(data["full_name"], "Юсипович Тетяна")
        self.assertEqual(data["birth_date"], "15.11.1997")
        self.assertEqual(data["candidate_tg"], "@tinahing")
        self.assertEqual(data["schedule"], "З 14 до 23")
        self.assertEqual(data["start_date"], "Сьогодні 18.02")
        self.assertEqual(data["city"], "Львів")
        self.assertEqual(data["admin_tg"], "@DINEROveech")

    def test_parse_last_username_is_admin(self):
        text = "Ім'я\n@email.com\n@candidate_user\nденна\n@admin_final"
        data = parse_registration_message(text)
        self.assertEqual(data["candidate_tg"], "@candidate_user")
        self.assertEqual(data["admin_tg"], "@admin_final")

    def test_media_filter(self):
        self.assertTrue(is_media_registration_message(_FakeMessage(photo=object())))
        self.assertTrue(is_media_registration_message(_FakeMessage(document=object())))
        self.assertTrue(is_media_registration_message(_FakeMessage(media=_FakeMedia(photo=object()))))
        self.assertFalse(is_media_registration_message(_FakeMessage()))

    def test_build_message_link(self):
        self.assertEqual(build_message_link(-1003224733439, 15), "https://t.me/c/3224733439/15")
        self.assertEqual(build_message_link(12345, 10), "")


if __name__ == "__main__":
    unittest.main()

