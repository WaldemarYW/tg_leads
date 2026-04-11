import unittest

from telegram_group_resolver import extract_invite_hash


class TelegramGroupResolverTests(unittest.TestCase):
    def test_extract_invite_hash_from_plus_link(self):
        self.assertEqual(
            extract_invite_hash("https://t.me/+dVwq3e9_WIwzNTMy"),
            "dVwq3e9_WIwzNTMy",
        )

    def test_extract_invite_hash_from_joinchat_link(self):
        self.assertEqual(
            extract_invite_hash("https://t.me/joinchat/dVwq3e9_WIwzNTMy"),
            "dVwq3e9_WIwzNTMy",
        )

    def test_non_invite_link_returns_empty_hash(self):
        self.assertEqual(extract_invite_hash("https://t.me/public_group"), "")


if __name__ == "__main__":
    unittest.main()
