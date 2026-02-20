import unittest

from faq_service import normalize_question, build_cluster_key


class FAQServiceTests(unittest.TestCase):
    def test_normalize(self):
        self.assertEqual(normalize_question(" ЯКИЙ графік? "), "який графік")

    def test_cluster_key(self):
        key = build_cluster_key("a" * 200)
        self.assertEqual(len(key), 160)


if __name__ == "__main__":
    unittest.main()
